"""
Google Search Console Data Extractor for rs_analytics

This module provides comprehensive data extraction from Google Search Console:
- All available dimensions (query, page, country, device, searchAppearance, date)
- All available metrics (clicks, impressions, ctr, position)
- Handles API pagination and rate limits
- Supports historical data extraction (up to 16 months per request)

Usage:
    from etl.gsc_extractor import GSCExtractor
    
    extractor = GSCExtractor(credentials_path, site_url)
    data = extractor.extract_all_data(start_date, end_date)
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class GSCExtractor:
    """
    Google Search Console data extractor with comprehensive metric support.
    
    This class handles:
    - Authentication with service account
    - Query construction with all dimensions
    - Pagination for large result sets
    - Error handling and rate limiting
    - Data transformation for DuckDB storage
    
    Attributes:
        service: Google Search Console API service
        site_url: The Search Console property URL
        logger: Logger instance
    """
    
    # GSC API limits
    MAX_ROWS_PER_REQUEST = 25000  # GSC API limit
    MAX_DIMENSIONS_PER_REQUEST = 3  # GSC API limit (unlike GA4's 9)
    
    # Available dimensions in GSC
    ALL_DIMENSIONS = [
        'query',           # Search query/keyword
        'page',            # Page URL
        'country',         # Country code (e.g., usa, gbr)
        'device',          # Device type (DESKTOP, MOBILE, TABLET)
        'searchAppearance', # Rich result type
        'date',            # Date of search
    ]
    
    # Available metrics in GSC (always included)
    ALL_METRICS = [
        'clicks',       # Number of clicks
        'impressions',  # Number of impressions
        'ctr',          # Click-through rate (clicks/impressions)
        'position',     # Average position in search results
    ]
    
    def __init__(
        self,
        credentials_path: str,
        site_url: str,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize GSC extractor with credentials.
        
        Args:
            credentials_path: Path to service account JSON file
            site_url: Search Console property URL
            logger: Optional logger instance
        """
        self.site_url = site_url
        self.logger = logger or logging.getLogger(__name__)
        
        # Authenticate and build service
        self.logger.info(f"Initializing GSC extractor for: {site_url}")
        
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/webmasters.readonly']
        )
        
        self.service = build('searchconsole', 'v1', credentials=credentials)
        self.logger.info("GSC API client initialized successfully")
    
    def get_earliest_data_date(self) -> str:
        """
        Determine the earliest date with data for this property.
        
        GSC typically stores 16 months of data. This method finds the actual
        earliest date by querying for data progressively.
        
        Returns:
            Earliest date with data in YYYY-MM-DD format
        """
        self.logger.info("Determining earliest available data date...")
        
        # Start from 16 months ago (GSC's typical limit)
        today = datetime.now()
        earliest_possible = today - timedelta(days=500)  # ~16 months
        
        # Try to find actual earliest date with data
        test_dates = [
            earliest_possible,
            today - timedelta(days=365),
            today - timedelta(days=180),
            today - timedelta(days=90),
        ]
        
        for test_date in test_dates:
            try:
                response = self.service.searchanalytics().query(
                    siteUrl=self.site_url,
                    body={
                        'startDate': test_date.strftime('%Y-%m-%d'),
                        'endDate': (test_date + timedelta(days=7)).strftime('%Y-%m-%d'),
                        'dimensions': ['date'],
                        'rowLimit': 1,
                    }
                ).execute()
                
                if response.get('rows'):
                    earliest = response['rows'][0]['keys'][0]
                    self.logger.info(f"Found data starting from: {earliest}")
                    return earliest
                    
            except HttpError as e:
                self.logger.debug(f"No data for date {test_date}: {e}")
                continue
        
        # Default to 30 days ago if no earlier data found
        default_start = (today - timedelta(days=30)).strftime('%Y-%m-%d')
        self.logger.info(f"Using default start date: {default_start}")
        return default_start
    
    def _execute_query(
        self,
        start_date: str,
        end_date: str,
        dimensions: List[str],
        row_limit: int = 25000,
        start_row: int = 0
    ) -> Dict[str, Any]:
        """
        Execute a single GSC query.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            dimensions: List of dimensions to include
            row_limit: Maximum rows to return
            start_row: Starting row for pagination
            
        Returns:
            API response dictionary
        """
        request_body = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': dimensions,
            'rowLimit': row_limit,
            'startRow': start_row,
        }
        
        return self.service.searchanalytics().query(
            siteUrl=self.site_url,
            body=request_body
        ).execute()
    
    def extract_with_dimensions(
        self,
        start_date: str,
        end_date: str,
        dimensions: List[str],
        dataset_name: str
    ) -> List[Dict[str, Any]]:
        """
        Extract data with specified dimensions, handling pagination.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            dimensions: List of dimensions (max 3)
            dataset_name: Name for this dataset
            
        Returns:
            List of data rows as dictionaries
        """
        if len(dimensions) > self.MAX_DIMENSIONS_PER_REQUEST:
            raise ValueError(f"GSC supports max {self.MAX_DIMENSIONS_PER_REQUEST} dimensions per request")
        
        self.logger.info(f"Extracting {dataset_name}: dimensions={dimensions}")
        
        all_rows = []
        start_row = 0
        
        while True:
            try:
                response = self._execute_query(
                    start_date=start_date,
                    end_date=end_date,
                    dimensions=dimensions,
                    row_limit=self.MAX_ROWS_PER_REQUEST,
                    start_row=start_row
                )
                
                rows = response.get('rows', [])
                
                if not rows:
                    break
                
                # Transform rows to dictionaries
                for row in rows:
                    record = {'_dataset': dataset_name}
                    
                    # Add dimension values
                    keys = row.get('keys', [])
                    for i, dim in enumerate(dimensions):
                        record[dim] = keys[i] if i < len(keys) else None
                    
                    # Add metric values
                    record['clicks'] = row.get('clicks', 0)
                    record['impressions'] = row.get('impressions', 0)
                    record['ctr'] = row.get('ctr', 0.0)
                    record['position'] = row.get('position', 0.0)
                    
                    all_rows.append(record)
                
                self.logger.info(f"  Fetched {len(all_rows)} rows so far...")
                
                # Check if we've reached the end
                if len(rows) < self.MAX_ROWS_PER_REQUEST:
                    break
                
                start_row += self.MAX_ROWS_PER_REQUEST
                
            except HttpError as e:
                self.logger.error(f"GSC API error: {e}")
                break
        
        self.logger.info(f"  Total: {len(all_rows)} rows for {dataset_name}")
        return all_rows
    
    def extract_all_data(
        self,
        start_date: str,
        end_date: str
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract all available data with different dimension combinations.
        
        GSC limits to 3 dimensions per request, so we make multiple requests
        with different dimension combinations to get comprehensive data.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            Dictionary of dataset_name -> list of rows
        """
        self.logger.info(f"Starting comprehensive GSC extraction: {start_date} to {end_date}")
        
        # Define dimension combinations for different datasets
        # Each combination creates a separate table
        dimension_sets = [
            {
                'name': 'queries',
                'dimensions': ['date', 'query'],
                'description': 'Search queries/keywords over time'
            },
            {
                'name': 'pages',
                'dimensions': ['date', 'page'],
                'description': 'Page performance over time'
            },
            {
                'name': 'countries',
                'dimensions': ['date', 'country'],
                'description': 'Geographic performance over time'
            },
            {
                'name': 'devices',
                'dimensions': ['date', 'device'],
                'description': 'Device performance over time'
            },
            {
                'name': 'search_appearance',
                'dimensions': ['date', 'searchAppearance'],
                'description': 'Rich results and search appearance types'
            },
            {
                'name': 'query_page',
                'dimensions': ['query', 'page'],
                'description': 'Query to page mapping (which queries drive which pages)'
            },
            {
                'name': 'query_country',
                'dimensions': ['query', 'country'],
                'description': 'Query performance by country'
            },
            {
                'name': 'query_device',
                'dimensions': ['query', 'device'],
                'description': 'Query performance by device'
            },
            {
                'name': 'page_country',
                'dimensions': ['page', 'country'],
                'description': 'Page performance by country'
            },
            {
                'name': 'page_device',
                'dimensions': ['page', 'device'],
                'description': 'Page performance by device'
            },
            {
                'name': 'daily_totals',
                'dimensions': ['date'],
                'description': 'Daily aggregate totals'
            },
        ]
        
        all_data = {}
        
        for dim_set in dimension_sets:
            dataset_name = dim_set['name']
            dimensions = dim_set['dimensions']
            
            self.logger.info(f"\n{'='*50}")
            self.logger.info(f"Extracting: {dataset_name}")
            self.logger.info(f"Description: {dim_set['description']}")
            self.logger.info(f"{'='*50}")
            
            try:
                data = self.extract_with_dimensions(
                    start_date=start_date,
                    end_date=end_date,
                    dimensions=dimensions,
                    dataset_name=dataset_name
                )
                
                if data:
                    all_data[dataset_name] = data
                    self.logger.info(f"Successfully extracted {len(data)} rows for {dataset_name}")
                else:
                    self.logger.warning(f"No data returned for {dataset_name}")
                    
            except Exception as e:
                self.logger.error(f"Failed to extract {dataset_name}: {e}")
                continue
        
        # Summary
        total_rows = sum(len(rows) for rows in all_data.values())
        self.logger.info(f"\n{'='*50}")
        self.logger.info(f"EXTRACTION COMPLETE")
        self.logger.info(f"Total datasets: {len(all_data)}")
        self.logger.info(f"Total rows: {total_rows:,}")
        self.logger.info(f"{'='*50}")
        
        return all_data
    
    def test_connection(self) -> tuple[bool, str]:
        """
        Test the GSC connection by running a minimal query.
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            
            response = self._execute_query(
                start_date=week_ago,
                end_date=yesterday,
                dimensions=['date'],
                row_limit=10
            )
            
            rows = response.get('rows', [])
            
            if rows:
                total_clicks = sum(r.get('clicks', 0) for r in rows)
                total_impressions = sum(r.get('impressions', 0) for r in rows)
                return True, (
                    f"GSC connection successful!\n"
                    f"Last 7 days: {total_clicks:,} clicks, {total_impressions:,} impressions"
                )
            else:
                return True, "GSC connection successful! No data in the last 7 days."
                
        except HttpError as e:
            return False, f"GSC API error: {e}"
        except Exception as e:
            return False, f"Connection test failed: {e}"


def get_gsc_dimension_sets() -> List[Dict[str, Any]]:
    """
    Return the dimension set configurations for reference.
    
    This is useful for documentation and understanding what data is extracted.
    
    Returns:
        List of dimension set configurations
    """
    return [
        {
            'table_name': 'gsc_queries',
            'dimensions': ['date', 'query'],
            'description': 'Search queries/keywords performance by date',
            'use_cases': ['Keyword research', 'Content optimization', 'SEO strategy']
        },
        {
            'table_name': 'gsc_pages',
            'dimensions': ['date', 'page'],
            'description': 'Page-level SEO performance by date',
            'use_cases': ['Content performance', 'URL optimization', 'Indexing issues']
        },
        {
            'table_name': 'gsc_countries',
            'dimensions': ['date', 'country'],
            'description': 'Geographic search performance',
            'use_cases': ['International SEO', 'Market analysis', 'Localization']
        },
        {
            'table_name': 'gsc_devices',
            'dimensions': ['date', 'device'],
            'description': 'Device-specific search performance',
            'use_cases': ['Mobile SEO', 'Responsive design', 'AMP performance']
        },
        {
            'table_name': 'gsc_search_appearance',
            'dimensions': ['date', 'searchAppearance'],
            'description': 'Rich results and special search features',
            'use_cases': ['Rich snippets', 'FAQ schema', 'Featured snippets']
        },
        {
            'table_name': 'gsc_query_page',
            'dimensions': ['query', 'page'],
            'description': 'Which queries drive traffic to which pages',
            'use_cases': ['Content gaps', 'Keyword cannibalization', 'Page optimization']
        },
        {
            'table_name': 'gsc_query_country',
            'dimensions': ['query', 'country'],
            'description': 'Query performance by country',
            'use_cases': ['Localized keywords', 'Regional content strategy']
        },
        {
            'table_name': 'gsc_query_device',
            'dimensions': ['query', 'device'],
            'description': 'Query performance by device type',
            'use_cases': ['Mobile keywords', 'Device-specific content']
        },
        {
            'table_name': 'gsc_page_country',
            'dimensions': ['page', 'country'],
            'description': 'Page performance by country',
            'use_cases': ['International page performance', 'Hreflang issues']
        },
        {
            'table_name': 'gsc_page_device',
            'dimensions': ['page', 'device'],
            'description': 'Page performance by device',
            'use_cases': ['Mobile page speed', 'Responsive issues']
        },
        {
            'table_name': 'gsc_daily_totals',
            'dimensions': ['date'],
            'description': 'Daily aggregate totals',
            'use_cases': ['Overall trends', 'Traffic monitoring', 'Algorithm updates']
        },
    ]
