"""
Google Ads Data Extractor for rs_analytics

This module provides comprehensive data extraction from Google Ads:
- Campaign performance metrics
- Ad group performance
- Keyword performance
- Ad performance
- Geographic performance
- Device performance
- Audience data
- Conversion tracking
- All available metrics for lifetime data

Usage:
    from etl.gads_extractor import GAdsExtractor
    
    extractor = GAdsExtractor(client, customer_id)
    data = extractor.extract_all_data(start_date, end_date)
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


class GAdsExtractor:
    """
    Google Ads data extractor with comprehensive metric support.
    
    This class handles:
    - Campaign, Ad Group, Keyword, Ad data extraction
    - Geographic and device breakdowns
    - Conversion metrics
    - All lifetime data
    """
    
    def __init__(
        self,
        client: GoogleAdsClient,
        customer_id: str,
        login_customer_id: Optional[str] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize Google Ads extractor.
        
        Args:
            client: GoogleAdsClient instance
            customer_id: Target customer ID
            login_customer_id: Manager account ID (if applicable)
            logger: Optional logger instance
        """
        self.client = client
        self.customer_id = customer_id.replace('-', '')
        self.login_customer_id = login_customer_id.replace('-', '') if login_customer_id else None
        self.logger = logger or logging.getLogger(__name__)
        
        # Get the service
        self.ga_service = client.get_service("GoogleAdsService")
        
        self.logger.info(f"Initialized Google Ads extractor for customer: {customer_id}")
    
    def _execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a GAQL query and return results as list of dicts.
        
        Args:
            query: Google Ads Query Language query
            
        Returns:
            List of dictionaries containing query results
        """
        results = []
        
        try:
            # For metrics queries, use the client customer_id (not manager)
            # The login_customer_id is set in the client for authentication
            response = self.ga_service.search(
                customer_id=self.customer_id,
                query=query
            )
            
            for row in response:
                results.append(self._row_to_dict(row))
                
        except GoogleAdsException as e:
            self.logger.error(f"Google Ads API error: {e.failure.errors[0].message}")
            raise
            
        return results
    
    def _row_to_dict(self, row) -> Dict[str, Any]:
        """Convert a GoogleAdsRow to a dictionary."""
        result = {}
        
        # Campaign fields
        if hasattr(row, 'campaign') and row.campaign:
            campaign = row.campaign
            result['campaign_id'] = campaign.id
            result['campaign_name'] = campaign.name
            result['campaign_status'] = campaign.status.name if campaign.status else None
            result['campaign_type'] = campaign.advertising_channel_type.name if campaign.advertising_channel_type else None
        
        # Ad Group fields
        if hasattr(row, 'ad_group') and row.ad_group:
            ad_group = row.ad_group
            result['ad_group_id'] = ad_group.id
            result['ad_group_name'] = ad_group.name
            result['ad_group_status'] = ad_group.status.name if ad_group.status else None
            result['ad_group_type'] = ad_group.type_.name if ad_group.type_ else None
        
        # Ad fields
        if hasattr(row, 'ad_group_ad') and row.ad_group_ad:
            ad = row.ad_group_ad
            result['ad_id'] = ad.ad.id if ad.ad else None
            result['ad_status'] = ad.status.name if ad.status else None
            result['ad_type'] = ad.ad.type_.name if ad.ad and ad.ad.type_ else None
        
        # Keyword fields
        if hasattr(row, 'ad_group_criterion') and row.ad_group_criterion:
            criterion = row.ad_group_criterion
            result['keyword_id'] = criterion.criterion_id
            if criterion.keyword:
                result['keyword_text'] = criterion.keyword.text
                result['keyword_match_type'] = criterion.keyword.match_type.name
            result['keyword_status'] = criterion.status.name if criterion.status else None
        
        # Segments (date, device, geo, etc.)
        if hasattr(row, 'segments') and row.segments:
            segments = row.segments
            if segments.date:
                result['date'] = segments.date
            if segments.device:
                result['device'] = segments.device.name
            if segments.ad_network_type:
                result['ad_network_type'] = segments.ad_network_type.name
            if segments.click_type:
                result['click_type'] = segments.click_type.name
            if segments.conversion_action:
                result['conversion_action'] = segments.conversion_action
            if segments.conversion_action_name:
                result['conversion_action_name'] = segments.conversion_action_name
            if segments.hour:
                result['hour'] = segments.hour
            if segments.day_of_week:
                result['day_of_week'] = segments.day_of_week.name
        
        # Geographic fields
        if hasattr(row, 'geographic_view') and row.geographic_view:
            geo = row.geographic_view
            result['country_criterion_id'] = geo.country_criterion_id
            result['location_type'] = geo.location_type.name if geo.location_type else None
        
        # Metrics
        if hasattr(row, 'metrics') and row.metrics:
            metrics = row.metrics
            
            # Core metrics
            result['impressions'] = metrics.impressions
            result['clicks'] = metrics.clicks
            result['cost_micros'] = metrics.cost_micros
            result['cost'] = metrics.cost_micros / 1_000_000 if metrics.cost_micros else 0
            
            # Rates
            result['ctr'] = metrics.ctr
            result['average_cpc_micros'] = metrics.average_cpc
            result['average_cpc'] = metrics.average_cpc / 1_000_000 if metrics.average_cpc else 0
            result['average_cpm_micros'] = metrics.average_cpm
            result['average_cpm'] = metrics.average_cpm / 1_000_000 if metrics.average_cpm else 0
            
            # Conversions
            result['conversions'] = metrics.conversions
            result['conversions_value'] = metrics.conversions_value
            result['all_conversions'] = metrics.all_conversions
            result['all_conversions_value'] = metrics.all_conversions_value
            result['conversion_rate'] = metrics.conversions / metrics.clicks if metrics.clicks else 0
            
            # View metrics
            result['view_through_conversions'] = metrics.view_through_conversions
            
            # Engagement metrics
            result['interactions'] = metrics.interactions
            result['interaction_rate'] = metrics.interaction_rate
            result['engagement_rate'] = metrics.engagement_rate
            
            # Video metrics (if applicable) - safely access as these may not exist
            result['video_views'] = getattr(metrics, 'video_views', 0)
            result['video_quartile_p25_rate'] = getattr(metrics, 'video_quartile_p25_rate', 0)
            result['video_quartile_p50_rate'] = getattr(metrics, 'video_quartile_p50_rate', 0)
            result['video_quartile_p75_rate'] = getattr(metrics, 'video_quartile_p75_rate', 0)
            result['video_quartile_p100_rate'] = getattr(metrics, 'video_quartile_p100_rate', 0)
            
            # Quality metrics
            result['search_impression_share'] = metrics.search_impression_share
            result['search_rank_lost_impression_share'] = metrics.search_rank_lost_impression_share
            result['search_budget_lost_impression_share'] = metrics.search_budget_lost_impression_share
            
            # Position metrics
            result['average_position'] = getattr(metrics, 'average_position', None)
            result['top_impression_percentage'] = metrics.top_impression_percentage
            result['absolute_top_impression_percentage'] = metrics.absolute_top_impression_percentage
        
        # Customer fields
        if hasattr(row, 'customer') and row.customer:
            customer = row.customer
            result['customer_id'] = customer.id
            result['customer_name'] = customer.descriptive_name
        
        return result
    
    def extract_campaign_performance(
        self,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """
        Extract campaign-level performance data.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            List of campaign performance records
        """
        self.logger.info(f"Extracting campaign performance: {start_date} to {end_date}")
        
        # Note: video_views removed as it's not available for all campaign types
        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                campaign.status,
                campaign.advertising_channel_type,
                segments.date,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.ctr,
                metrics.average_cpc,
                metrics.conversions,
                metrics.conversions_value,
                metrics.all_conversions,
                metrics.interactions,
                metrics.interaction_rate,
                metrics.search_impression_share,
                metrics.top_impression_percentage,
                metrics.absolute_top_impression_percentage
            FROM campaign
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY segments.date DESC, metrics.impressions DESC
        """
        
        results = self._execute_query(query)
        self.logger.info(f"Extracted {len(results)} campaign records")
        return results
    
    def extract_ad_group_performance(
        self,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """Extract ad group level performance."""
        self.logger.info(f"Extracting ad group performance: {start_date} to {end_date}")
        
        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group.status,
                ad_group.type,
                segments.date,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.ctr,
                metrics.average_cpc,
                metrics.conversions,
                metrics.conversions_value,
                metrics.all_conversions,
                metrics.interactions
            FROM ad_group
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY segments.date DESC, metrics.impressions DESC
        """
        
        results = self._execute_query(query)
        self.logger.info(f"Extracted {len(results)} ad group records")
        return results
    
    def extract_keyword_performance(
        self,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """Extract keyword level performance."""
        self.logger.info(f"Extracting keyword performance: {start_date} to {end_date}")
        
        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group_criterion.criterion_id,
                ad_group_criterion.keyword.text,
                ad_group_criterion.keyword.match_type,
                ad_group_criterion.status,
                segments.date,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.ctr,
                metrics.average_cpc,
                metrics.conversions,
                metrics.conversions_value,
                metrics.top_impression_percentage,
                metrics.absolute_top_impression_percentage
            FROM keyword_view
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY segments.date DESC, metrics.impressions DESC
        """
        
        results = self._execute_query(query)
        self.logger.info(f"Extracted {len(results)} keyword records")
        return results
    
    def extract_ad_performance(
        self,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """Extract ad level performance."""
        self.logger.info(f"Extracting ad performance: {start_date} to {end_date}")
        
        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                ad_group_ad.ad.id,
                ad_group_ad.ad.type,
                ad_group_ad.status,
                segments.date,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.ctr,
                metrics.average_cpc,
                metrics.conversions,
                metrics.conversions_value,
                metrics.all_conversions
            FROM ad_group_ad
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY segments.date DESC, metrics.impressions DESC
        """
        
        results = self._execute_query(query)
        self.logger.info(f"Extracted {len(results)} ad records")
        return results
    
    def extract_device_performance(
        self,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """Extract performance by device."""
        self.logger.info(f"Extracting device performance: {start_date} to {end_date}")
        
        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                segments.date,
                segments.device,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.ctr,
                metrics.average_cpc,
                metrics.conversions,
                metrics.conversions_value
            FROM campaign
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY segments.date DESC, segments.device
        """
        
        results = self._execute_query(query)
        self.logger.info(f"Extracted {len(results)} device records")
        return results
    
    def extract_geographic_performance(
        self,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """Extract geographic performance data."""
        self.logger.info(f"Extracting geographic performance: {start_date} to {end_date}")
        
        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                geographic_view.country_criterion_id,
                geographic_view.location_type,
                segments.date,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.ctr,
                metrics.average_cpc,
                metrics.conversions,
                metrics.conversions_value
            FROM geographic_view
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY segments.date DESC, metrics.impressions DESC
        """
        
        results = self._execute_query(query)
        self.logger.info(f"Extracted {len(results)} geographic records")
        return results
    
    def extract_hourly_performance(
        self,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """Extract hourly performance aggregates."""
        self.logger.info(f"Extracting hourly performance: {start_date} to {end_date}")
        
        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                segments.date,
                segments.hour,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions
            FROM campaign
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY segments.date DESC, segments.hour
        """
        
        results = self._execute_query(query)
        self.logger.info(f"Extracted {len(results)} hourly records")
        return results
    
    def extract_conversion_actions(
        self,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """Extract conversion action performance."""
        self.logger.info(f"Extracting conversion actions: {start_date} to {end_date}")
        
        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                segments.date,
                segments.conversion_action,
                segments.conversion_action_name,
                metrics.conversions,
                metrics.conversions_value,
                metrics.all_conversions,
                metrics.all_conversions_value
            FROM campaign
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                AND segments.conversion_action IS NOT NULL
            ORDER BY segments.date DESC, metrics.conversions DESC
        """
        
        results = self._execute_query(query)
        self.logger.info(f"Extracted {len(results)} conversion action records")
        return results
    
    def extract_daily_account_summary(
        self,
        start_date: str,
        end_date: str
    ) -> List[Dict[str, Any]]:
        """Extract daily account-level summary."""
        self.logger.info(f"Extracting daily account summary: {start_date} to {end_date}")
        
        # Query campaign-level data and aggregate for account summary
        query = f"""
            SELECT
                segments.date,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                metrics.conversions_value,
                metrics.all_conversions
            FROM campaign
            WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY segments.date DESC
        """
        
        results = self._execute_query(query)
        self.logger.info(f"Extracted {len(results)} daily summary records")
        return results
    
    def extract_all_data(
        self,
        start_date: str,
        end_date: str
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract all available Google Ads data.
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            Dictionary of dataset_name -> list of records
        """
        self.logger.info(f"Starting comprehensive Google Ads extraction: {start_date} to {end_date}")
        
        all_data = {}
        
        # Define extraction methods and their table names
        extractions = [
            ('daily_summary', self.extract_daily_account_summary),
            ('campaigns', self.extract_campaign_performance),
            ('ad_groups', self.extract_ad_group_performance),
            ('keywords', self.extract_keyword_performance),
            ('ads', self.extract_ad_performance),
            ('devices', self.extract_device_performance),
            ('geographic', self.extract_geographic_performance),
            ('hourly', self.extract_hourly_performance),
            ('conversions', self.extract_conversion_actions),
        ]
        
        for dataset_name, extract_method in extractions:
            self.logger.info(f"\n{'='*50}")
            self.logger.info(f"Extracting: {dataset_name}")
            self.logger.info(f"{'='*50}")
            
            try:
                data = extract_method(start_date, end_date)
                
                if data:
                    all_data[dataset_name] = data
                    self.logger.info(f"Successfully extracted {len(data)} rows for {dataset_name}")
                else:
                    self.logger.warning(f"No data returned for {dataset_name}")
                    
            except Exception as e:
                self.logger.error(f"Failed to extract {dataset_name}: {e}")
                # Continue with other extractions
                continue
        
        # Summary
        total_rows = sum(len(rows) for rows in all_data.values())
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"EXTRACTION COMPLETE")
        self.logger.info(f"Total datasets: {len(all_data)}")
        self.logger.info(f"Total rows: {total_rows:,}")
        self.logger.info(f"{'='*60}")
        
        return all_data
    
    def test_connection(self) -> tuple[bool, str]:
        """
        Test the Google Ads connection.
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            query = """
                SELECT
                    customer.id,
                    customer.descriptive_name,
                    customer.currency_code,
                    customer.time_zone
                FROM customer
                LIMIT 1
            """
            
            effective_customer_id = self.login_customer_id or self.customer_id
            response = self.ga_service.search(
                customer_id=effective_customer_id,
                query=query
            )
            
            for row in response:
                customer = row.customer
                return True, (
                    f"Google Ads connection successful!\n"
                    f"Account: {customer.descriptive_name}\n"
                    f"Customer ID: {customer.id}\n"
                    f"Currency: {customer.currency_code}"
                )
            
            return True, "Connection successful (no customer details returned)"
            
        except GoogleAdsException as e:
            error_msg = e.failure.errors[0].message if e.failure.errors else str(e)
            return False, f"Google Ads API error: {error_msg}"
        except Exception as e:
            return False, f"Connection test failed: {e}"


def get_gads_table_descriptions() -> Dict[str, str]:
    """
    Get descriptions for all Google Ads tables.
    
    Returns:
        Dictionary of table_name -> description
    """
    return {
        'gads_daily_summary': 'Account-level daily performance summary',
        'gads_campaigns': 'Campaign-level performance metrics by date',
        'gads_ad_groups': 'Ad group level performance metrics',
        'gads_keywords': 'Keyword-level performance and quality metrics',
        'gads_ads': 'Individual ad performance metrics',
        'gads_devices': 'Performance breakdown by device type',
        'gads_geographic': 'Geographic/location-based performance',
        'gads_hourly': 'Hourly performance for time-of-day analysis',
        'gads_conversions': 'Conversion action performance tracking',
    }
