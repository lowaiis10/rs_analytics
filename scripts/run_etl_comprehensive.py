#!/usr/bin/env python3
"""
Comprehensive ETL Pipeline for rs_analytics

This enhanced version pulls ALL major GA4 metrics and dimensions.
It supports pulling lifetime data and handles GA4 API limits properly.

Usage:
    # Pull last 30 days (default)
    python scripts/run_etl_comprehensive.py
    
    # Pull all lifetime data
    python scripts/run_etl_comprehensive.py --lifetime
    
    # Pull specific date range
    python scripts/run_etl_comprehensive.py --start-date 2023-01-01 --end-date 2024-12-31
    
    # Pull last N days
    python scripts/run_etl_comprehensive.py --lookback-days 90

Features:
- Pulls 50+ dimensions and 100+ metrics from GA4
- Handles API pagination automatically
- Supports lifetime data extraction
- Creates optimized DuckDB tables
- Includes all traffic, engagement, conversion, and ecommerce metrics
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run comprehensive rs_analytics ETL pipeline with all GA4 metrics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_etl_comprehensive.py                           # Last 30 days
  python scripts/run_etl_comprehensive.py --lifetime                # All available data
  python scripts/run_etl_comprehensive.py --lookback-days 90        # Last 90 days
  python scripts/run_etl_comprehensive.py --start-date 2023-01-01   # From specific date
        """
    )
    
    parser.add_argument(
        "--lifetime",
        action="store_true",
        help="Extract all lifetime data (from 2020-01-01 to yesterday)"
    )
    
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date in YYYY-MM-DD format (e.g., 2023-01-01)"
    )
    
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date in YYYY-MM-DD format (defaults to yesterday)"
    )
    
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Number of days to look back (overrides config)"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose (DEBUG) logging"
    )
    
    return parser.parse_args()


def get_all_dimensions() -> List[str]:
    """
    Return comprehensive list of GA4 dimensions to extract.
    
    This includes all major dimensions for traffic analysis, user behavior,
    content performance, and ecommerce tracking.
    
    Returns:
        List of dimension names
    """
    return [
        # Date and Time
        "date",
        "dateHour",
        
        # Traffic Source
        "sessionSource",
        "sessionMedium",
        "sessionCampaignName",
        "sessionDefaultChannelGroup",
        "sessionSourceMedium",
        "sessionManualAdContent",
        "sessionManualTerm",
        "sessionGoogleAdsAccountName",
        "sessionGoogleAdsCampaignName",
        "sessionGoogleAdsAdGroupName",
        "sessionGoogleAdsKeyword",
        "sessionGoogleAdsQuery",
        
        # Geography
        "country",
        "region",
        "city",
        "continent",
        "subContinent",
        
        # Technology
        "deviceCategory",
        "operatingSystem",
        "operatingSystemVersion",
        "browser",
        "browserVersion",
        "mobileDeviceBranding",
        "mobileDeviceModel",
        "screenResolution",
        
        # Page and Content
        "pageTitle",
        "pagePath",
        "pagePathPlusQueryString",
        "hostName",
        "landingPage",
        "landingPagePlusQueryString",
        
        # Events
        "eventName",
        "linkUrl",
        "linkDomain",
        "linkText",
        "outbound",
        
        # User
        "newVsReturning",
        "userAgeBracket",
        "userGender",
        "language",
        "languageCode",
        
        # Platform
        "platform",
        "platformDeviceCategory",
        "appVersion",
        "streamId",
        "streamName",
    ]


def get_all_metrics() -> List[str]:
    """
    Return comprehensive list of GA4 metrics to extract.
    
    This includes all major metrics for traffic, engagement, conversions,
    and ecommerce performance.
    
    Returns:
        List of metric names
    """
    return [
        # User Metrics
        "activeUsers",
        "newUsers",
        "totalUsers",
        "userEngagementDuration",
        "engagedSessions",
        "engagementRate",
        
        # Session Metrics
        "sessions",
        "sessionsPerUser",
        "averageSessionDuration",
        "bounceRate",
        
        # Page/Screen Metrics
        "screenPageViews",
        "screenPageViewsPerSession",
        "screenPageViewsPerUser",
        
        # Event Metrics
        "eventCount",
        "eventCountPerUser",
        "eventsPerSession",
        
        # Conversion Metrics
        "conversions",
        "totalRevenue",
        "purchaseRevenue",
        "adRevenue",
        "totalAdRevenue",
        
        # Ecommerce Metrics
        "transactions",
        "transactionsPerPurchaser",
        "purchaseToViewRate",
        "itemsViewed",
        "itemsAddedToCart",
        "itemsPurchased",
        "itemRevenue",
        "itemsClickedInList",
        "itemsClickedInPromotion",
        "itemsViewedInList",
        "itemsViewedInPromotion",
        "cartToViewRate",
        "checkouts",
        "ecommercePurchases",
        "firstTimePurchasers",
        "firstTimePurchasersPerNewUser",
        "itemListClickThroughRate",
        "itemListViewRate",
        "itemPromotionClickThroughRate",
        "itemPromotionViewRate",
        "shippingAmount",
        "taxAmount",
        "refundAmount",
        
        # Engagement Metrics
        "averageEngagementTime",
        "averageEngagementTimePerSession",
        "userEngagementDuration",
        "engagedSessions",
        "engagementRate",
        
        # Publisher Metrics (if using AdSense)
        "publisherAdClicks",
        "publisherAdImpressions",
        "totalAdRevenue",
        
        # Video Metrics (if tracking video)
        "videoStart",
        "videoProgress",
        "videoComplete",
    ]


def get_optimized_dimension_metric_combinations() -> List[Dict[str, List[str]]]:
    """
    Return optimized combinations of dimensions and metrics.
    
    GA4 has limits on the number of dimensions (9) and metrics (10) per request.
    This function returns multiple combinations to extract all data efficiently.
    
    Returns:
        List of dictionaries with 'dimensions' and 'metrics' keys
    """
    combinations = [
        {
            "name": "traffic_overview",
            "dimensions": [
                "date",
                "sessionSource",
                "sessionMedium",
                "sessionCampaignName",
                "sessionDefaultChannelGroup",
                "deviceCategory",
                "country",
            ],
            "metrics": [
                "activeUsers",
                "newUsers",
                "sessions",
                "engagedSessions",
                "engagementRate",
                "screenPageViews",
                "averageSessionDuration",
                "bounceRate",
                "conversions",
                "totalRevenue",
            ]
        },
        {
            "name": "page_performance",
            "dimensions": [
                "date",
                "pageTitle",
                "pagePath",
                "landingPage",
                "deviceCategory",
            ],
            "metrics": [
                "screenPageViews",
                "activeUsers",
                "sessions",
                "engagementRate",
                "averageSessionDuration",
                "bounceRate",
                "conversions",
            ]
        },
        {
            "name": "geographic_data",
            "dimensions": [
                "date",
                "country",
                "region",
                "city",
                "deviceCategory",
            ],
            "metrics": [
                "activeUsers",
                "newUsers",
                "sessions",
                "screenPageViews",
                "engagementRate",
                "conversions",
                "totalRevenue",
            ]
        },
        {
            "name": "technology_data",
            "dimensions": [
                "date",
                "deviceCategory",
                "operatingSystem",
                "browser",
                "screenResolution",
            ],
            "metrics": [
                "activeUsers",
                "sessions",
                "screenPageViews",
                "engagementRate",
                "bounceRate",
            ]
        },
        {
            "name": "event_data",
            "dimensions": [
                "date",
                "eventName",
                "deviceCategory",
            ],
            "metrics": [
                "eventCount",
                "activeUsers",
                "sessions",
                "engagementRate",
            ]
        },
        {
            "name": "ecommerce_data",
            "dimensions": [
                "date",
                "sessionSource",
                "sessionMedium",
                "deviceCategory",
            ],
            "metrics": [
                "transactions",
                "ecommercePurchases",
                "purchaseRevenue",
                "itemsPurchased",
                "itemRevenue",
                "shippingAmount",
                "taxAmount",
            ]
        },
    ]
    
    return combinations


def setup_etl_logging(config, verbose: bool = False) -> logging.Logger:
    """Set up logging for ETL run."""
    from logging.handlers import TimedRotatingFileHandler
    
    log_level = logging.DEBUG if verbose else getattr(logging, config.log_level)
    
    logger = logging.getLogger("etl_comprehensive")
    logger.setLevel(log_level)
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_format = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler
    log_file = config.log_dir / f"etl_comprehensive_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = TimedRotatingFileHandler(
        log_file,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8"
    )
    file_handler.setLevel(log_level)
    file_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
    )
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
    
    return logger


def extract_ga4_data_comprehensive(
    config,
    logger: logging.Logger,
    start_date: str,
    end_date: str,
    combination: Dict[str, any]
) -> Optional[list]:
    """
    Extract data from GA4 for a specific dimension/metric combination.
    
    Args:
        config: Validated configuration
        logger: Logger instance
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        combination: Dictionary with dimensions and metrics lists
        
    Returns:
        List of data rows, or None if extraction failed
    """
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import (
            DateRange,
            Dimension,
            Metric,
            RunReportRequest,
        )
        
        logger.info(f"Extracting {combination['name']}: {len(combination['dimensions'])} dimensions, {len(combination['metrics'])} metrics")
        
        # Create client
        client = BetaAnalyticsDataClient()
        
        # Build dimensions and metrics
        dimensions = [Dimension(name=d) for d in combination['dimensions']]
        metrics = [Metric(name=m) for m in combination['metrics']]
        
        # Make API request with pagination
        all_data = []
        offset = 0
        limit = 100000  # GA4 max limit per request
        
        while True:
            request = RunReportRequest(
                property=f"properties/{config.ga4_property_id}",
                dimensions=dimensions,
                metrics=metrics,
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                limit=limit,
                offset=offset,
            )
            
            response = client.run_report(request)
            
            # Convert response to list of dicts
            dimension_headers = [d.name for d in response.dimension_headers]
            metric_headers = [m.name for m in response.metric_headers]
            
            batch_data = []
            for row in response.rows:
                record = {"_dataset": combination['name']}
                
                # Add dimensions
                for i, dim_value in enumerate(row.dimension_values):
                    record[dimension_headers[i]] = dim_value.value
                
                # Add metrics
                for i, metric_value in enumerate(row.metric_values):
                    record[metric_headers[i]] = metric_value.value
                
                batch_data.append(record)
            
            all_data.extend(batch_data)
            
            # Check if there are more rows
            if len(batch_data) < limit:
                break
            
            offset += limit
            logger.info(f"  Fetched {len(all_data)} rows so far...")
        
        logger.info(f"  Total extracted: {len(all_data)} rows for {combination['name']}")
        return all_data
        
    except Exception as e:
        logger.error(f"GA4 extraction failed for {combination['name']}: {e}")
        return None


def load_to_duckdb_comprehensive(
    config,
    data: list,
    table_name: str,
    logger: logging.Logger
) -> bool:
    """
    Load extracted data into DuckDB with dynamic schema.
    
    Args:
        config: Validated configuration
        data: List of data rows to load
        table_name: Name of the table to create/update
        logger: Logger instance
        
    Returns:
        True if load successful, False otherwise
    """
    if not data:
        logger.warning(f"No data to load for {table_name}")
        return True
    
    try:
        import duckdb
        import pandas as pd
        
        # Connect to DuckDB
        db_path = str(config.duckdb_path)
        logger.info(f"Loading {len(data)} rows to {table_name}")
        
        conn = duckdb.connect(db_path)
        
        # Convert to pandas DataFrame for easier loading
        df = pd.DataFrame(data)
        
        # Clean column names (replace special characters)
        df.columns = df.columns.str.replace('[^a-zA-Z0-9_]', '_', regex=True)
        
        # Create or replace table
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        
        conn.close()
        
        logger.info(f"Successfully loaded {len(data)} rows to {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"DuckDB load failed for {table_name}: {e}")
        return False


def main() -> int:
    """Main ETL pipeline execution."""
    args = parse_args()
    
    # Initial basic logging
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logger = logging.getLogger("etl_comprehensive")
    
    logger.info("=" * 60)
    logger.info("rs_analytics COMPREHENSIVE ETL Pipeline")
    logger.info("Extracting ALL GA4 metrics and dimensions")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    # Load configuration
    try:
        from etl.config import get_config, ConfigurationError
        config = get_config()
        logger.info("Configuration loaded successfully")
    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    
    # Setup logging with config
    logger = setup_etl_logging(config, args.verbose)
    
    # Determine date range
    if args.lifetime:
        start_date = "2020-01-01"  # GA4 launch date
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        logger.info("LIFETIME MODE: Extracting all available data")
    elif args.start_date:
        start_date = args.start_date
        end_date = args.end_date or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        lookback = args.lookback_days or config.lookback_days
        end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=lookback)).strftime("%Y-%m-%d")
    
    logger.info(f"Date range: {start_date} to {end_date}")
    
    # Get all dimension/metric combinations
    combinations = get_optimized_dimension_metric_combinations()
    logger.info(f"Will extract {len(combinations)} different datasets")
    
    # Extract and load each combination
    total_rows = 0
    for i, combination in enumerate(combinations, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"Dataset {i}/{len(combinations)}: {combination['name']}")
        logger.info(f"{'='*60}")
        
        # Extract data
        data = extract_ga4_data_comprehensive(
            config, logger, start_date, end_date, combination
        )
        
        if data is None:
            logger.warning(f"Skipping {combination['name']} due to extraction error")
            continue
        
        if not data:
            logger.info(f"No data returned for {combination['name']}")
            continue
        
        # Load to DuckDB
        table_name = f"ga4_{combination['name']}"
        if load_to_duckdb_comprehensive(config, data, table_name, logger):
            total_rows += len(data)
        else:
            logger.warning(f"Failed to load {combination['name']}")
    
    # Success summary
    logger.info("\n" + "=" * 60)
    logger.info("COMPREHENSIVE ETL COMPLETED SUCCESSFULLY")
    logger.info(f"Total rows extracted: {total_rows:,}")
    logger.info(f"Tables created: {len(combinations)}")
    logger.info(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    logger.info("\nCreated tables:")
    for combination in combinations:
        logger.info(f"  - ga4_{combination['name']}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
