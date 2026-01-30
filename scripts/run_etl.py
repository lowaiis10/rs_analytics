#!/usr/bin/env python3
"""
ETL Pipeline Runner for rs_analytics

This script executes the full ETL pipeline:
1. Validates configuration at startup (fail-fast)
2. Extracts data from GA4
3. Transforms and loads data into DuckDB
4. Optionally mirrors to BigQuery

Usage:
    # Run full ETL
    python scripts/run_etl.py
    
    # Run with custom lookback
    python scripts/run_etl.py --lookback-days 30
    
    # Dry run (validate only)
    python scripts/run_etl.py --dry-run

Exit Codes:
    0 - Success
    1 - Configuration error (credentials, paths, etc.)
    2 - GA4 extraction error
    3 - DuckDB load error
    4 - BigQuery mirror error

For cron/systemd:
    Ensure .env file is in the project root with absolute paths.
    No environment variables need to be exported in the cron command.
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run the rs_analytics ETL pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_etl.py                    # Run with default settings
  python scripts/run_etl.py --lookback-days 30 # Last 30 days of data
  python scripts/run_etl.py --dry-run          # Validate config only
        """
    )
    
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Number of days to look back (overrides LOOKBACK_DAYS env var)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration and exit without running ETL"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose (DEBUG) logging"
    )
    
    return parser.parse_args()


def setup_etl_logging(config, verbose: bool = False) -> logging.Logger:
    """
    Set up logging for ETL run.
    
    Args:
        config: Validated configuration object
        verbose: If True, set log level to DEBUG
        
    Returns:
        Configured logger
    """
    from logging.handlers import TimedRotatingFileHandler
    
    # Determine log level
    log_level = logging.DEBUG if verbose else getattr(logging, config.log_level)
    
    # Create logger
    logger = logging.getLogger("etl")
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
    log_file = config.log_dir / f"etl_{datetime.now().strftime('%Y%m%d')}.log"
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


def validate_configuration(logger: logging.Logger) -> Optional[object]:
    """
    Load and validate configuration.
    
    Args:
        logger: Logger instance
        
    Returns:
        Config object if valid, None if invalid
    """
    logger.info("=" * 50)
    logger.info("STEP 1: Validating Configuration")
    logger.info("=" * 50)
    
    try:
        from etl.config import get_config, ConfigurationError
        config = get_config()
        
        logger.info("Configuration validated successfully")
        logger.info(f"  GA4 Property ID: {config.ga4_property_id}")
        logger.info(f"  Credentials: {config.google_credentials_path}")
        logger.info(f"  DuckDB Path: {config.duckdb_path}")
        logger.info(f"  Lookback Days: {config.lookback_days}")
        logger.info(f"  BigQuery Mirror: {'Enabled' if config.enable_bq_mirror else 'Disabled'}")
        
        return config
        
    except ConfigurationError as e:
        logger.error("Configuration validation FAILED")
        logger.error(str(e))
        return None
    except Exception as e:
        logger.error(f"Unexpected error during configuration: {e}")
        return None


def test_ga4_connection(config, logger: logging.Logger) -> bool:
    """
    Test GA4 API connectivity before running full ETL.
    
    Args:
        config: Validated configuration
        logger: Logger instance
        
    Returns:
        True if connection successful, False otherwise
    """
    logger.info("=" * 50)
    logger.info("STEP 2: Testing GA4 Connection")
    logger.info("=" * 50)
    
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import (
            DateRange,
            Dimension,
            Metric,
            RunReportRequest,
        )
        
        # Create client using GOOGLE_APPLICATION_CREDENTIALS
        client = BetaAnalyticsDataClient()
        
        # Minimal test query
        request = RunReportRequest(
            property=f"properties/{config.ga4_property_id}",
            dimensions=[Dimension(name="date")],
            metrics=[Metric(name="sessions")],
            date_ranges=[DateRange(start_date="yesterday", end_date="yesterday")],
        )
        
        response = client.run_report(request)
        
        sessions = "0"
        if response.rows:
            sessions = response.rows[0].metric_values[0].value
        
        logger.info(f"GA4 connection successful (yesterday's sessions: {sessions})")
        return True
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"GA4 connection failed: {error_msg}")
        
        # Provide specific guidance
        if "403" in error_msg or "permission" in error_msg.lower():
            logger.error(
                "Permission denied. Add the service account email to "
                "GA4 → Admin → Property Access Management with 'Viewer' role."
            )
        elif "API" in error_msg and "not been used" in error_msg.lower():
            logger.error(
                "Google Analytics Data API is not enabled. "
                "Enable it at: https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com"
            )
        
        return False


def extract_ga4_data(config, logger: logging.Logger, lookback_days: int) -> Optional[list]:
    """
    Extract data from GA4.
    
    Args:
        config: Validated configuration
        logger: Logger instance
        lookback_days: Number of days to extract
        
    Returns:
        List of data rows, or None if extraction failed
    """
    logger.info("=" * 50)
    logger.info("STEP 3: Extracting GA4 Data")
    logger.info("=" * 50)
    
    try:
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import (
            DateRange,
            Dimension,
            Metric,
            RunReportRequest,
        )
        
        # Calculate date range
        end_date = datetime.now() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=lookback_days - 1)
        
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        logger.info(f"Date range: {start_str} to {end_str} ({lookback_days} days)")
        
        # Create client
        client = BetaAnalyticsDataClient()
        
        # Define dimensions and metrics to extract
        # This is a basic set - expand based on your needs
        dimensions = [
            Dimension(name="date"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium"),
            Dimension(name="deviceCategory"),
            Dimension(name="country"),
        ]
        
        metrics = [
            Metric(name="sessions"),
            Metric(name="activeUsers"),
            Metric(name="newUsers"),
            Metric(name="screenPageViews"),
            Metric(name="averageSessionDuration"),
            Metric(name="bounceRate"),
            Metric(name="engagementRate"),
        ]
        
        logger.info(f"Requesting {len(dimensions)} dimensions, {len(metrics)} metrics")
        
        # Make API request
        request = RunReportRequest(
            property=f"properties/{config.ga4_property_id}",
            dimensions=dimensions,
            metrics=metrics,
            date_ranges=[DateRange(start_date=start_str, end_date=end_str)],
            limit=100000,  # Adjust based on expected data volume
        )
        
        response = client.run_report(request)
        
        # Convert response to list of dicts
        data = []
        dimension_headers = [d.name for d in response.dimension_headers]
        metric_headers = [m.name for m in response.metric_headers]
        
        for row in response.rows:
            record = {}
            
            # Add dimensions
            for i, dim_value in enumerate(row.dimension_values):
                record[dimension_headers[i]] = dim_value.value
            
            # Add metrics
            for i, metric_value in enumerate(row.metric_values):
                record[metric_headers[i]] = metric_value.value
            
            data.append(record)
        
        logger.info(f"Extracted {len(data)} rows from GA4")
        return data
        
    except Exception as e:
        logger.error(f"GA4 extraction failed: {e}")
        return None


def load_to_duckdb(config, data: list, logger: logging.Logger) -> bool:
    """
    Load extracted data into DuckDB.
    
    Args:
        config: Validated configuration
        data: List of data rows to load
        logger: Logger instance
        
    Returns:
        True if load successful, False otherwise
    """
    logger.info("=" * 50)
    logger.info("STEP 4: Loading to DuckDB")
    logger.info("=" * 50)
    
    if not data:
        logger.warning("No data to load")
        return True
    
    try:
        import duckdb
        
        # Connect to DuckDB
        db_path = str(config.duckdb_path)
        logger.info(f"Connecting to DuckDB: {db_path}")
        
        conn = duckdb.connect(db_path)
        
        # Create table if not exists
        # Schema based on GA4 dimensions and metrics we extract
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS ga4_sessions (
            date DATE,
            session_source VARCHAR,
            session_medium VARCHAR,
            device_category VARCHAR,
            country VARCHAR,
            sessions INTEGER,
            active_users INTEGER,
            new_users INTEGER,
            screen_page_views INTEGER,
            avg_session_duration DOUBLE,
            bounce_rate DOUBLE,
            engagement_rate DOUBLE,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, session_source, session_medium, device_category, country)
        )
        """
        conn.execute(create_table_sql)
        logger.info("Ensured ga4_sessions table exists")
        
        # Prepare data for insertion
        # Map API field names to database column names
        field_mapping = {
            'date': 'date',
            'sessionSource': 'session_source',
            'sessionMedium': 'session_medium',
            'deviceCategory': 'device_category',
            'country': 'country',
            'sessions': 'sessions',
            'activeUsers': 'active_users',
            'newUsers': 'new_users',
            'screenPageViews': 'screen_page_views',
            'averageSessionDuration': 'avg_session_duration',
            'bounceRate': 'bounce_rate',
            'engagementRate': 'engagement_rate',
        }
        
        # Transform data
        transformed_data = []
        for row in data:
            transformed_row = {}
            for api_field, db_field in field_mapping.items():
                value = row.get(api_field, '')
                # Convert types as needed
                if db_field == 'date':
                    # GA4 returns dates as YYYYMMDD strings
                    value = f"{value[:4]}-{value[4:6]}-{value[6:8]}" if len(value) == 8 else value
                elif db_field in ('sessions', 'active_users', 'new_users', 'screen_page_views'):
                    value = int(value) if value else 0
                elif db_field in ('avg_session_duration', 'bounce_rate', 'engagement_rate'):
                    value = float(value) if value else 0.0
                transformed_row[db_field] = value
            transformed_data.append(transformed_row)
        
        # Use INSERT OR REPLACE for upsert behavior
        insert_sql = """
        INSERT OR REPLACE INTO ga4_sessions (
            date, session_source, session_medium, device_category, country,
            sessions, active_users, new_users, screen_page_views,
            avg_session_duration, bounce_rate, engagement_rate
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Insert data
        for row in transformed_data:
            conn.execute(insert_sql, [
                row['date'], row['session_source'], row['session_medium'],
                row['device_category'], row['country'], row['sessions'],
                row['active_users'], row['new_users'], row['screen_page_views'],
                row['avg_session_duration'], row['bounce_rate'], row['engagement_rate']
            ])
        
        conn.commit()
        conn.close()
        
        logger.info(f"Successfully loaded {len(transformed_data)} rows to DuckDB")
        return True
        
    except Exception as e:
        logger.error(f"DuckDB load failed: {e}")
        return False


def mirror_to_bigquery(config, data: list, logger: logging.Logger) -> bool:
    """
    Mirror data to BigQuery (if enabled).
    
    Args:
        config: Validated configuration
        data: List of data rows to mirror
        logger: Logger instance
        
    Returns:
        True if mirror successful (or disabled), False if failed
    """
    if not config.enable_bq_mirror:
        logger.info("BigQuery mirroring is disabled, skipping")
        return True
    
    logger.info("=" * 50)
    logger.info("STEP 5: Mirroring to BigQuery")
    logger.info("=" * 50)
    
    # TODO: Implement BigQuery mirroring
    # This is a placeholder for future implementation
    logger.warning("BigQuery mirroring is not yet implemented")
    return True


def main() -> int:
    """
    Main ETL pipeline execution.
    
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    args = parse_args()
    
    # Initial basic logging until config is loaded
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logger = logging.getLogger("etl")
    
    logger.info("=" * 60)
    logger.info("rs_analytics ETL Pipeline")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    # ========================================
    # Step 1: Validate Configuration
    # ========================================
    config = validate_configuration(logger)
    if config is None:
        logger.error("ETL aborted due to configuration errors")
        return 1
    
    # Re-setup logging with config settings
    logger = setup_etl_logging(config, args.verbose)
    
    # Determine lookback days
    lookback_days = args.lookback_days if args.lookback_days else config.lookback_days
    logger.info(f"Using lookback period: {lookback_days} days")
    
    # ========================================
    # Dry Run Check
    # ========================================
    if args.dry_run:
        logger.info("=" * 50)
        logger.info("DRY RUN - Validation only, no data extracted")
        logger.info("=" * 50)
        logger.info("Configuration is valid. Run without --dry-run to execute ETL.")
        return 0
    
    # ========================================
    # Step 2: Test GA4 Connection
    # ========================================
    if not test_ga4_connection(config, logger):
        logger.error("ETL aborted due to GA4 connection failure")
        return 2
    
    # ========================================
    # Step 3: Extract Data
    # ========================================
    data = extract_ga4_data(config, logger, lookback_days)
    if data is None:
        logger.error("ETL aborted due to extraction failure")
        return 2
    
    # ========================================
    # Step 4: Load to DuckDB
    # ========================================
    if not load_to_duckdb(config, data, logger):
        logger.error("ETL aborted due to DuckDB load failure")
        return 3
    
    # ========================================
    # Step 5: Mirror to BigQuery (optional)
    # ========================================
    if not mirror_to_bigquery(config, data, logger):
        logger.error("ETL completed with BigQuery mirror failure")
        return 4
    
    # ========================================
    # Success
    # ========================================
    logger.info("=" * 60)
    logger.info("ETL Pipeline Completed Successfully")
    logger.info(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
