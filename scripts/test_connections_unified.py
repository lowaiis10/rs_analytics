#!/usr/bin/env python3
"""
Unified Connection Test Script for rs_analytics

This script tests connections to all configured data sources:
- Google Analytics 4 (GA4)
- Google Search Console (GSC)
- Google Ads
- Meta (Facebook) Ads
- Twitter/X

It consolidates individual test scripts into a unified interface while
providing detailed diagnostics for each platform.

Usage:
    # Test all configured sources
    python scripts/test_connections_unified.py --all
    
    # Test specific source
    python scripts/test_connections_unified.py --source ga4
    python scripts/test_connections_unified.py --source gads
    python scripts/test_connections_unified.py --source gsc
    python scripts/test_connections_unified.py --source meta
    python scripts/test_connections_unified.py --source twitter
    
    # Verbose output
    python scripts/test_connections_unified.py --all --verbose

Exit Codes:
    0 - All tests passed
    1 - Configuration error
    2 - Connection error
    3 - Partial failure (some tests passed)
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Tuple

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.utils.test_helpers import (
    print_header,
    print_success,
    print_error,
    print_info,
    print_warning,
    print_step,
    TestResult,
    get_oauth_error_instructions,
    get_permission_error_instructions,
)


# ============================================
# Available Test Sources
# ============================================

AVAILABLE_SOURCES = ['ga4', 'gsc', 'gads', 'meta', 'twitter']


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Test connections to data sources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/test_connections_unified.py --all
  python scripts/test_connections_unified.py --source ga4
  python scripts/test_connections_unified.py --source gads --verbose
        """
    )
    
    parser.add_argument(
        "--source", "-s",
        type=str,
        choices=AVAILABLE_SOURCES,
        help="Test a specific source"
    )
    
    parser.add_argument(
        "--all", "-a",
        action="store_true",
        help="Test all configured sources"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    
    args = parser.parse_args()
    
    if not args.source and not args.all:
        parser.error("Must specify --source or --all")
    
    return args


# ============================================
# GA4 Connection Test
# ============================================

def test_ga4_connection(verbose: bool = False) -> Tuple[bool, str, List[str]]:
    """
    Test Google Analytics 4 connection.
    
    Returns:
        Tuple of (success, message, details)
    """
    details = []
    
    try:
        # Step 1: Load configuration
        from etl.config import get_config
        config = get_config()
        details.append(f"Property ID: {config.ga4_property_id}")
        details.append(f"Credentials: {config.google_credentials_path}")
        
        # Step 2: Test API connection
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        from google.analytics.data_v1beta.types import (
            DateRange, Dimension, Metric, RunReportRequest
        )
        
        client = BetaAnalyticsDataClient()
        
        request = RunReportRequest(
            property=f"properties/{config.ga4_property_id}",
            dimensions=[Dimension(name="date")],
            metrics=[Metric(name="sessions")],
            date_ranges=[DateRange(start_date="yesterday", end_date="yesterday")],
        )
        
        response = client.run_report(request)
        
        sessions = 0
        if response.rows:
            sessions = response.rows[0].metric_values[0].value
        
        details.append(f"Yesterday's sessions: {sessions}")
        
        return True, "GA4 connection successful!", details
        
    except Exception as e:
        error_msg = str(e)
        
        if "403" in error_msg or "permission" in error_msg.lower():
            fix = get_permission_error_instructions("GA4")
            return False, f"Permission denied: {error_msg}", [fix]
        elif "API" in error_msg and "not enabled" in error_msg.lower():
            fix = "Enable the Google Analytics Data API in your GCP project"
            return False, f"API not enabled: {error_msg}", [fix]
        else:
            return False, f"Connection failed: {error_msg}", details


# ============================================
# GSC Connection Test
# ============================================

def test_gsc_connection(verbose: bool = False) -> Tuple[bool, str, List[str]]:
    """
    Test Google Search Console connection.
    
    Returns:
        Tuple of (success, message, details)
    """
    details = []
    
    try:
        # Step 1: Load configuration
        from etl.gsc_config import get_gsc_config
        config = get_gsc_config()
        details.append(f"Site URL: {config.site_url}")
        details.append(f"Credentials: {config.credentials_path}")
        
        # Step 2: Create extractor and test
        from etl.gsc_extractor import GSCExtractor
        
        extractor = GSCExtractor(
            credentials_path=str(config.credentials_path),
            site_url=config.site_url
        )
        
        success, message = extractor.test_connection()
        
        if success:
            details.append(message)
            return True, "GSC connection successful!", details
        else:
            return False, message, details
        
    except Exception as e:
        error_msg = str(e)
        return False, f"Connection failed: {error_msg}", details


# ============================================
# Google Ads Connection Test
# ============================================

def test_gads_connection(verbose: bool = False) -> Tuple[bool, str, List[str]]:
    """
    Test Google Ads connection.
    
    Returns:
        Tuple of (success, message, details)
    """
    details = []
    
    try:
        # Step 1: Load configuration
        from etl.gads_config import get_gads_config, get_gads_client
        config = get_gads_config()
        details.append(f"Customer ID: {config.customer_id}")
        details.append(f"YAML Path: {config.yaml_path}")
        details.append(f"Login Customer ID: {config.login_customer_id or 'Not set'}")
        
        # Step 2: Create client
        client = get_gads_client()
        
        # Step 3: Create extractor and test
        from etl.gads_extractor import GAdsExtractor
        
        extractor = GAdsExtractor(
            client=client,
            customer_id=config.customer_id,
            login_customer_id=config.login_customer_id
        )
        
        success, message = extractor.test_connection()
        
        if success:
            for line in message.split('\n'):
                if line.strip():
                    details.append(line.strip())
            return True, "Google Ads connection successful!", details
        else:
            return False, message, details
        
    except Exception as e:
        error_msg = str(e)
        
        if "refresh_token" in error_msg.lower() or "oauth" in error_msg.lower():
            fix = get_oauth_error_instructions("Google Ads")
            return False, f"OAuth error: {error_msg}", [fix]
        elif "PERMISSION_DENIED" in error_msg:
            fix = get_permission_error_instructions("Google Ads")
            return False, f"Permission denied: {error_msg}", [fix]
        else:
            return False, f"Connection failed: {error_msg}", details


# ============================================
# Meta Ads Connection Test
# ============================================

def test_meta_connection(verbose: bool = False) -> Tuple[bool, str, List[str]]:
    """
    Test Meta (Facebook) Ads connection.
    
    Returns:
        Tuple of (success, message, details)
    """
    details = []
    
    try:
        # Step 1: Load configuration
        from etl.meta_config import get_meta_config
        config = get_meta_config()
        details.append(f"Access Token: {config.access_token[:20]}...")
        details.append(f"Ad Accounts: {config.ad_account_ids}")
        
        # Step 2: Create extractor and test each account
        from etl.meta_extractor import MetaExtractor
        
        all_success = True
        for account_id in config.ad_account_ids:
            extractor = MetaExtractor(
                access_token=config.access_token,
                ad_account_id=account_id
            )
            
            success, message = extractor.test_connection()
            
            if success:
                details.append(f"Account {account_id}: {message}")
            else:
                details.append(f"Account {account_id}: FAILED - {message}")
                all_success = False
        
        if all_success:
            return True, "Meta Ads connection successful!", details
        else:
            return False, "Some Meta accounts failed", details
        
    except Exception as e:
        error_msg = str(e)
        
        if "access token" in error_msg.lower() or "oauth" in error_msg.lower():
            fix = "Your Meta access token may be expired. Generate a new one in Meta Business Suite."
            return False, f"Token error: {error_msg}", [fix]
        else:
            return False, f"Connection failed: {error_msg}", details


# ============================================
# Twitter Connection Test
# ============================================

def test_twitter_connection(verbose: bool = False) -> Tuple[bool, str, List[str]]:
    """
    Test Twitter/X connection.
    
    Returns:
        Tuple of (success, message, details)
    """
    details = []
    
    try:
        # Step 1: Load configuration
        from etl.twitter_config import get_twitter_config
        config = get_twitter_config()
        details.append(f"Username: @{config.username}")
        details.append(f"Bearer Token: {config.bearer_token[:20]}...")
        
        # Step 2: Create extractor and test
        from etl.twitter_extractor import TwitterExtractor
        
        extractor = TwitterExtractor(
            bearer_token=config.bearer_token,
            consumer_key=config.consumer_key,
            consumer_secret=config.consumer_secret,
            access_token=config.access_token,
            access_token_secret=config.access_token_secret,
            username=config.username
        )
        
        success, message = extractor.test_connection()
        
        if success:
            details.append(message)
            return True, "Twitter connection successful!", details
        else:
            return False, message, details
        
    except Exception as e:
        error_msg = str(e)
        return False, f"Connection failed: {error_msg}", details


# ============================================
# Main Test Runner
# ============================================

def run_test(source: str, verbose: bool = False) -> Tuple[bool, str, List[str]]:
    """
    Run connection test for a specific source.
    
    Args:
        source: Source identifier (ga4, gsc, gads, meta, twitter)
        verbose: Enable verbose output
        
    Returns:
        Tuple of (success, message, details)
    """
    test_functions = {
        'ga4': test_ga4_connection,
        'gsc': test_gsc_connection,
        'gads': test_gads_connection,
        'meta': test_meta_connection,
        'twitter': test_twitter_connection,
    }
    
    if source not in test_functions:
        return False, f"Unknown source: {source}", []
    
    return test_functions[source](verbose)


def main() -> int:
    """Run connection tests."""
    args = parse_args()
    
    print_header("rs_analytics - Connection Tests")
    
    # Determine which sources to test
    if args.all:
        sources = AVAILABLE_SOURCES
        print(f"Testing all sources: {', '.join(sources)}")
    else:
        sources = [args.source]
        print(f"Testing: {args.source}")
    
    print()
    
    # Run tests
    results: Dict[str, Tuple[bool, str, List[str]]] = {}
    
    for i, source in enumerate(sources, 1):
        source_names = {
            'ga4': 'Google Analytics 4',
            'gsc': 'Google Search Console',
            'gads': 'Google Ads',
            'meta': 'Meta (Facebook) Ads',
            'twitter': 'Twitter/X',
        }
        
        print_step(i, f"Testing {source_names.get(source, source)}")
        
        try:
            success, message, details = run_test(source, args.verbose)
            results[source] = (success, message, details)
            
            if success:
                print_success(message)
            else:
                print_error(message)
            
            if args.verbose and details:
                for detail in details:
                    print_info(detail)
            
        except Exception as e:
            results[source] = (False, str(e), [])
            print_error(f"Test failed: {e}")
        
        print()
    
    # Summary
    print_header("Test Results Summary")
    
    passed = sum(1 for success, _, _ in results.values() if success)
    failed = len(results) - passed
    
    print(f"  Passed: {passed}/{len(results)}")
    print(f"  Failed: {failed}/{len(results)}")
    print()
    
    for source, (success, message, details) in results.items():
        status = "[OK]" if success else "[X]"
        source_name = {
            'ga4': 'Google Analytics 4',
            'gsc': 'Google Search Console',
            'gads': 'Google Ads',
            'meta': 'Meta Ads',
            'twitter': 'Twitter/X',
        }.get(source, source)
        
        print(f"  {status} {source_name}")
    
    print()
    
    # Show failed test details
    if failed > 0:
        print("Failed Tests Details:")
        print("-" * 40)
        
        for source, (success, message, details) in results.items():
            if not success:
                print(f"\n  {source.upper()}:")
                print(f"    Error: {message}")
                if details:
                    for detail in details:
                        print(f"    {detail}")
    
    # Return appropriate exit code
    if failed == 0:
        print("\nAll tests passed! Your credentials are correctly configured.")
        return 0
    elif passed > 0:
        print("\nSome tests failed. Check the details above for fixes.")
        return 3
    else:
        print("\nAll tests failed. Check your configuration.")
        return 2


if __name__ == "__main__":
    sys.exit(main())
