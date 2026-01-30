#!/usr/bin/env python3
"""
Test Google Search Console Connection Script

This script verifies that:
1. GSC configuration is valid
2. Service account credentials are properly set up
3. Search Console API is enabled
4. Service account has access to the specified property

Usage:
    python scripts/test_gsc_connection.py

Exit Codes:
    0 - Success (all checks passed)
    1 - Configuration error
    2 - GSC API error (authentication/authorization)
    3 - Property access error

This script is safe to run and will not modify any data.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path
import json

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def print_header(title: str) -> None:
    """Print a formatted section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def print_success(message: str) -> None:
    """Print a success message with checkmark."""
    print(f"  [OK] {message}")


def print_error(message: str) -> None:
    """Print an error message with X mark."""
    print(f"  [X] {message}")


def print_info(message: str) -> None:
    """Print an info message."""
    print(f"  [i] {message}")


def main() -> int:
    """Run comprehensive GSC connection tests."""
    print_header("rs_analytics - GSC Connection Test")
    
    # ========================================
    # Step 1: Load Configuration
    # ========================================
    print("Step 1: Loading GSC configuration...")
    
    try:
        from etl.gsc_config import get_gsc_config, ConfigurationError
        config = get_gsc_config()
        print_success("Configuration loaded successfully")
        print_info(f"Site URL: {config.site_url}")
        print_info(f"Credentials Path: {config.credentials_path}")
    except ConfigurationError as e:
        print_error("Configuration validation failed")
        print(e)
        return 1
    except Exception as e:
        print_error(f"Unexpected error loading configuration: {e}")
        return 1
    
    # ========================================
    # Step 2: Verify Credentials File
    # ========================================
    print("\nStep 2: Verifying credentials file...")
    
    if not config.credentials_path.exists():
        print_error(f"Credentials file not found: {config.credentials_path}")
        return 1
    
    print_success(f"Credentials file exists")
    
    # Verify JSON structure
    try:
        with open(config.credentials_path, 'r') as f:
            creds_data = json.load(f)
        
        required_fields = ['type', 'project_id', 'client_email', 'private_key']
        missing_fields = [f for f in required_fields if f not in creds_data]
        
        if missing_fields:
            print_error(f"Credentials JSON missing fields: {missing_fields}")
            return 1
        
        if creds_data.get('type') != 'service_account':
            print_error(f"Invalid credential type: '{creds_data.get('type')}'. Expected 'service_account'")
            return 1
        
        print_success("Credentials JSON structure is valid")
        print_info(f"Service Account Type: {creds_data.get('type')}")
        print_info(f"GCP Project: {creds_data.get('project_id')}")
        print_info(f"Service Account Email: {creds_data.get('client_email')}")
        
        service_account_email = creds_data.get('client_email')
        
    except json.JSONDecodeError as e:
        print_error(f"Invalid JSON in credentials file: {e}")
        return 1
    except Exception as e:
        print_error(f"Error reading credentials file: {e}")
        return 1
    
    # ========================================
    # Step 3: Test API Authentication
    # ========================================
    print("\nStep 3: Testing GSC API authentication...")
    
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        
        credentials = service_account.Credentials.from_service_account_file(
            str(config.credentials_path),
            scopes=['https://www.googleapis.com/auth/webmasters.readonly']
        )
        
        print_success("Service account credentials created")
        
        service = build('searchconsole', 'v1', credentials=credentials)
        print_success("GSC API client created")
        
    except ImportError as e:
        print_error(f"Missing required package: {e}")
        print_info("Install with: pip install google-api-python-client")
        return 2
    except Exception as e:
        print_error(f"Failed to authenticate: {e}")
        return 2
    
    # ========================================
    # Step 4: List Available Sites
    # ========================================
    print("\nStep 4: Listing available Search Console sites...")
    
    try:
        site_list = service.sites().list().execute()
        sites = site_list.get('siteEntry', [])
        
        if not sites:
            print_error("No Search Console sites accessible to this service account")
            print(f"""
  The service account has no access to any Search Console properties.
  
  HOW TO FIX:
  1. Go to: https://search.google.com/search-console
  2. Select your property: {config.site_url}
  3. Go to Settings → Users and permissions
  4. Click 'Add user'
  5. Enter: {service_account_email}
  6. Select 'Full' or 'Owner' permission
  7. Click 'Add'
  8. Wait a few minutes and run this test again
""")
            return 3
        
        print_success(f"Found {len(sites)} accessible site(s)")
        
        # List sites
        print_info("Available sites:")
        for site in sites:
            url = site.get('siteUrl', 'Unknown')
            permission = site.get('permissionLevel', 'Unknown')
            print(f"      - {url} (permission: {permission})")
        
        # Check if our site is in the list
        site_urls = [s.get('siteUrl', '') for s in sites]
        
        # Handle trailing slash variations
        target_url = config.site_url
        target_variations = [
            target_url,
            target_url.rstrip('/'),
            target_url + '/',
        ]
        
        site_found = any(v in site_urls for v in target_variations)
        
        if not site_found:
            print_error(f"Configured site not found: {config.site_url}")
            print_info("Update GSC_SITE_URL in .env to match one of the available sites")
            return 3
        
        print_success(f"Configured site is accessible: {config.site_url}")
        
    except Exception as e:
        error_msg = str(e)
        
        if "403" in error_msg or "permission" in error_msg.lower():
            print_error("Permission denied - API access issue")
            print(f"""
  HOW TO FIX:
  1. Enable Search Console API:
     https://console.cloud.google.com/apis/library/searchconsole.googleapis.com
  2. Ensure the service account has GSC access
""")
        elif "API" in error_msg:
            print_error("Search Console API is not enabled")
            print(f"""
  HOW TO FIX:
  1. Go to: https://console.cloud.google.com/apis/library/searchconsole.googleapis.com
  2. Select project: {creds_data.get('project_id')}
  3. Click 'Enable'
  4. Wait a few minutes and try again
""")
        else:
            print_error(f"Failed to list sites: {e}")
        return 2
    
    # ========================================
    # Step 5: Run Test Query
    # ========================================
    print("\nStep 5: Running test query...")
    
    try:
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        # Find the correct site URL to use
        site_url_to_use = config.site_url
        for url in site_urls:
            if config.site_url.rstrip('/') in url or url in config.site_url:
                site_url_to_use = url
                break
        
        print_info(f"Querying: {site_url_to_use}")
        print_info(f"Date range: {week_ago} to {yesterday}")
        
        response = service.searchanalytics().query(
            siteUrl=site_url_to_use,
            body={
                'startDate': week_ago,
                'endDate': yesterday,
                'dimensions': ['date'],
                'rowLimit': 10
            }
        ).execute()
        
        rows = response.get('rows', [])
        
        print_success("GSC API query executed successfully!")
        
        if rows:
            total_clicks = sum(r.get('clicks', 0) for r in rows)
            total_impressions = sum(r.get('impressions', 0) for r in rows)
            avg_position = sum(r.get('position', 0) for r in rows) / len(rows) if rows else 0
            
            print("\n  --- Last 7 Days Summary ---")
            print(f"  Total Clicks: {total_clicks:,}")
            print(f"  Total Impressions: {total_impressions:,}")
            print(f"  Average Position: {avg_position:.1f}")
        else:
            print_info("No data returned for the last 7 days (this may be normal for new properties)")
        
    except Exception as e:
        print_error(f"Test query failed: {e}")
        return 2
    
    # ========================================
    # Success Summary
    # ========================================
    print_header("All Tests Passed!")
    print(f"""  Your GSC credentials are correctly configured.
  
  Summary:
  - Configuration: Valid
  - Credentials File: Found and readable
  - Service Account: Valid JSON structure
  - GSC API: Enabled and accessible
  - Property Access: Confirmed
  
  You can now run the GSC ETL pipeline:
    python scripts/run_etl_gsc.py --lifetime
    
  To run in the dashboard:
    streamlit run app/main.py
""")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
