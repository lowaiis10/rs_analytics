#!/usr/bin/env python3
"""
Test Google Ads Connection Script

This script verifies that:
1. Google Ads configuration is valid
2. OAuth credentials in google_ads.yaml are correct
3. Google Ads API is accessible
4. Service account has proper permissions

Usage:
    python scripts/test_gads_connection.py

Exit Codes:
    0 - Success (all checks passed)
    1 - Configuration error
    2 - Google Ads API error (authentication/authorization)
    3 - Account access error

This script is safe to run and will not modify any data.
"""

import sys
from pathlib import Path
import yaml

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
    """Run comprehensive Google Ads connection tests."""
    print_header("rs_analytics - Google Ads Connection Test")
    
    # ========================================
    # Step 1: Load Configuration
    # ========================================
    print("Step 1: Loading Google Ads configuration...")
    
    try:
        from etl.gads_config import get_gads_config
        config = get_gads_config()
        print_success("Configuration loaded successfully")
        print_info(f"YAML Path: {config.yaml_path}")
        print_info(f"Customer ID: {config.customer_id}")
        print_info(f"Login Customer ID: {config.login_customer_id or 'Not set'}")
    except Exception as e:
        print_error(f"Configuration validation failed: {e}")
        return 1
    
    # ========================================
    # Step 2: Verify YAML File
    # ========================================
    print("\nStep 2: Verifying google_ads.yaml file...")
    
    if not config.yaml_path.exists():
        print_error(f"YAML file not found: {config.yaml_path}")
        return 1
    
    print_success(f"YAML file exists")
    
    # Verify YAML structure
    try:
        with open(config.yaml_path, 'r') as f:
            yaml_data = yaml.safe_load(f)
        
        required_fields = ['developer_token', 'client_id', 'client_secret', 'refresh_token']
        present_fields = []
        missing_fields = []
        placeholder_fields = []
        
        for field in required_fields:
            value = yaml_data.get(field)
            if not value:
                missing_fields.append(field)
            elif value == f'YOUR_{field.upper()}' or value.startswith('YOUR_'):
                placeholder_fields.append(field)
            else:
                present_fields.append(field)
        
        if missing_fields:
            print_error(f"YAML missing required fields: {missing_fields}")
            return 1
        
        if placeholder_fields:
            print_error(f"YAML has placeholder values (not filled in): {placeholder_fields}")
            print(f"""
  HOW TO FIX:
  Fill in the following fields in google_ads.yaml:
  {', '.join(placeholder_fields)}
""")
            return 1
        
        print_success("YAML structure is valid")
        print_info(f"Developer Token: {yaml_data.get('developer_token')[:10]}...")
        print_info(f"Client ID: {yaml_data.get('client_id')[:20]}...")
        print_info(f"Login Customer ID: {yaml_data.get('login_customer_id', 'Not set')}")
        
    except yaml.YAMLError as e:
        print_error(f"Invalid YAML format: {e}")
        return 1
    except Exception as e:
        print_error(f"Error reading YAML file: {e}")
        return 1
    
    # ========================================
    # Step 3: Test Client Creation
    # ========================================
    print("\nStep 3: Creating Google Ads client...")
    
    try:
        from etl.gads_config import get_gads_client
        client = get_gads_client()
        print_success("Google Ads client created successfully")
    except Exception as e:
        error_msg = str(e)
        print_error(f"Failed to create client: {error_msg}")
        
        if "refresh_token" in error_msg.lower() or "oauth" in error_msg.lower() or "unauthorized" in error_msg.lower():
            print(f"""
  OAuth Authentication Issue detected: {error_msg}
  
  This usually means your refresh_token is expired or invalid.
  
  HOW TO FIX - Generate a new refresh token:
  
  1. Go to https://developers.google.com/oauthplayground
  
  2. Click the gear icon (Settings) in the top right corner
  
  3. Check "Use your own OAuth credentials"
  
  4. Enter your credentials:
     - Client ID: (from google_ads.yaml)
     - Client Secret: (from google_ads.yaml)
  
  5. In the left panel, scroll to find:
     "Google Ads API" -> select "https://www.googleapis.com/auth/adwords"
  
  6. Click "Authorize APIs" 
     - Sign in with the Google account that has access to Google Ads
     - Grant permission when prompted
  
  7. Click "Exchange authorization code for tokens"
  
  8. Copy the new "Refresh token" value
  
  9. Update google_ads.yaml with the new refresh_token
  
  10. Run this test again
""")
        return 2
    
    # ========================================
    # Step 4: Test API Connection
    # ========================================
    print("\nStep 4: Testing Google Ads API connection...")
    
    try:
        from etl.gads_extractor import GAdsExtractor
        
        extractor = GAdsExtractor(
            client=client,
            customer_id=config.customer_id,
            login_customer_id=config.login_customer_id
        )
        
        success, message = extractor.test_connection()
        
        if success:
            print_success("API connection successful!")
            for line in message.split('\n'):
                if line.strip():
                    print_info(line.strip())
        else:
            print_error("API connection failed")
            print(f"\n{message}")
            return 3
            
    except Exception as e:
        error_msg = str(e)
        print_error(f"API test failed: {error_msg}")
        
        if "PERMISSION_DENIED" in error_msg:
            print(f"""
  Permission Denied - The credentials don't have access to this account.
  
  HOW TO FIX:
  1. Verify GOOGLE_ADS_CUSTOMER_ID is correct (no dashes)
  2. Ensure the OAuth account has access to the Google Ads account
  3. If using a Manager Account (MCA):
     - Set login_customer_id in google_ads.yaml to the MCA ID
     - Set GOOGLE_ADS_CUSTOMER_ID to the child account ID
""")
        elif "DEVELOPER_TOKEN" in error_msg:
            print(f"""
  Developer Token Issue detected.
  
  HOW TO FIX:
  1. If using a test token, you can only access test accounts
  2. Create a test account at:
     https://ads.google.com/intl/en/home/tools/manager-accounts/test-accounts/
  3. For production access, apply for Standard Access in Google Ads API Center
""")
        elif "CUSTOMER_NOT_FOUND" in error_msg:
            print(f"""
  Customer Not Found.
  
  HOW TO FIX:
  1. Verify the Customer ID is correct: {config.customer_id}
  2. The account may not exist or may be suspended
  3. If using a test developer token, create a test account
""")
        
        return 3
    
    # ========================================
    # Step 5: Test Simple Query
    # ========================================
    print("\nStep 5: Running test query...")
    
    try:
        ga_service = client.get_service("GoogleAdsService")
        
        # Query for account info
        query = """
            SELECT
                customer.id,
                customer.descriptive_name,
                customer.currency_code,
                customer.time_zone,
                customer.test_account
            FROM customer
            LIMIT 1
        """
        
        effective_customer_id = config.login_customer_id or config.customer_id
        response = ga_service.search(customer_id=effective_customer_id, query=query)
        
        for row in response:
            customer = row.customer
            print_success("Test query executed successfully!")
            print("\n  --- Account Information ---")
            print(f"  Account Name: {customer.descriptive_name}")
            print(f"  Customer ID: {customer.id}")
            print(f"  Currency: {customer.currency_code}")
            print(f"  Timezone: {customer.time_zone}")
            print(f"  Test Account: {customer.test_account}")
            
            if customer.test_account:
                print("\n  NOTE: This is a TEST account. Data may be limited or simulated.")
                
    except Exception as e:
        print_error(f"Test query failed: {e}")
        return 3
    
    # ========================================
    # Success Summary
    # ========================================
    print_header("All Tests Passed!")
    print(f"""  Your Google Ads credentials are correctly configured.
  
  Summary:
  - Configuration: Valid
  - YAML File: Found and readable
  - Client Creation: Successful
  - API Connection: Confirmed
  - Test Query: Successful
  
  You can now run the Google Ads ETL pipeline:
    python scripts/run_etl_gads.py --lifetime
    
  To run in the dashboard:
    streamlit run app/main.py
""")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
