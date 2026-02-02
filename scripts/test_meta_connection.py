"""
Meta (Facebook) Ads API Connection Test Script

This script tests the Meta Ads API connection and discovers:
1. Token validity and permissions
2. Associated business accounts
3. Ad accounts accessible with the token
4. Sample ad data (if available)

Usage:
    python scripts/test_meta_connection.py

Requirements:
    - META_ACCESS_TOKEN in .env file
    - Token must have: ads_read, ads_management, business_management permissions

Security Note:
    - Never commit tokens to git
    - Use short-lived tokens for testing
    - Rotate tokens regularly
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple

# Fix Windows console encoding for special characters
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
from dotenv import load_dotenv
load_dotenv(project_root / ".env")


def get_meta_token() -> Optional[str]:
    """
    Retrieve Meta access token from environment.
    
    Returns:
        str: The access token if found, None otherwise
    """
    token = os.getenv("META_ACCESS_TOKEN")
    
    if not token:
        print("❌ ERROR: META_ACCESS_TOKEN not found in .env file")
        print("\nTo fix this:")
        print("1. Open .env file")
        print("2. Set META_ACCESS_TOKEN=your_token_here")
        return None
    
    if token.startswith("your_") or len(token) < 50:
        print("❌ ERROR: META_ACCESS_TOKEN appears to be a placeholder")
        print("Please set a valid token in .env")
        return None
    
    return token


def test_token_validity(token: str) -> Tuple[bool, Dict]:
    """
    Test if the token is valid by calling the debug_token endpoint.
    
    Args:
        token: Meta access token
        
    Returns:
        Tuple of (success, token_info_dict)
    """
    print("\n" + "=" * 60)
    print("🔐 STEP 1: Testing Token Validity")
    print("=" * 60)
    
    try:
        from facebook_business.api import FacebookAdsApi
        from facebook_business.adobjects.user import User
        
        # Initialize the API
        FacebookAdsApi.init(access_token=token)
        
        # Get user info to validate token
        me = User(fbid='me')
        user_data = me.api_get(fields=['id', 'name'])
        
        print(f"✅ Token is valid!")
        print(f"   User ID: {user_data.get('id', 'Unknown')}")
        print(f"   User Name: {user_data.get('name', 'Unknown')}")
        
        return True, dict(user_data)
        
    except Exception as e:
        print(f"❌ Token validation failed: {str(e)}")
        return False, {}


def get_token_permissions(token: str) -> List[str]:
    """
    Get the permissions granted to this token.
    
    Args:
        token: Meta access token
        
    Returns:
        List of permission names
    """
    print("\n" + "=" * 60)
    print("🔑 STEP 2: Checking Token Permissions")
    print("=" * 60)
    
    try:
        import requests
        
        # Use the debug_token endpoint
        url = f"https://graph.facebook.com/v21.0/me/permissions"
        params = {"access_token": token}
        
        response = requests.get(url, params=params)
        data = response.json()
        
        if "error" in data:
            print(f"❌ Error getting permissions: {data['error'].get('message', 'Unknown error')}")
            return []
        
        permissions = data.get("data", [])
        granted = [p["permission"] for p in permissions if p.get("status") == "granted"]
        
        print(f"✅ Found {len(granted)} granted permissions:")
        for perm in sorted(granted):
            # Highlight ads-related permissions
            if "ads" in perm.lower() or "business" in perm.lower():
                print(f"   ⭐ {perm}")
            else:
                print(f"   • {perm}")
        
        # Check for required permissions
        required = ["ads_read", "ads_management", "business_management"]
        missing = [p for p in required if p not in granted]
        
        if missing:
            print(f"\n⚠️  Missing recommended permissions: {', '.join(missing)}")
        else:
            print(f"\n✅ All required permissions are granted!")
        
        return granted
        
    except Exception as e:
        print(f"❌ Failed to get permissions: {str(e)}")
        return []


def discover_businesses(token: str) -> List[Dict]:
    """
    Discover business accounts accessible with this token.
    
    Args:
        token: Meta access token
        
    Returns:
        List of business account dictionaries
    """
    print("\n" + "=" * 60)
    print("🏢 STEP 3: Discovering Business Accounts")
    print("=" * 60)
    
    try:
        from facebook_business.api import FacebookAdsApi
        from facebook_business.adobjects.user import User
        
        FacebookAdsApi.init(access_token=token)
        
        me = User(fbid='me')
        businesses = me.get_businesses(fields=['id', 'name', 'verification_status'])
        
        business_list = []
        for biz in businesses:
            business_list.append({
                'id': biz.get('id'),
                'name': biz.get('name'),
                'verification_status': biz.get('verification_status', 'Unknown')
            })
        
        if business_list:
            print(f"✅ Found {len(business_list)} business account(s):")
            for biz in business_list:
                print(f"   • {biz['name']} (ID: {biz['id']})")
                print(f"     Verification: {biz['verification_status']}")
        else:
            print("⚠️  No business accounts found")
            print("   This token may be for personal ad accounts only")
        
        return business_list
        
    except Exception as e:
        print(f"❌ Failed to discover businesses: {str(e)}")
        return []


def discover_ad_accounts(token: str) -> List[Dict]:
    """
    Discover ad accounts accessible with this token.
    
    Args:
        token: Meta access token
        
    Returns:
        List of ad account dictionaries
    """
    print("\n" + "=" * 60)
    print("📊 STEP 4: Discovering Ad Accounts")
    print("=" * 60)
    
    try:
        from facebook_business.api import FacebookAdsApi
        from facebook_business.adobjects.user import User
        
        FacebookAdsApi.init(access_token=token)
        
        me = User(fbid='me')
        ad_accounts = me.get_ad_accounts(fields=[
            'id', 
            'name', 
            'account_status',
            'currency',
            'amount_spent',
            'balance'
        ])
        
        account_list = []
        for account in ad_accounts:
            # Convert amount_spent from cents to dollars
            amount_spent = float(account.get('amount_spent', 0)) / 100
            balance = float(account.get('balance', 0)) / 100
            
            account_list.append({
                'id': account.get('id'),
                'name': account.get('name', 'Unnamed'),
                'status': account.get('account_status'),
                'currency': account.get('currency', 'USD'),
                'amount_spent': amount_spent,
                'balance': balance
            })
        
        if account_list:
            print(f"✅ Found {len(account_list)} ad account(s):\n")
            
            # Status mapping
            status_map = {
                1: "✅ ACTIVE",
                2: "❌ DISABLED",
                3: "⚠️ UNSETTLED",
                7: "⏸️ PENDING_RISK_REVIEW",
                8: "⏸️ PENDING_SETTLEMENT",
                9: "🔒 IN_GRACE_PERIOD",
                100: "⏸️ PENDING_CLOSURE",
                101: "🚫 CLOSED",
                201: "⚠️ ANY_ACTIVE",
                202: "⚠️ ANY_CLOSED"
            }
            
            for acc in account_list:
                status_text = status_map.get(acc['status'], f"Unknown ({acc['status']})")
                print(f"   📁 {acc['name']}")
                print(f"      ID: {acc['id']}")
                print(f"      Status: {status_text}")
                print(f"      Currency: {acc['currency']}")
                print(f"      Total Spent: {acc['currency']} {acc['amount_spent']:,.2f}")
                print(f"      Balance: {acc['currency']} {acc['balance']:,.2f}")
                print()
            
            # Provide the first active account ID for .env
            active_accounts = [a for a in account_list if a['status'] == 1]
            if active_accounts:
                print(f"💡 TIP: Add this to your .env file:")
                print(f"   META_AD_ACCOUNT_ID={active_accounts[0]['id']}")
        else:
            print("⚠️  No ad accounts found")
            print("   Make sure the token has ads_read permission")
        
        return account_list
        
    except Exception as e:
        print(f"❌ Failed to discover ad accounts: {str(e)}")
        return []


def test_ads_data_pull(token: str, ad_account_id: Optional[str] = None) -> bool:
    """
    Test pulling actual ads data from an ad account.
    
    Args:
        token: Meta access token
        ad_account_id: Optional specific ad account ID to use
        
    Returns:
        bool: True if data pull was successful
    """
    print("\n" + "=" * 60)
    print("📈 STEP 5: Testing Ads Data Pull")
    print("=" * 60)
    
    try:
        from facebook_business.api import FacebookAdsApi
        from facebook_business.adobjects.user import User
        from facebook_business.adobjects.adaccount import AdAccount
        
        FacebookAdsApi.init(access_token=token)
        
        # Get ad account ID
        if not ad_account_id:
            ad_account_id = os.getenv("META_AD_ACCOUNT_ID")
        
        if not ad_account_id:
            # Try to get the first available ad account
            me = User(fbid='me')
            ad_accounts = list(me.get_ad_accounts(fields=['id']))
            if ad_accounts:
                ad_account_id = ad_accounts[0].get('id')
            else:
                print("❌ No ad account ID available")
                print("   Set META_AD_ACCOUNT_ID in .env or pass as parameter")
                return False
        
        # Ensure the ID has the 'act_' prefix
        if not ad_account_id.startswith('act_'):
            ad_account_id = f"act_{ad_account_id}"
        
        print(f"Using ad account: {ad_account_id}")
        
        account = AdAccount(ad_account_id)
        
        # Test 1: Get campaigns
        print("\n📋 Fetching campaigns...")
        campaigns = account.get_campaigns(fields=[
            'id',
            'name', 
            'status',
            'objective',
            'created_time'
        ])
        
        campaign_list = list(campaigns)
        print(f"   Found {len(campaign_list)} campaign(s)")
        
        if campaign_list:
            for camp in campaign_list[:5]:  # Show first 5
                print(f"   • {camp.get('name', 'Unnamed')} ({camp.get('status', 'Unknown')})")
            if len(campaign_list) > 5:
                print(f"   ... and {len(campaign_list) - 5} more")
        
        # Test 2: Get insights (last 30 days)
        print("\n📊 Fetching account insights (last 30 days)...")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        insights = account.get_insights(
            fields=[
                'impressions',
                'clicks',
                'spend',
                'ctr',
                'cpc',
                'reach'
            ],
            params={
                'time_range': {
                    'since': start_date.strftime('%Y-%m-%d'),
                    'until': end_date.strftime('%Y-%m-%d')
                },
                'level': 'account'
            }
        )
        
        insights_list = list(insights)
        
        if insights_list:
            data = insights_list[0]
            print(f"\n   📈 Account Performance (Last 30 Days):")
            print(f"   ┌─────────────────────────────────────")
            print(f"   │ Impressions: {int(data.get('impressions', 0)):,}")
            print(f"   │ Reach:       {int(data.get('reach', 0)):,}")
            print(f"   │ Clicks:      {int(data.get('clicks', 0)):,}")
            print(f"   │ CTR:         {float(data.get('ctr', 0)):.2f}%")
            print(f"   │ Spend:       ${float(data.get('spend', 0)):,.2f}")
            print(f"   │ CPC:         ${float(data.get('cpc', 0)):.2f}")
            print(f"   └─────────────────────────────────────")
        else:
            print("   ⚠️  No insights data available for this period")
            print("   (This is normal if no ads ran in the last 30 days)")
        
        print("\n✅ Ads data pull successful!")
        return True
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Failed to pull ads data: {error_msg}")
        
        # Provide helpful error messages
        if "OAuthException" in error_msg:
            print("\n💡 This might be a permission issue.")
            print("   Ensure your token has 'ads_read' permission.")
        elif "Invalid OAuth" in error_msg or "expired" in error_msg.lower():
            print("\n💡 Your token may have expired.")
            print("   Generate a new token from Meta Business Suite.")
        
        return False


def main():
    """Main entry point for Meta connection test."""
    
    print("\n" + "=" * 60)
    print("🔵 META (FACEBOOK) ADS API CONNECTION TEST")
    print("=" * 60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 0: Get token
    token = get_meta_token()
    if not token:
        print("\n❌ Test aborted: No valid token found")
        sys.exit(1)
    
    print(f"Token: {token[:20]}...{token[-10:]} (truncated for security)")
    
    # Step 1: Validate token
    valid, user_info = test_token_validity(token)
    if not valid:
        print("\n❌ Test aborted: Invalid token")
        sys.exit(1)
    
    # Step 2: Check permissions
    permissions = get_token_permissions(token)
    
    # Step 3: Discover businesses
    businesses = discover_businesses(token)
    
    # Step 4: Discover ad accounts
    ad_accounts = discover_ad_accounts(token)
    
    # Step 5: Test data pull
    # Prefer the account from .env, otherwise use first active account
    env_account_id = os.getenv("META_AD_ACCOUNT_ID")
    if env_account_id:
        test_ads_data_pull(token, env_account_id)
    elif ad_accounts:
        # Use the first active account
        active = [a for a in ad_accounts if a['status'] == 1]
        account_id = active[0]['id'] if active else ad_accounts[0]['id']
        test_ads_data_pull(token, account_id)
    else:
        print("\n⚠️  Skipping data pull test: No ad accounts found")
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 TEST SUMMARY")
    print("=" * 60)
    print(f"✅ Token Valid: Yes")
    print(f"✅ Permissions: {len(permissions)} granted")
    print(f"{'✅' if businesses else '⚠️ '} Businesses: {len(businesses)} found")
    print(f"{'✅' if ad_accounts else '⚠️ '} Ad Accounts: {len(ad_accounts)} found")
    
    if ad_accounts:
        active = [a for a in ad_accounts if a['status'] == 1]
        print(f"   └─ Active: {len(active)}")
    
    print("\n🎉 Meta Ads API connection test complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
