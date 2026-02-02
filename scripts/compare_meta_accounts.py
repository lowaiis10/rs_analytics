"""
Meta Ads Account Comparison Script

Pulls and compares ads data from multiple Meta ad accounts,
showing campaigns, ad sets, and performance metrics segmented by account.

Usage:
    python scripts/compare_meta_accounts.py

Requirements:
    - META_ACCESS_TOKEN in .env file
    - Ad account IDs passed as arguments or configured in script
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List

# Fix Windows console encoding for special characters
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
from dotenv import load_dotenv
load_dotenv(project_root / ".env")

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad


# ============================================
# Configuration - Your Ad Account IDs
# ============================================
AD_ACCOUNTS = [
    {
        "id": "act_1368909211583634",
        "name": "[D.M] ReadyServer Ads"
    },
    {
        "id": "act_727951759704777",
        "name": "Ready Server Pte Ltd"
    }
]


def get_account_info(account_id: str) -> Dict:
    """
    Get detailed account information.
    
    Args:
        account_id: Meta ad account ID (with act_ prefix)
        
    Returns:
        Dictionary with account details
    """
    account = AdAccount(account_id)
    
    try:
        info = account.api_get(fields=[
            'id',
            'name',
            'account_status',
            'currency',
            'amount_spent',
            'balance',
            'spend_cap',
            'business_name',
            'timezone_name'
        ])
        
        return {
            'id': info.get('id'),
            'name': info.get('name', 'Unnamed'),
            'status': info.get('account_status'),
            'currency': info.get('currency', 'USD'),
            'amount_spent': float(info.get('amount_spent', 0)) / 100,
            'balance': float(info.get('balance', 0)) / 100,
            'spend_cap': float(info.get('spend_cap', 0)) / 100 if info.get('spend_cap') else None,
            'business_name': info.get('business_name'),
            'timezone': info.get('timezone_name')
        }
    except Exception as e:
        return {'id': account_id, 'error': str(e)}


def get_campaigns(account_id: str) -> List[Dict]:
    """
    Get all campaigns for an account.
    
    Args:
        account_id: Meta ad account ID
        
    Returns:
        List of campaign dictionaries
    """
    account = AdAccount(account_id)
    
    try:
        campaigns = account.get_campaigns(fields=[
            'id',
            'name',
            'status',
            'objective',
            'created_time',
            'start_time',
            'stop_time',
            'daily_budget',
            'lifetime_budget'
        ])
        
        campaign_list = []
        for camp in campaigns:
            campaign_list.append({
                'id': camp.get('id'),
                'name': camp.get('name', 'Unnamed'),
                'status': camp.get('status'),
                'objective': camp.get('objective'),
                'created': camp.get('created_time'),
                'daily_budget': float(camp.get('daily_budget', 0)) / 100 if camp.get('daily_budget') else None,
                'lifetime_budget': float(camp.get('lifetime_budget', 0)) / 100 if camp.get('lifetime_budget') else None
            })
        
        return campaign_list
    except Exception as e:
        return [{'error': str(e)}]


def get_ad_sets(account_id: str) -> List[Dict]:
    """
    Get all ad sets for an account.
    
    Args:
        account_id: Meta ad account ID
        
    Returns:
        List of ad set dictionaries
    """
    account = AdAccount(account_id)
    
    try:
        ad_sets = account.get_ad_sets(fields=[
            'id',
            'name',
            'status',
            'campaign_id',
            'daily_budget',
            'lifetime_budget',
            'targeting',
            'optimization_goal',
            'billing_event'
        ])
        
        adset_list = []
        for adset in ad_sets:
            targeting = adset.get('targeting', {})
            geo_locations = targeting.get('geo_locations', {})
            countries = geo_locations.get('countries', [])
            
            adset_list.append({
                'id': adset.get('id'),
                'name': adset.get('name', 'Unnamed'),
                'status': adset.get('status'),
                'campaign_id': adset.get('campaign_id'),
                'daily_budget': float(adset.get('daily_budget', 0)) / 100 if adset.get('daily_budget') else None,
                'optimization_goal': adset.get('optimization_goal'),
                'billing_event': adset.get('billing_event'),
                'countries': countries
            })
        
        return adset_list
    except Exception as e:
        return [{'error': str(e)}]


def get_ads(account_id: str) -> List[Dict]:
    """
    Get all ads for an account.
    
    Args:
        account_id: Meta ad account ID
        
    Returns:
        List of ad dictionaries
    """
    account = AdAccount(account_id)
    
    try:
        ads = account.get_ads(fields=[
            'id',
            'name',
            'status',
            'campaign_id',
            'adset_id',
            'creative',
            'created_time'
        ])
        
        ad_list = []
        for ad in ads:
            ad_list.append({
                'id': ad.get('id'),
                'name': ad.get('name', 'Unnamed'),
                'status': ad.get('status'),
                'campaign_id': ad.get('campaign_id'),
                'adset_id': ad.get('adset_id'),
                'created': ad.get('created_time')
            })
        
        return ad_list
    except Exception as e:
        return [{'error': str(e)}]


def get_account_insights(account_id: str, days: int = 30) -> Dict:
    """
    Get performance insights for an account.
    
    Args:
        account_id: Meta ad account ID
        days: Number of days to look back
        
    Returns:
        Dictionary with performance metrics
    """
    account = AdAccount(account_id)
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    try:
        insights = account.get_insights(
            fields=[
                'impressions',
                'clicks',
                'spend',
                'ctr',
                'cpc',
                'cpm',
                'reach',
                'frequency',
                'actions',
                'cost_per_action_type'
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
            
            # Parse actions for conversions
            actions = data.get('actions', [])
            installs = 0
            purchases = 0
            for action in actions:
                if action.get('action_type') == 'app_install':
                    installs = int(action.get('value', 0))
                elif action.get('action_type') == 'purchase':
                    purchases = int(action.get('value', 0))
            
            return {
                'impressions': int(data.get('impressions', 0)),
                'reach': int(data.get('reach', 0)),
                'clicks': int(data.get('clicks', 0)),
                'ctr': float(data.get('ctr', 0)),
                'spend': float(data.get('spend', 0)),
                'cpc': float(data.get('cpc', 0)),
                'cpm': float(data.get('cpm', 0)),
                'frequency': float(data.get('frequency', 0)),
                'app_installs': installs,
                'purchases': purchases
            }
        else:
            return {'no_data': True}
            
    except Exception as e:
        return {'error': str(e)}


def get_campaign_insights(account_id: str, days: int = 30) -> List[Dict]:
    """
    Get performance insights broken down by campaign.
    
    Args:
        account_id: Meta ad account ID
        days: Number of days to look back
        
    Returns:
        List of campaign insight dictionaries
    """
    account = AdAccount(account_id)
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    try:
        insights = account.get_insights(
            fields=[
                'campaign_id',
                'campaign_name',
                'impressions',
                'clicks',
                'spend',
                'ctr',
                'cpc',
                'reach',
                'actions'
            ],
            params={
                'time_range': {
                    'since': start_date.strftime('%Y-%m-%d'),
                    'until': end_date.strftime('%Y-%m-%d')
                },
                'level': 'campaign'
            }
        )
        
        campaign_insights = []
        for data in insights:
            # Parse actions
            actions = data.get('actions', [])
            installs = 0
            for action in actions:
                if action.get('action_type') == 'app_install':
                    installs = int(action.get('value', 0))
            
            campaign_insights.append({
                'campaign_id': data.get('campaign_id'),
                'campaign_name': data.get('campaign_name', 'Unnamed'),
                'impressions': int(data.get('impressions', 0)),
                'clicks': int(data.get('clicks', 0)),
                'spend': float(data.get('spend', 0)),
                'ctr': float(data.get('ctr', 0)),
                'cpc': float(data.get('cpc', 0)),
                'reach': int(data.get('reach', 0)),
                'app_installs': installs
            })
        
        # Sort by spend descending
        campaign_insights.sort(key=lambda x: x['spend'], reverse=True)
        return campaign_insights
        
    except Exception as e:
        return [{'error': str(e)}]


def print_separator(title: str, char: str = "=", width: int = 80):
    """Print a formatted section separator."""
    print(f"\n{char * width}")
    print(f" {title}")
    print(f"{char * width}")


def print_account_data(account_config: Dict, token: str):
    """
    Print all data for a single account.
    
    Args:
        account_config: Dictionary with 'id' and 'name' keys
        token: Meta access token
    """
    account_id = account_config['id']
    account_label = account_config['name']
    
    print_separator(f"📊 ACCOUNT: {account_label}", "█", 80)
    print(f"   Account ID: {account_id}")
    
    # Account Info
    print("\n┌─ Account Information ─────────────────────────────────────────────────────┐")
    info = get_account_info(account_id)
    
    if 'error' in info:
        print(f"│  ❌ Error: {info['error']}")
    else:
        status_map = {1: "✅ ACTIVE", 2: "❌ DISABLED", 3: "⚠️ UNSETTLED"}
        status = status_map.get(info['status'], f"Unknown ({info['status']})")
        
        print(f"│  Name:        {info['name']}")
        print(f"│  Status:      {status}")
        print(f"│  Currency:    {info['currency']}")
        print(f"│  Total Spent: {info['currency']} {info['amount_spent']:,.2f}")
        print(f"│  Balance:     {info['currency']} {info['balance']:,.2f}")
        if info['spend_cap']:
            print(f"│  Spend Cap:   {info['currency']} {info['spend_cap']:,.2f}")
        print(f"│  Timezone:    {info.get('timezone', 'Unknown')}")
    print("└───────────────────────────────────────────────────────────────────────────┘")
    
    # Performance Insights (Last 30 Days)
    print("\n┌─ Performance (Last 30 Days) ─────────────────────────────────────────────┐")
    insights = get_account_insights(account_id, 30)
    
    if 'error' in insights:
        print(f"│  ❌ Error: {insights['error']}")
    elif 'no_data' in insights:
        print("│  ⚠️  No performance data available for this period")
        print("│     (This is normal if no ads ran in the last 30 days)")
    else:
        print(f"│  Impressions:  {insights['impressions']:>15,}")
        print(f"│  Reach:        {insights['reach']:>15,}")
        print(f"│  Clicks:       {insights['clicks']:>15,}")
        print(f"│  CTR:          {insights['ctr']:>14.2f}%")
        print(f"│  Spend:        {insights['spend']:>14,.2f}")
        print(f"│  CPC:          {insights['cpc']:>14,.2f}")
        print(f"│  CPM:          {insights['cpm']:>14,.2f}")
        print(f"│  Frequency:    {insights['frequency']:>14.2f}")
        if insights['app_installs'] > 0:
            print(f"│  App Installs: {insights['app_installs']:>15,}")
            cpi = insights['spend'] / insights['app_installs'] if insights['app_installs'] > 0 else 0
            print(f"│  Cost/Install: {cpi:>14,.2f}")
    print("└───────────────────────────────────────────────────────────────────────────┘")
    
    # Campaigns
    print("\n┌─ Campaigns ──────────────────────────────────────────────────────────────┐")
    campaigns = get_campaigns(account_id)
    
    if campaigns and 'error' in campaigns[0]:
        print(f"│  ❌ Error: {campaigns[0]['error']}")
    elif not campaigns:
        print("│  ⚠️  No campaigns found")
    else:
        active = [c for c in campaigns if c['status'] == 'ACTIVE']
        paused = [c for c in campaigns if c['status'] == 'PAUSED']
        
        print(f"│  Total: {len(campaigns)} | Active: {len(active)} | Paused: {len(paused)}")
        print("│")
        
        # Show active campaigns first
        if active:
            print("│  🟢 ACTIVE CAMPAIGNS:")
            for camp in active[:10]:
                budget_str = ""
                if camp['daily_budget']:
                    budget_str = f" (${camp['daily_budget']:.2f}/day)"
                elif camp['lifetime_budget']:
                    budget_str = f" (${camp['lifetime_budget']:.2f} lifetime)"
                print(f"│     • {camp['name'][:50]}{budget_str}")
            if len(active) > 10:
                print(f"│     ... and {len(active) - 10} more active campaigns")
        
        if paused:
            print("│")
            print(f"│  ⏸️  PAUSED CAMPAIGNS: {len(paused)}")
            for camp in paused[:5]:
                print(f"│     • {camp['name'][:50]}")
            if len(paused) > 5:
                print(f"│     ... and {len(paused) - 5} more paused campaigns")
    
    print("└───────────────────────────────────────────────────────────────────────────┘")
    
    # Campaign Performance Breakdown
    print("\n┌─ Campaign Performance (Last 30 Days) ────────────────────────────────────┐")
    camp_insights = get_campaign_insights(account_id, 30)
    
    if camp_insights and 'error' in camp_insights[0]:
        print(f"│  ❌ Error: {camp_insights[0]['error']}")
    elif not camp_insights:
        print("│  ⚠️  No campaign performance data available")
    else:
        print(f"│  {'Campaign':<35} {'Spend':>10} {'Clicks':>8} {'CTR':>7} {'Installs':>9}")
        print(f"│  {'-'*35} {'-'*10} {'-'*8} {'-'*7} {'-'*9}")
        
        for camp in camp_insights[:10]:
            name = camp['campaign_name'][:35]
            spend = f"${camp['spend']:,.2f}"
            clicks = f"{camp['clicks']:,}"
            ctr = f"{camp['ctr']:.2f}%"
            installs = f"{camp['app_installs']:,}" if camp['app_installs'] > 0 else "-"
            print(f"│  {name:<35} {spend:>10} {clicks:>8} {ctr:>7} {installs:>9}")
        
        if len(camp_insights) > 10:
            print(f"│  ... and {len(camp_insights) - 10} more campaigns")
    
    print("└───────────────────────────────────────────────────────────────────────────┘")
    
    # Ad Sets Summary
    print("\n┌─ Ad Sets ────────────────────────────────────────────────────────────────┐")
    ad_sets = get_ad_sets(account_id)
    
    if ad_sets and 'error' in ad_sets[0]:
        print(f"│  ❌ Error: {ad_sets[0]['error']}")
    elif not ad_sets:
        print("│  ⚠️  No ad sets found")
    else:
        active_sets = [s for s in ad_sets if s['status'] == 'ACTIVE']
        print(f"│  Total: {len(ad_sets)} | Active: {len(active_sets)}")
        
        if active_sets:
            print("│")
            print("│  🟢 ACTIVE AD SETS:")
            for adset in active_sets[:8]:
                countries = ", ".join(adset['countries']) if adset['countries'] else "N/A"
                budget = f"${adset['daily_budget']:.2f}/day" if adset['daily_budget'] else "No daily budget"
                print(f"│     • {adset['name'][:40]}")
                print(f"│       Countries: {countries} | {budget}")
            if len(active_sets) > 8:
                print(f"│     ... and {len(active_sets) - 8} more active ad sets")
    
    print("└───────────────────────────────────────────────────────────────────────────┘")
    
    # Ads Summary
    print("\n┌─ Ads ────────────────────────────────────────────────────────────────────┐")
    ads = get_ads(account_id)
    
    if ads and 'error' in ads[0]:
        print(f"│  ❌ Error: {ads[0]['error']}")
    elif not ads:
        print("│  ⚠️  No ads found")
    else:
        active_ads = [a for a in ads if a['status'] == 'ACTIVE']
        print(f"│  Total: {len(ads)} | Active: {len(active_ads)}")
        
        if active_ads:
            print("│")
            print("│  🟢 ACTIVE ADS (first 10):")
            for ad in active_ads[:10]:
                print(f"│     • {ad['name'][:60]}")
            if len(active_ads) > 10:
                print(f"│     ... and {len(active_ads) - 10} more active ads")
    
    print("└───────────────────────────────────────────────────────────────────────────┘")


def main():
    """Main entry point."""
    
    print("\n" + "█" * 80)
    print(" 🔵 META ADS - MULTI-ACCOUNT COMPARISON REPORT")
    print(" " + "█" * 78)
    print(f" Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("█" * 80)
    
    # Get token
    token = os.getenv("META_ACCESS_TOKEN")
    if not token:
        print("\n❌ ERROR: META_ACCESS_TOKEN not found in .env")
        sys.exit(1)
    
    # Initialize API
    FacebookAdsApi.init(access_token=token)
    
    print(f"\nAnalyzing {len(AD_ACCOUNTS)} ad accounts...")
    
    # Process each account
    for account in AD_ACCOUNTS:
        print_account_data(account, token)
    
    # Summary Comparison
    print_separator("📊 COMPARISON SUMMARY", "█", 80)
    
    print("\n┌─ Account Comparison ─────────────────────────────────────────────────────┐")
    print(f"│  {'Account':<30} {'Spend (30d)':>12} {'Clicks':>10} {'Campaigns':>10}")
    print(f"│  {'-'*30} {'-'*12} {'-'*10} {'-'*10}")
    
    for account in AD_ACCOUNTS:
        insights = get_account_insights(account['id'], 30)
        campaigns = get_campaigns(account['id'])
        
        name = account['name'][:30]
        spend = f"${insights.get('spend', 0):,.2f}" if 'spend' in insights else "N/A"
        clicks = f"{insights.get('clicks', 0):,}" if 'clicks' in insights else "N/A"
        camp_count = str(len(campaigns)) if campaigns and 'error' not in campaigns[0] else "N/A"
        
        print(f"│  {name:<30} {spend:>12} {clicks:>10} {camp_count:>10}")
    
    print("└───────────────────────────────────────────────────────────────────────────┘")
    
    print("\n" + "█" * 80)
    print(" ✅ Report complete!")
    print("█" * 80 + "\n")


if __name__ == "__main__":
    main()
