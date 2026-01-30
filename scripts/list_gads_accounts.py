#!/usr/bin/env python3
"""
List Google Ads Client Accounts under a Manager Account

This script lists all client accounts accessible from your manager account.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from etl.gads_config import get_gads_config, get_gads_client


def main():
    print("=" * 60)
    print("Google Ads - List Client Accounts")
    print("=" * 60)
    
    config = get_gads_config()
    client = get_gads_client()
    
    ga_service = client.get_service("GoogleAdsService")
    
    # Query to list accessible customer accounts
    query = """
        SELECT
            customer_client.client_customer,
            customer_client.level,
            customer_client.manager,
            customer_client.descriptive_name,
            customer_client.currency_code,
            customer_client.time_zone,
            customer_client.id
        FROM customer_client
        WHERE customer_client.level <= 1
    """
    
    print(f"\nQuerying accounts under manager: {config.customer_id}")
    print("-" * 60)
    
    try:
        response = ga_service.search(
            customer_id=config.customer_id,
            query=query
        )
        
        accounts = []
        for row in response:
            cc = row.customer_client
            account_info = {
                'id': cc.id,
                'name': cc.descriptive_name,
                'currency': cc.currency_code,
                'timezone': cc.time_zone,
                'is_manager': cc.manager,
                'level': cc.level
            }
            accounts.append(account_info)
            
            account_type = "MANAGER" if cc.manager else "CLIENT"
            print(f"\n[{account_type}] {cc.descriptive_name}")
            print(f"  Customer ID: {cc.id}")
            print(f"  Currency: {cc.currency_code}")
            print(f"  Timezone: {cc.time_zone}")
            print(f"  Level: {cc.level}")
        
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        
        client_accounts = [a for a in accounts if not a['is_manager']]
        manager_accounts = [a for a in accounts if a['is_manager']]
        
        print(f"Total accounts found: {len(accounts)}")
        print(f"  - Manager accounts: {len(manager_accounts)}")
        print(f"  - Client accounts: {len(client_accounts)}")
        
        if client_accounts:
            print("\n" + "=" * 60)
            print("CLIENT ACCOUNTS (these can be queried for metrics)")
            print("=" * 60)
            for acc in client_accounts:
                print(f"  {acc['id']} - {acc['name']}")
            
            print("\nTo pull data from a specific client account, update your .env:")
            print(f"  GOOGLE_ADS_CUSTOMER_ID={client_accounts[0]['id']}")
            print("\nKeep login_customer_id as your manager account in google_ads.yaml")
        else:
            print("\nNo client accounts found under this manager account.")
            print("You may need to link client accounts or check permissions.")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
