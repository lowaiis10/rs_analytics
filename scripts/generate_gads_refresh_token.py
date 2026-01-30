#!/usr/bin/env python3
"""
Generate Google Ads OAuth Refresh Token

This script helps you generate a refresh token for Google Ads API access.
It will open a browser for you to authenticate with your Google account.

Usage:
    python scripts/generate_gads_refresh_token.py
"""

import sys
from google_auth_oauthlib.flow import InstalledAppFlow

# Your OAuth credentials from Google Cloud Console
CLIENT_ID = "1091537919040-5ib2donbdg26aj6s6b1k6mf07nrrv737.apps.googleusercontent.com"
CLIENT_SECRET = "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"

# Google Ads API scope
SCOPES = ["https://www.googleapis.com/auth/adwords"]

def main():
    """Generate refresh token using OAuth flow."""
    
    print("=" * 60)
    print("Google Ads OAuth Refresh Token Generator")
    print("=" * 60)
    print()
    print("This will open a browser window for you to authenticate.")
    print("Sign in with the Google account that has access to Google Ads.")
    print()
    
    # Create the OAuth client config
    client_config = {
        "installed": {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost", "urn:ietf:wg:oauth:2.0:oob"],
        }
    }
    
    # Run the OAuth flow
    flow = InstalledAppFlow.from_client_config(client_config, scopes=SCOPES)
    
    # This will open a browser and wait for authentication
    credentials = flow.run_local_server(port=8080)
    
    print()
    print("=" * 60)
    print("SUCCESS! Here is your refresh token:")
    print("=" * 60)
    print()
    print(f"refresh_token: {credentials.refresh_token}")
    print()
    print("=" * 60)
    print("Next steps:")
    print("1. Copy the refresh_token above")
    print("2. Update secrets/google_ads.yaml with this new refresh_token")
    print("3. Run: python scripts/test_gads_connection.py")
    print("=" * 60)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
