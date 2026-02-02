#!/usr/bin/env python3
"""
Twitter/X API Connection Test Script

Tests Twitter API credentials and validates:
1. Bearer token authentication
2. OAuth 1.0a user authentication
3. API access and permissions
4. Sample data pull

Usage:
    python scripts/test_twitter_connection.py
"""

import os
import sys
from pathlib import Path

# Handle Windows console encoding for emojis
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env")


def print_header(text: str):
    """Print a formatted header."""
    print(f"\n{'='*60}")
    print(f" {text}")
    print('='*60)


def test_tweepy_import():
    """Test if tweepy is installed."""
    print("\n[1/5] Checking tweepy installation...")
    try:
        import tweepy
        print(f"    ✅ tweepy {tweepy.__version__} installed")
        return True
    except ImportError:
        print("    ❌ tweepy not installed")
        print("    Run: pip install tweepy")
        return False


def test_env_variables():
    """Test if all required environment variables are set."""
    print("\n[2/5] Checking environment variables...")
    
    required_vars = [
        'TWITTER_BEARER_TOKEN',
        'TWITTER_CONSUMER_KEY',
        'TWITTER_CONSUMER_SECRET',
        'TWITTER_ACCESS_TOKEN',
        'TWITTER_ACCESS_TOKEN_SECRET',
        'TWITTER_USERNAME',
    ]
    
    optional_vars = [
        'TWITTER_CLIENT_ID',
        'TWITTER_CLIENT_SECRET',
    ]
    
    all_present = True
    for var in required_vars:
        value = os.getenv(var, '')
        if value:
            # Mask the value
            masked = value[:10] + "..." if len(value) > 10 else value
            print(f"    ✅ {var}: {masked}")
        else:
            print(f"    ❌ {var}: NOT SET")
            all_present = False
    
    for var in optional_vars:
        value = os.getenv(var, '')
        if value:
            print(f"    ✅ {var}: (set)")
        else:
            print(f"    ⚪ {var}: (optional, not set)")
    
    return all_present


def test_bearer_token_auth():
    """Test Bearer Token (app-only) authentication."""
    print("\n[3/5] Testing Bearer Token authentication...")
    
    try:
        import tweepy
        
        bearer_token = os.getenv('TWITTER_BEARER_TOKEN')
        client = tweepy.Client(bearer_token=bearer_token)
        
        # Test with a simple lookup
        username = os.getenv('TWITTER_USERNAME', 'Twitter')
        user = client.get_user(username=username, user_fields=['public_metrics'])
        
        if user.data:
            metrics = user.data.public_metrics or {}
            print(f"    ✅ Bearer token valid")
            print(f"    📊 Found @{user.data.username}:")
            print(f"       Followers: {metrics.get('followers_count', 0):,}")
            print(f"       Following: {metrics.get('following_count', 0):,}")
            print(f"       Tweets: {metrics.get('tweet_count', 0):,}")
            return True
        else:
            print(f"    ❌ User @{username} not found")
            return False
            
    except tweepy.errors.Unauthorized as e:
        print(f"    ❌ Unauthorized: Invalid bearer token")
        print(f"       {e}")
        return False
    except Exception as e:
        print(f"    ❌ Error: {e}")
        return False


def test_oauth_auth():
    """Test OAuth 1.0a (user context) authentication."""
    print("\n[4/5] Testing OAuth 1.0a authentication...")
    
    try:
        import tweepy
        
        client = tweepy.Client(
            consumer_key=os.getenv('TWITTER_CONSUMER_KEY'),
            consumer_secret=os.getenv('TWITTER_CONSUMER_SECRET'),
            access_token=os.getenv('TWITTER_ACCESS_TOKEN'),
            access_token_secret=os.getenv('TWITTER_ACCESS_TOKEN_SECRET'),
        )
        
        # Test by getting authenticated user
        me = client.get_me(user_fields=['public_metrics', 'description'])
        
        if me.data:
            metrics = me.data.public_metrics or {}
            print(f"    ✅ OAuth authentication successful")
            print(f"    👤 Authenticated as: @{me.data.username}")
            print(f"       Name: {me.data.name}")
            print(f"       Followers: {metrics.get('followers_count', 0):,}")
            return True, me.data.username
        else:
            print(f"    ❌ Could not get authenticated user")
            return False, None
            
    except tweepy.errors.Unauthorized as e:
        print(f"    ❌ Unauthorized: Invalid OAuth credentials")
        print(f"       {e}")
        return False, None
    except Exception as e:
        print(f"    ❌ Error: {e}")
        return False, None


def test_data_pull(auth_username: str = None):
    """Test pulling sample data."""
    print("\n[5/5] Testing data extraction...")
    
    try:
        import tweepy
        
        client = tweepy.Client(
            bearer_token=os.getenv('TWITTER_BEARER_TOKEN'),
            consumer_key=os.getenv('TWITTER_CONSUMER_KEY'),
            consumer_secret=os.getenv('TWITTER_CONSUMER_SECRET'),
            access_token=os.getenv('TWITTER_ACCESS_TOKEN'),
            access_token_secret=os.getenv('TWITTER_ACCESS_TOKEN_SECRET'),
        )
        
        # Get target username
        target_username = os.getenv('TWITTER_USERNAME', auth_username or 'Twitter')
        
        # Get user
        user = client.get_user(username=target_username)
        if not user.data:
            print(f"    ❌ User @{target_username} not found")
            return False
        
        user_id = user.data.id
        
        # Get recent tweets
        tweets = client.get_users_tweets(
            id=user_id,
            max_results=10,
            tweet_fields=['created_at', 'public_metrics'],
            exclude=['retweets']
        )
        
        if tweets.data:
            print(f"    ✅ Successfully fetched {len(tweets.data)} tweets from @{target_username}")
            print(f"\n    📝 Recent Tweets:")
            
            for tweet in tweets.data[:5]:
                text = tweet.text[:60] + "..." if len(tweet.text) > 60 else tweet.text
                text = text.replace('\n', ' ')
                metrics = tweet.public_metrics or {}
                print(f"       • {text}")
                print(f"         ❤️ {metrics.get('like_count', 0)} | 🔄 {metrics.get('retweet_count', 0)} | 💬 {metrics.get('reply_count', 0)}")
            
            return True
        else:
            print(f"    ⚠️  No tweets found for @{target_username}")
            return True  # Still successful, just no tweets
            
    except tweepy.errors.Forbidden as e:
        print(f"    ❌ Forbidden: Insufficient permissions")
        print(f"       {e}")
        print(f"       Note: Some metrics require elevated API access")
        return False
    except Exception as e:
        print(f"    ❌ Error: {e}")
        return False


def main():
    """Run all connection tests."""
    print_header("Twitter/X API Connection Test")
    
    # Check enabled
    enabled = os.getenv('ENABLE_TWITTER', '0')
    if enabled not in ('1', 'true', 'True'):
        print("\n⚠️  Twitter integration is DISABLED")
        print("   Set ENABLE_TWITTER=1 in .env to enable")
        print("   Continuing with tests anyway...\n")
    
    results = []
    
    # Test 1: Tweepy installed
    results.append(("tweepy installed", test_tweepy_import()))
    if not results[-1][1]:
        print("\n❌ Cannot continue without tweepy. Exiting.")
        return
    
    # Test 2: Environment variables
    results.append(("Environment variables", test_env_variables()))
    if not results[-1][1]:
        print("\n❌ Missing required environment variables. Check your .env file.")
        return
    
    # Test 3: Bearer token
    results.append(("Bearer Token auth", test_bearer_token_auth()))
    
    # Test 4: OAuth
    oauth_success, auth_username = test_oauth_auth()
    results.append(("OAuth 1.0a auth", oauth_success))
    
    # Test 5: Data pull
    results.append(("Data extraction", test_data_pull(auth_username)))
    
    # Summary
    print_header("Test Summary")
    
    all_passed = True
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status}: {name}")
        if not passed:
            all_passed = False
    
    print()
    if all_passed:
        print("🎉 All tests passed! Twitter integration is ready.")
        print("\nNext steps:")
        print("  1. Run ETL: python scripts/run_etl_twitter.py")
        print("  2. View dashboard: streamlit run app/main.py")
    else:
        print("⚠️  Some tests failed. Please check your credentials.")
        print("\nCommon issues:")
        print("  - Bearer token: Regenerate at developer.twitter.com")
        print("  - OAuth: Check all 4 OAuth credentials match")
        print("  - Permissions: Ensure app has 'Read' access in Twitter Developer Portal")


if __name__ == "__main__":
    main()
