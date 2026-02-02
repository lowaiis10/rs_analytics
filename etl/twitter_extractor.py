"""
Twitter/X Data Extractor

Extracts organic page analytics from Twitter API v2:
- User profile metrics (followers, following, tweet count)
- Tweet performance (impressions, likes, retweets, replies, quotes)
- Tweet timeline with engagement metrics
- Daily aggregated metrics

Twitter API v2 Free Tier Limitations:
- Tweet lookup: 1,500 tweets/month
- User lookup: 500 requests/15 min
- User tweets: 1,500 tweets/month
- No historical data beyond 7 days for most endpoints

Usage:
    from etl.twitter_extractor import TwitterExtractor
    
    extractor = TwitterExtractor(config)
    tweets_df = extractor.extract_recent_tweets(max_results=100)
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TwitterExtractor:
    """
    Twitter/X data extractor class.
    
    Extracts organic page analytics using Twitter API v2.
    """
    
    # Tweet fields to request from API
    TWEET_FIELDS = [
        'id',
        'text',
        'author_id',
        'created_at',
        'public_metrics',  # likes, retweets, replies, quotes, impressions
        'non_public_metrics',  # impressions, user_profile_clicks, url_link_clicks (requires user auth)
        'organic_metrics',  # organic impressions, likes, retweets (requires user auth)
        'conversation_id',
        'in_reply_to_user_id',
        'referenced_tweets',
        'attachments',
        'lang',
        'source',
    ]
    
    # User fields to request from API
    USER_FIELDS = [
        'id',
        'name',
        'username',
        'created_at',
        'description',
        'location',
        'profile_image_url',
        'public_metrics',  # followers_count, following_count, tweet_count, listed_count
        'verified',
        'verified_type',
    ]
    
    def __init__(self, config):
        """
        Initialize the Twitter extractor.
        
        Args:
            config: TwitterConfig object with credentials
        """
        try:
            import tweepy
        except ImportError:
            raise ImportError("tweepy is required. Install with: pip install tweepy")
        
        self.config = config
        self.username = config.username
        
        # Initialize client with OAuth 1.0a for user-context endpoints
        # This is needed to get non_public_metrics like impressions
        self.client = tweepy.Client(
            bearer_token=config.bearer_token,
            consumer_key=config.consumer_key,
            consumer_secret=config.consumer_secret,
            access_token=config.access_token,
            access_token_secret=config.access_token_secret,
            wait_on_rate_limit=True
        )
        
        # Cache user info
        self._user_id = None
        self._user_data = None
        
        logger.info(f"Initialized TwitterExtractor for @{self.username}")
    
    def test_connection(self) -> Tuple[bool, str]:
        """
        Test API connection and retrieve authenticated user info.
        
        Returns:
            Tuple of (success, message)
        """
        try:
            me = self.client.get_me(user_fields=self.USER_FIELDS)
            
            if me.data:
                self._user_data = me.data
                self._user_id = me.data.id
                metrics = me.data.public_metrics
                return True, (
                    f"Connected as @{me.data.username} "
                    f"({metrics['followers_count']:,} followers, "
                    f"{metrics['tweet_count']:,} tweets)"
                )
            return False, "Could not retrieve user information"
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False, str(e)
    
    def _get_user_id(self) -> str:
        """Get the authenticated user's ID, caching the result."""
        if self._user_id is None:
            me = self.client.get_me()
            if me.data:
                self._user_id = me.data.id
            else:
                raise ValueError("Could not get authenticated user ID")
        return self._user_id
    
    def extract_user_profile(self) -> pd.DataFrame:
        """
        Extract current user profile metrics.
        
        Returns:
            DataFrame with user profile information
        """
        logger.info(f"Extracting profile for @{self.username}")
        
        try:
            # Get user by username
            user = self.client.get_user(
                username=self.username,
                user_fields=self.USER_FIELDS
            )
            
            if not user.data:
                logger.warning(f"User @{self.username} not found")
                return pd.DataFrame()
            
            data = user.data
            metrics = data.public_metrics or {}
            
            record = {
                'user_id': data.id,
                'username': data.username,
                'name': data.name,
                'description': data.description or '',
                'location': getattr(data, 'location', '') or '',
                'verified': getattr(data, 'verified', False),
                'verified_type': getattr(data, 'verified_type', None),
                'created_at': data.created_at.isoformat() if data.created_at else None,
                'followers_count': metrics.get('followers_count', 0),
                'following_count': metrics.get('following_count', 0),
                'tweet_count': metrics.get('tweet_count', 0),
                'listed_count': metrics.get('listed_count', 0),
                'profile_image_url': getattr(data, 'profile_image_url', ''),
                'snapshot_date': datetime.now().strftime('%Y-%m-%d'),
                'extracted_at': datetime.now().isoformat()
            }
            
            df = pd.DataFrame([record])
            logger.info(f"Extracted profile: {metrics.get('followers_count', 0):,} followers")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting user profile: {e}")
            return pd.DataFrame()
    
    def extract_recent_tweets(self, 
                              max_results: int = 100,
                              start_time: Optional[datetime] = None,
                              end_time: Optional[datetime] = None) -> pd.DataFrame:
        """
        Extract recent tweets with engagement metrics.
        
        Note: Twitter API v2 free tier only allows fetching recent tweets
        (up to 7 days for most endpoints, 3200 most recent for timeline).
        
        Args:
            max_results: Maximum number of tweets to fetch (10-100 per request)
            start_time: Start time for tweet search (optional)
            end_time: End time for tweet search (optional)
            
        Returns:
            DataFrame with tweet data and metrics
        """
        logger.info(f"Extracting recent tweets for @{self.username}")
        
        try:
            # Get user ID first
            user = self.client.get_user(username=self.username)
            if not user.data:
                logger.warning(f"User @{self.username} not found")
                return pd.DataFrame()
            
            user_id = user.data.id
            
            # Prepare parameters
            params = {
                'id': user_id,
                'max_results': min(max_results, 100),  # API limit per request
                'tweet_fields': ['id', 'text', 'created_at', 'public_metrics', 
                                'conversation_id', 'in_reply_to_user_id', 
                                'referenced_tweets', 'lang', 'source'],
                'exclude': ['retweets'],  # Only original tweets and replies
            }
            
            if start_time:
                params['start_time'] = start_time
            if end_time:
                params['end_time'] = end_time
            
            # Fetch tweets using pagination
            tweets_data = []
            pagination_token = None
            fetched = 0
            
            while fetched < max_results:
                if pagination_token:
                    params['pagination_token'] = pagination_token
                
                response = self.client.get_users_tweets(**params)
                
                if not response.data:
                    break
                
                for tweet in response.data:
                    metrics = tweet.public_metrics or {}
                    
                    # Determine tweet type
                    tweet_type = 'original'
                    if tweet.in_reply_to_user_id:
                        tweet_type = 'reply'
                    elif tweet.referenced_tweets:
                        for ref in tweet.referenced_tweets:
                            if ref.type == 'quoted':
                                tweet_type = 'quote'
                                break
                    
                    record = {
                        'tweet_id': tweet.id,
                        'user_id': user_id,
                        'username': self.username,
                        'text': tweet.text,
                        'tweet_type': tweet_type,
                        'created_at': tweet.created_at.isoformat() if tweet.created_at else None,
                        'created_date': tweet.created_at.strftime('%Y-%m-%d') if tweet.created_at else None,
                        'impressions': metrics.get('impression_count', 0),
                        'likes': metrics.get('like_count', 0),
                        'retweets': metrics.get('retweet_count', 0),
                        'replies': metrics.get('reply_count', 0),
                        'quotes': metrics.get('quote_count', 0),
                        'bookmarks': metrics.get('bookmark_count', 0),
                        'language': tweet.lang,
                        'source': tweet.source,
                        'conversation_id': tweet.conversation_id,
                        'in_reply_to_user_id': tweet.in_reply_to_user_id,
                        'extracted_at': datetime.now().isoformat()
                    }
                    tweets_data.append(record)
                    fetched += 1
                    
                    if fetched >= max_results:
                        break
                
                # Check for more pages
                if response.meta and 'next_token' in response.meta:
                    pagination_token = response.meta['next_token']
                else:
                    break
            
            df = pd.DataFrame(tweets_data)
            logger.info(f"Extracted {len(df)} tweets")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting tweets: {e}")
            return pd.DataFrame()
    
    def extract_daily_metrics(self, tweets_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Aggregate tweet metrics by day.
        
        Args:
            tweets_df: DataFrame of tweets (if None, will fetch recent tweets)
            
        Returns:
            DataFrame with daily aggregated metrics
        """
        logger.info("Calculating daily metrics")
        
        if tweets_df is None or tweets_df.empty:
            tweets_df = self.extract_recent_tweets(max_results=100)
        
        if tweets_df.empty:
            return pd.DataFrame()
        
        try:
            # Group by date
            daily = tweets_df.groupby('created_date').agg({
                'tweet_id': 'count',
                'impressions': 'sum',
                'likes': 'sum',
                'retweets': 'sum',
                'replies': 'sum',
                'quotes': 'sum',
                'bookmarks': 'sum',
            }).reset_index()
            
            daily.columns = [
                'date', 'tweet_count', 'impressions', 'likes', 
                'retweets', 'replies', 'quotes', 'bookmarks'
            ]
            
            # Calculate engagement metrics
            daily['total_engagements'] = (
                daily['likes'] + daily['retweets'] + 
                daily['replies'] + daily['quotes'] + daily['bookmarks']
            )
            daily['engagement_rate'] = (
                daily['total_engagements'] / daily['impressions'].replace(0, 1) * 100
            ).round(2)
            
            # Add username and extraction timestamp
            daily['username'] = self.username
            daily['extracted_at'] = datetime.now().isoformat()
            
            logger.info(f"Generated {len(daily)} daily metric records")
            return daily
            
        except Exception as e:
            logger.error(f"Error calculating daily metrics: {e}")
            return pd.DataFrame()
    
    def extract_all(self, max_tweets: int = 100) -> Dict[str, pd.DataFrame]:
        """
        Extract all available Twitter data.
        
        Args:
            max_tweets: Maximum number of tweets to fetch
            
        Returns:
            Dictionary with DataFrames for each data type
        """
        logger.info(f"Starting full extraction for @{self.username}")
        
        # Extract profile
        profile_df = self.extract_user_profile()
        
        # Extract tweets
        tweets_df = self.extract_recent_tweets(max_results=max_tweets)
        
        # Calculate daily metrics
        daily_df = self.extract_daily_metrics(tweets_df)
        
        results = {
            'profile': profile_df,
            'tweets': tweets_df,
            'daily_metrics': daily_df,
        }
        
        total_rows = sum(len(df) for df in results.values())
        logger.info(f"Full extraction complete: {total_rows} total rows")
        
        return results


if __name__ == "__main__":
    """Test extractor when run directly."""
    import sys
    
    # Handle Windows console encoding
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    
    from twitter_config import get_twitter_config
    
    print("Testing Twitter extractor...")
    
    try:
        config = get_twitter_config()
        extractor = TwitterExtractor(config)
        
        # Test connection
        success, message = extractor.test_connection()
        print(f"\nConnection: {'✅' if success else '❌'} {message}")
        
        if success:
            # Extract profile
            print("\n--- Profile ---")
            profile_df = extractor.extract_user_profile()
            if not profile_df.empty:
                print(f"Followers: {profile_df['followers_count'].iloc[0]:,}")
                print(f"Tweets: {profile_df['tweet_count'].iloc[0]:,}")
            
            # Extract recent tweets (just 10 for testing)
            print("\n--- Recent Tweets (last 10) ---")
            tweets_df = extractor.extract_recent_tweets(max_results=10)
            if not tweets_df.empty:
                for _, tweet in tweets_df.head(5).iterrows():
                    text = tweet['text'][:50] + "..." if len(tweet['text']) > 50 else tweet['text']
                    print(f"  {tweet['created_date']}: {text}")
                    print(f"    Likes: {tweet['likes']}, RT: {tweet['retweets']}, Impressions: {tweet['impressions']}")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
