"""
Meta (Facebook) Ads Data Extractor

Extracts comprehensive advertising data from Meta Ads API including:
- Account-level metrics
- Campaign performance over time
- Ad Set (Ad Group) performance
- Individual ad performance
- Geographic breakdowns
- Device/platform breakdowns
- Age and gender demographics
- Placement performance

Usage:
    from etl.meta_extractor import MetaExtractor
    
    extractor = MetaExtractor(access_token, ad_account_id)
    campaigns_df = extractor.extract_campaigns(days=365)
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
import pandas as pd

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MetaExtractor:
    """
    Meta Ads data extractor class.
    
    Handles extraction of all advertising data from Meta Ads API
    with support for lifetime data, date ranges, and breakdowns.
    """
    
    # Standard metrics to fetch for all levels
    STANDARD_METRICS = [
        'impressions',
        'reach',
        'clicks',
        'spend',
        'ctr',
        'cpc',
        'cpm',
        'frequency',
        'unique_clicks',
        'cost_per_unique_click',
        'actions',
        'action_values',
        'conversions',
        'cost_per_action_type',
        'video_p25_watched_actions',
        'video_p50_watched_actions',
        'video_p75_watched_actions',
        'video_p100_watched_actions',
    ]
    
    # Campaign fields
    CAMPAIGN_FIELDS = [
        'id',
        'name',
        'status',
        'effective_status',
        'objective',
        'created_time',
        'start_time',
        'stop_time',
        'daily_budget',
        'lifetime_budget',
        'budget_remaining',
        'buying_type',
        'special_ad_categories',
    ]
    
    # Ad Set fields
    ADSET_FIELDS = [
        'id',
        'name',
        'campaign_id',
        'status',
        'effective_status',
        'daily_budget',
        'lifetime_budget',
        'budget_remaining',
        'optimization_goal',
        'billing_event',
        'bid_strategy',
        'targeting',
        'created_time',
        'start_time',
        'end_time',
    ]
    
    # Ad fields
    AD_FIELDS = [
        'id',
        'name',
        'adset_id',
        'campaign_id',
        'status',
        'effective_status',
        'created_time',
        'creative',
    ]
    
    def __init__(self, access_token: str, ad_account_id: str):
        """
        Initialize the Meta Ads extractor.
        
        Args:
            access_token: Meta API access token
            ad_account_id: Ad account ID (with or without act_ prefix)
        """
        self.access_token = access_token
        self.ad_account_id = ad_account_id if ad_account_id.startswith('act_') else f'act_{ad_account_id}'
        
        # Initialize API
        FacebookAdsApi.init(access_token=access_token)
        self.account = AdAccount(self.ad_account_id)
        
        logger.info(f"Initialized MetaExtractor for account: {self.ad_account_id}")
    
    def test_connection(self) -> Tuple[bool, str]:
        """
        Test API connection.
        
        Returns:
            Tuple of (success, message)
        """
        try:
            info = self.account.api_get(fields=['id', 'name', 'account_status'])
            return True, f"Connected to: {info.get('name')} ({info.get('id')})"
        except Exception as e:
            return False, f"Connection failed: {str(e)}"
    
    def get_account_info(self) -> Dict:
        """Get account information."""
        try:
            info = self.account.api_get(fields=[
                'id', 'name', 'account_status', 'currency',
                'amount_spent', 'balance', 'spend_cap',
                'business_name', 'timezone_name', 'timezone_offset_hours_utc'
            ])
            return dict(info)
        except Exception as e:
            logger.error(f"Error getting account info: {e}")
            return {}
    
    def _parse_actions(self, actions: List[Dict], action_type: str) -> int:
        """
        Parse actions list to get specific action count.
        
        For app_install, checks multiple action types that Meta may use:
        - mobile_app_install (most common for mobile apps)
        - omni_app_install (cross-device installs)
        - app_install (legacy/generic)
        """
        if not actions:
            return 0
        
        # For app installs, check multiple possible action types
        # Meta returns installs under different names depending on campaign type
        if action_type == 'app_install':
            install_types = ['mobile_app_install', 'omni_app_install', 'app_install']
            for install_type in install_types:
                for action in actions:
                    if action.get('action_type') == install_type:
                        return int(float(action.get('value', 0)))
            return 0
        
        # Standard lookup for other action types
        for action in actions:
            if action.get('action_type') == action_type:
                return int(float(action.get('value', 0)))
        return 0
    
    def _parse_action_values(self, action_values: List[Dict], action_type: str) -> float:
        """Parse action values list to get specific action value."""
        if not action_values:
            return 0.0
        for action in action_values:
            if action.get('action_type') == action_type:
                return float(action.get('value', 0))
        return 0.0
    
    def _get_date_range(self, days: Optional[int] = None, 
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None,
                        lifetime: bool = False) -> Dict:
        """
        Build date range parameter for API calls.
        
        Args:
            days: Number of days to look back
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            lifetime: If True, get all available data
            
        Returns:
            Dictionary with 'since' and 'until' keys
        """
        if lifetime:
            # Meta API limitations:
            # 1. Maximum historical data: 37 months
            # 2. IMPORTANT: Very long date ranges (>400 days) may not return 
            #    action breakdowns like mobile_app_install in the response.
            #    Use 400 days (~13 months) to ensure action data is included.
            end = datetime.now()
            start = end - timedelta(days=400)  # ~13 months for reliable action data
            return {
                'since': start.strftime('%Y-%m-%d'),
                'until': end.strftime('%Y-%m-%d')
            }
        
        if start_date and end_date:
            return {'since': start_date, 'until': end_date}
        
        if days:
            end = datetime.now()
            start = end - timedelta(days=days)
            return {
                'since': start.strftime('%Y-%m-%d'),
                'until': end.strftime('%Y-%m-%d')
            }
        
        # Default: last 30 days
        end = datetime.now()
        start = end - timedelta(days=30)
        return {
            'since': start.strftime('%Y-%m-%d'),
            'until': end.strftime('%Y-%m-%d')
        }
    
    def extract_daily_account_insights(self, days: int = None, 
                                       start_date: str = None,
                                       end_date: str = None,
                                       lifetime: bool = False) -> pd.DataFrame:
        """
        Extract daily account-level insights.
        
        Args:
            days: Number of days to look back
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            lifetime: If True, get all available data
            
        Returns:
            DataFrame with daily account metrics
        """
        logger.info(f"Extracting daily account insights for {self.ad_account_id}")
        
        date_range = self._get_date_range(days, start_date, end_date, lifetime)
        
        try:
            insights = self.account.get_insights(
                fields=self.STANDARD_METRICS,
                params={
                    'time_range': date_range,
                    'time_increment': 1,  # Daily breakdown
                    'level': 'account'
                }
            )
            
            data = []
            for row in insights:
                record = {
                    'date': row.get('date_start'),
                    'ad_account_id': self.ad_account_id,
                    'impressions': int(row.get('impressions', 0)),
                    'reach': int(row.get('reach', 0)),
                    'clicks': int(row.get('clicks', 0)),
                    'unique_clicks': int(row.get('unique_clicks', 0)),
                    'spend': float(row.get('spend', 0)),
                    'ctr': float(row.get('ctr', 0)),
                    'cpc': float(row.get('cpc', 0)),
                    'cpm': float(row.get('cpm', 0)),
                    'frequency': float(row.get('frequency', 0)),
                    'cost_per_unique_click': float(row.get('cost_per_unique_click', 0)),
                    # Parse conversions
                    'link_clicks': self._parse_actions(row.get('actions'), 'link_click'),
                    'page_engagement': self._parse_actions(row.get('actions'), 'page_engagement'),
                    'post_engagement': self._parse_actions(row.get('actions'), 'post_engagement'),
                    'app_installs': self._parse_actions(row.get('actions'), 'app_install'),
                    'purchases': self._parse_actions(row.get('actions'), 'purchase'),
                    'leads': self._parse_actions(row.get('actions'), 'lead'),
                    'purchase_value': self._parse_action_values(row.get('action_values'), 'purchase'),
                    # Video metrics
                    'video_p25': self._parse_actions(row.get('video_p25_watched_actions'), 'video_view'),
                    'video_p50': self._parse_actions(row.get('video_p50_watched_actions'), 'video_view'),
                    'video_p75': self._parse_actions(row.get('video_p75_watched_actions'), 'video_view'),
                    'video_p100': self._parse_actions(row.get('video_p100_watched_actions'), 'video_view'),
                    'extracted_at': datetime.now().isoformat()
                }
                data.append(record)
            
            df = pd.DataFrame(data)
            logger.info(f"Extracted {len(df)} daily account records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting account insights: {e}")
            return pd.DataFrame()
    
    def extract_campaigns(self) -> pd.DataFrame:
        """
        Extract campaign metadata.
        
        Returns:
            DataFrame with campaign information
        """
        logger.info(f"Extracting campaigns for {self.ad_account_id}")
        
        try:
            campaigns = self.account.get_campaigns(fields=self.CAMPAIGN_FIELDS)
            
            data = []
            for camp in campaigns:
                record = {
                    'campaign_id': camp.get('id'),
                    'ad_account_id': self.ad_account_id,
                    'campaign_name': camp.get('name'),
                    'status': camp.get('status'),
                    'effective_status': camp.get('effective_status'),
                    'objective': camp.get('objective'),
                    'buying_type': camp.get('buying_type'),
                    'daily_budget': float(camp.get('daily_budget', 0)) / 100 if camp.get('daily_budget') else None,
                    'lifetime_budget': float(camp.get('lifetime_budget', 0)) / 100 if camp.get('lifetime_budget') else None,
                    'budget_remaining': float(camp.get('budget_remaining', 0)) / 100 if camp.get('budget_remaining') else None,
                    'created_time': camp.get('created_time'),
                    'start_time': camp.get('start_time'),
                    'stop_time': camp.get('stop_time'),
                    'extracted_at': datetime.now().isoformat()
                }
                data.append(record)
            
            df = pd.DataFrame(data)
            logger.info(f"Extracted {len(df)} campaigns")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting campaigns: {e}")
            return pd.DataFrame()
    
    def extract_campaign_insights(self, days: int = None,
                                  start_date: str = None,
                                  end_date: str = None,
                                  lifetime: bool = False) -> pd.DataFrame:
        """
        Extract daily campaign-level insights.
        
        Returns:
            DataFrame with daily campaign metrics
        """
        logger.info(f"Extracting campaign insights for {self.ad_account_id}")
        
        date_range = self._get_date_range(days, start_date, end_date, lifetime)
        
        try:
            insights = self.account.get_insights(
                fields=['campaign_id', 'campaign_name'] + self.STANDARD_METRICS,
                params={
                    'time_range': date_range,
                    'time_increment': 1,
                    'level': 'campaign'
                }
            )
            
            data = []
            for row in insights:
                record = {
                    'date': row.get('date_start'),
                    'ad_account_id': self.ad_account_id,
                    'campaign_id': row.get('campaign_id'),
                    'campaign_name': row.get('campaign_name'),
                    'impressions': int(row.get('impressions', 0)),
                    'reach': int(row.get('reach', 0)),
                    'clicks': int(row.get('clicks', 0)),
                    'unique_clicks': int(row.get('unique_clicks', 0)),
                    'spend': float(row.get('spend', 0)),
                    'ctr': float(row.get('ctr', 0)),
                    'cpc': float(row.get('cpc', 0)),
                    'cpm': float(row.get('cpm', 0)),
                    'frequency': float(row.get('frequency', 0)),
                    'link_clicks': self._parse_actions(row.get('actions'), 'link_click'),
                    'app_installs': self._parse_actions(row.get('actions'), 'app_install'),
                    'purchases': self._parse_actions(row.get('actions'), 'purchase'),
                    'leads': self._parse_actions(row.get('actions'), 'lead'),
                    'purchase_value': self._parse_action_values(row.get('action_values'), 'purchase'),
                    'extracted_at': datetime.now().isoformat()
                }
                data.append(record)
            
            df = pd.DataFrame(data)
            logger.info(f"Extracted {len(df)} campaign insight records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting campaign insights: {e}")
            return pd.DataFrame()
    
    def extract_adsets(self) -> pd.DataFrame:
        """
        Extract ad set (ad group) metadata.
        
        Returns:
            DataFrame with ad set information
        """
        logger.info(f"Extracting ad sets for {self.ad_account_id}")
        
        try:
            adsets = self.account.get_ad_sets(fields=self.ADSET_FIELDS)
            
            data = []
            for adset in adsets:
                targeting = adset.get('targeting', {})
                geo_locations = targeting.get('geo_locations', {})
                countries = geo_locations.get('countries', [])
                
                record = {
                    'adset_id': adset.get('id'),
                    'ad_account_id': self.ad_account_id,
                    'campaign_id': adset.get('campaign_id'),
                    'adset_name': adset.get('name'),
                    'status': adset.get('status'),
                    'effective_status': adset.get('effective_status'),
                    'optimization_goal': adset.get('optimization_goal'),
                    'billing_event': adset.get('billing_event'),
                    'bid_strategy': adset.get('bid_strategy'),
                    'daily_budget': float(adset.get('daily_budget', 0)) / 100 if adset.get('daily_budget') else None,
                    'lifetime_budget': float(adset.get('lifetime_budget', 0)) / 100 if adset.get('lifetime_budget') else None,
                    'budget_remaining': float(adset.get('budget_remaining', 0)) / 100 if adset.get('budget_remaining') else None,
                    'target_countries': ','.join(countries) if countries else None,
                    'created_time': adset.get('created_time'),
                    'start_time': adset.get('start_time'),
                    'end_time': adset.get('end_time'),
                    'extracted_at': datetime.now().isoformat()
                }
                data.append(record)
            
            df = pd.DataFrame(data)
            logger.info(f"Extracted {len(df)} ad sets")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting ad sets: {e}")
            return pd.DataFrame()
    
    def extract_adset_insights(self, days: int = None,
                               start_date: str = None,
                               end_date: str = None,
                               lifetime: bool = False) -> pd.DataFrame:
        """
        Extract daily ad set-level insights.
        
        Returns:
            DataFrame with daily ad set metrics
        """
        logger.info(f"Extracting ad set insights for {self.ad_account_id}")
        
        date_range = self._get_date_range(days, start_date, end_date, lifetime)
        
        try:
            insights = self.account.get_insights(
                fields=['adset_id', 'adset_name', 'campaign_id', 'campaign_name'] + self.STANDARD_METRICS,
                params={
                    'time_range': date_range,
                    'time_increment': 1,
                    'level': 'adset'
                }
            )
            
            data = []
            for row in insights:
                record = {
                    'date': row.get('date_start'),
                    'ad_account_id': self.ad_account_id,
                    'campaign_id': row.get('campaign_id'),
                    'campaign_name': row.get('campaign_name'),
                    'adset_id': row.get('adset_id'),
                    'adset_name': row.get('adset_name'),
                    'impressions': int(row.get('impressions', 0)),
                    'reach': int(row.get('reach', 0)),
                    'clicks': int(row.get('clicks', 0)),
                    'unique_clicks': int(row.get('unique_clicks', 0)),
                    'spend': float(row.get('spend', 0)),
                    'ctr': float(row.get('ctr', 0)),
                    'cpc': float(row.get('cpc', 0)),
                    'cpm': float(row.get('cpm', 0)),
                    'frequency': float(row.get('frequency', 0)),
                    'link_clicks': self._parse_actions(row.get('actions'), 'link_click'),
                    'app_installs': self._parse_actions(row.get('actions'), 'app_install'),
                    'purchases': self._parse_actions(row.get('actions'), 'purchase'),
                    'leads': self._parse_actions(row.get('actions'), 'lead'),
                    'purchase_value': self._parse_action_values(row.get('action_values'), 'purchase'),
                    'extracted_at': datetime.now().isoformat()
                }
                data.append(record)
            
            df = pd.DataFrame(data)
            logger.info(f"Extracted {len(df)} ad set insight records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting ad set insights: {e}")
            return pd.DataFrame()
    
    def extract_ads(self) -> pd.DataFrame:
        """
        Extract ad metadata.
        
        Returns:
            DataFrame with ad information
        """
        logger.info(f"Extracting ads for {self.ad_account_id}")
        
        try:
            ads = self.account.get_ads(fields=self.AD_FIELDS)
            
            data = []
            for ad in ads:
                creative = ad.get('creative', {})
                
                record = {
                    'ad_id': ad.get('id'),
                    'ad_account_id': self.ad_account_id,
                    'campaign_id': ad.get('campaign_id'),
                    'adset_id': ad.get('adset_id'),
                    'ad_name': ad.get('name'),
                    'status': ad.get('status'),
                    'effective_status': ad.get('effective_status'),
                    'creative_id': creative.get('id') if isinstance(creative, dict) else None,
                    'created_time': ad.get('created_time'),
                    'extracted_at': datetime.now().isoformat()
                }
                data.append(record)
            
            df = pd.DataFrame(data)
            logger.info(f"Extracted {len(df)} ads")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting ads: {e}")
            return pd.DataFrame()
    
    def extract_ad_insights(self, days: int = None,
                            start_date: str = None,
                            end_date: str = None,
                            lifetime: bool = False) -> pd.DataFrame:
        """
        Extract daily ad-level insights.
        
        Returns:
            DataFrame with daily ad metrics
        """
        logger.info(f"Extracting ad insights for {self.ad_account_id}")
        
        date_range = self._get_date_range(days, start_date, end_date, lifetime)
        
        try:
            insights = self.account.get_insights(
                fields=['ad_id', 'ad_name', 'adset_id', 'adset_name', 'campaign_id', 'campaign_name'] + self.STANDARD_METRICS,
                params={
                    'time_range': date_range,
                    'time_increment': 1,
                    'level': 'ad'
                }
            )
            
            data = []
            for row in insights:
                record = {
                    'date': row.get('date_start'),
                    'ad_account_id': self.ad_account_id,
                    'campaign_id': row.get('campaign_id'),
                    'campaign_name': row.get('campaign_name'),
                    'adset_id': row.get('adset_id'),
                    'adset_name': row.get('adset_name'),
                    'ad_id': row.get('ad_id'),
                    'ad_name': row.get('ad_name'),
                    'impressions': int(row.get('impressions', 0)),
                    'reach': int(row.get('reach', 0)),
                    'clicks': int(row.get('clicks', 0)),
                    'spend': float(row.get('spend', 0)),
                    'ctr': float(row.get('ctr', 0)),
                    'cpc': float(row.get('cpc', 0)),
                    'cpm': float(row.get('cpm', 0)),
                    'link_clicks': self._parse_actions(row.get('actions'), 'link_click'),
                    'app_installs': self._parse_actions(row.get('actions'), 'app_install'),
                    'purchases': self._parse_actions(row.get('actions'), 'purchase'),
                    'purchase_value': self._parse_action_values(row.get('action_values'), 'purchase'),
                    'extracted_at': datetime.now().isoformat()
                }
                data.append(record)
            
            df = pd.DataFrame(data)
            logger.info(f"Extracted {len(df)} ad insight records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting ad insights: {e}")
            return pd.DataFrame()
    
    def extract_geographic_insights(self, days: int = None,
                                    start_date: str = None,
                                    end_date: str = None,
                                    lifetime: bool = False) -> pd.DataFrame:
        """
        Extract insights broken down by country.
        
        Returns:
            DataFrame with geographic metrics
        """
        logger.info(f"Extracting geographic insights for {self.ad_account_id}")
        
        date_range = self._get_date_range(days, start_date, end_date, lifetime)
        
        try:
            insights = self.account.get_insights(
                fields=self.STANDARD_METRICS,
                params={
                    'time_range': date_range,
                    'breakdowns': ['country'],
                    'level': 'account'
                }
            )
            
            data = []
            for row in insights:
                record = {
                    'date_start': row.get('date_start'),
                    'date_stop': row.get('date_stop'),
                    'ad_account_id': self.ad_account_id,
                    'country': row.get('country'),
                    'impressions': int(row.get('impressions', 0)),
                    'reach': int(row.get('reach', 0)),
                    'clicks': int(row.get('clicks', 0)),
                    'spend': float(row.get('spend', 0)),
                    'ctr': float(row.get('ctr', 0)),
                    'cpc': float(row.get('cpc', 0)),
                    'cpm': float(row.get('cpm', 0)),
                    'app_installs': self._parse_actions(row.get('actions'), 'app_install'),
                    'purchases': self._parse_actions(row.get('actions'), 'purchase'),
                    'purchase_value': self._parse_action_values(row.get('action_values'), 'purchase'),
                    'extracted_at': datetime.now().isoformat()
                }
                data.append(record)
            
            df = pd.DataFrame(data)
            logger.info(f"Extracted {len(df)} geographic records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting geographic insights: {e}")
            return pd.DataFrame()
    
    def extract_device_insights(self, days: int = None,
                                start_date: str = None,
                                end_date: str = None,
                                lifetime: bool = False) -> pd.DataFrame:
        """
        Extract insights broken down by device platform.
        
        Returns:
            DataFrame with device metrics
        """
        logger.info(f"Extracting device insights for {self.ad_account_id}")
        
        date_range = self._get_date_range(days, start_date, end_date, lifetime)
        
        try:
            insights = self.account.get_insights(
                fields=self.STANDARD_METRICS,
                params={
                    'time_range': date_range,
                    'breakdowns': ['device_platform', 'publisher_platform'],
                    'level': 'account'
                }
            )
            
            data = []
            for row in insights:
                record = {
                    'date_start': row.get('date_start'),
                    'date_stop': row.get('date_stop'),
                    'ad_account_id': self.ad_account_id,
                    'device_platform': row.get('device_platform'),
                    'publisher_platform': row.get('publisher_platform'),
                    'impressions': int(row.get('impressions', 0)),
                    'reach': int(row.get('reach', 0)),
                    'clicks': int(row.get('clicks', 0)),
                    'spend': float(row.get('spend', 0)),
                    'ctr': float(row.get('ctr', 0)),
                    'cpc': float(row.get('cpc', 0)),
                    'cpm': float(row.get('cpm', 0)),
                    'app_installs': self._parse_actions(row.get('actions'), 'app_install'),
                    'purchases': self._parse_actions(row.get('actions'), 'purchase'),
                    'extracted_at': datetime.now().isoformat()
                }
                data.append(record)
            
            df = pd.DataFrame(data)
            logger.info(f"Extracted {len(df)} device records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting device insights: {e}")
            return pd.DataFrame()
    
    def extract_age_gender_insights(self, days: int = None,
                                    start_date: str = None,
                                    end_date: str = None,
                                    lifetime: bool = False) -> pd.DataFrame:
        """
        Extract insights broken down by age and gender.
        
        Returns:
            DataFrame with demographic metrics
        """
        logger.info(f"Extracting age/gender insights for {self.ad_account_id}")
        
        date_range = self._get_date_range(days, start_date, end_date, lifetime)
        
        try:
            insights = self.account.get_insights(
                fields=self.STANDARD_METRICS,
                params={
                    'time_range': date_range,
                    'breakdowns': ['age', 'gender'],
                    'level': 'account'
                }
            )
            
            data = []
            for row in insights:
                record = {
                    'date_start': row.get('date_start'),
                    'date_stop': row.get('date_stop'),
                    'ad_account_id': self.ad_account_id,
                    'age': row.get('age'),
                    'gender': row.get('gender'),
                    'impressions': int(row.get('impressions', 0)),
                    'reach': int(row.get('reach', 0)),
                    'clicks': int(row.get('clicks', 0)),
                    'spend': float(row.get('spend', 0)),
                    'ctr': float(row.get('ctr', 0)),
                    'cpc': float(row.get('cpc', 0)),
                    'cpm': float(row.get('cpm', 0)),
                    'app_installs': self._parse_actions(row.get('actions'), 'app_install'),
                    'purchases': self._parse_actions(row.get('actions'), 'purchase'),
                    'extracted_at': datetime.now().isoformat()
                }
                data.append(record)
            
            df = pd.DataFrame(data)
            logger.info(f"Extracted {len(df)} demographic records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting demographic insights: {e}")
            return pd.DataFrame()
    
    def extract_all(self, days: int = None,
                    start_date: str = None,
                    end_date: str = None,
                    lifetime: bool = False) -> Dict[str, pd.DataFrame]:
        """
        Extract all available data.
        
        Returns:
            Dictionary of DataFrames with all extracted data
        """
        logger.info(f"Starting full extraction for {self.ad_account_id}")
        
        results = {
            'daily_account': self.extract_daily_account_insights(days, start_date, end_date, lifetime),
            'campaigns': self.extract_campaigns(),
            'campaign_insights': self.extract_campaign_insights(days, start_date, end_date, lifetime),
            'adsets': self.extract_adsets(),
            'adset_insights': self.extract_adset_insights(days, start_date, end_date, lifetime),
            'ads': self.extract_ads(),
            'ad_insights': self.extract_ad_insights(days, start_date, end_date, lifetime),
            'geographic': self.extract_geographic_insights(days, start_date, end_date, lifetime),
            'devices': self.extract_device_insights(days, start_date, end_date, lifetime),
            'demographics': self.extract_age_gender_insights(days, start_date, end_date, lifetime),
        }
        
        total_rows = sum(len(df) for df in results.values())
        logger.info(f"Full extraction complete: {total_rows} total rows across {len(results)} tables")
        
        return results
