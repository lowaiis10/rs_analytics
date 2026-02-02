"""
rs_analytics Streamlit Dashboard

Main entry point for the Streamlit analytics dashboard.
This app provides:
- GA4 Analytics data visualization
- Google Search Console (SEO) data visualization
- Google Ads (PPC) data visualization
- Meta (Facebook) Ads visualization
- Twitter/X analytics
- ETL Control Panel (manual data pulls)
- ETL status monitoring
- Configuration validation
- Connection testing

Usage:
    streamlit run app/main.py
    
Security:
    - Configuration is validated at startup
    - Credentials are NEVER displayed in the UI
    - All sensitive data handling follows security best practices
"""

import sys
import subprocess
import threading
import queue
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

import streamlit as st
import pandas as pd
import duckdb

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


# ============================================
# Page Configuration
# ============================================
st.set_page_config(
    page_title="rs_analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ============================================
# Configuration Loading
# ============================================
@st.cache_resource
def load_ga4_configuration():
    """Load and validate GA4 configuration."""
    try:
        from etl.config import get_config, ConfigurationError
        config = get_config()
        return config, None
    except Exception as e:
        return None, str(e)


@st.cache_resource
def load_gsc_configuration():
    """Load and validate GSC configuration."""
    try:
        from etl.gsc_config import get_gsc_config
        config = get_gsc_config()
        return config, None
    except Exception as e:
        return None, str(e)


@st.cache_resource
def load_gads_configuration():
    """Load and validate Google Ads configuration."""
    try:
        from etl.gads_config import get_gads_config
        config = get_gads_config()
        return config, None
    except Exception as e:
        return None, str(e)


@st.cache_resource
def load_meta_configuration():
    """Load and validate Meta Ads configuration."""
    try:
        from etl.meta_config import get_meta_config
        config = get_meta_config()
        return config, None
    except Exception as e:
        return None, str(e)


# ============================================
# Data Loading Functions
# ============================================
@st.cache_data(ttl=300)
def load_duckdb_data(duckdb_path: str, query: str) -> Optional[pd.DataFrame]:
    """Load data from DuckDB with caching."""
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        df = conn.execute(query).fetchdf()
        conn.close()
        return df
    except Exception as e:
        return None


def get_table_info(duckdb_path: str) -> dict:
    """Get information about all tables in the database."""
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        tables_df = conn.execute("SHOW TABLES").fetchdf()
        
        table_info = {}
        for table in tables_df['name'].tolist():
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                table_info[table] = count
            except:
                table_info[table] = 0
        
        conn.close()
        return table_info
    except:
        return {}


def check_gsc_data_exists(duckdb_path: str) -> Tuple[bool, int, list]:
    """Check if GSC data exists in the database."""
    gsc_tables = [
        'gsc_queries', 'gsc_pages', 'gsc_countries', 'gsc_devices',
        'gsc_search_appearance', 'gsc_query_page', 'gsc_daily_totals'
    ]
    
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        tables_df = conn.execute("SHOW TABLES").fetchdf()
        existing_tables = tables_df['name'].tolist()
        
        found_tables = [t for t in gsc_tables if t in existing_tables]
        
        total_rows = 0
        for table in found_tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                total_rows += count
            except:
                pass
        
        conn.close()
        return len(found_tables) > 0, total_rows, found_tables
    except:
        return False, 0, []


def check_gads_data_exists(duckdb_path: str) -> Tuple[bool, int, list]:
    """Check if Google Ads data exists in the database."""
    gads_tables = [
        'gads_daily_summary', 'gads_campaigns', 'gads_ad_groups', 
        'gads_keywords', 'gads_ads', 'gads_devices', 
        'gads_geographic', 'gads_hourly', 'gads_conversions'
    ]
    
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        tables_df = conn.execute("SHOW TABLES").fetchdf()
        existing_tables = tables_df['name'].tolist()
        
        found_tables = [t for t in gads_tables if t in existing_tables]
        
        total_rows = 0
        for table in found_tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                total_rows += count
            except:
                pass
        
        conn.close()
        return len(found_tables) > 0, total_rows, found_tables
    except:
        return False, 0, []


def check_meta_data_exists(duckdb_path: str) -> Tuple[bool, int, list]:
    """Check if Meta Ads data exists in the database."""
    meta_tables = [
        'meta_daily_account', 'meta_campaigns', 'meta_campaign_insights',
        'meta_adsets', 'meta_adset_insights', 'meta_ads', 'meta_ad_insights',
        'meta_geographic', 'meta_devices', 'meta_demographics'
    ]
    
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        tables_df = conn.execute("SHOW TABLES").fetchdf()
        existing_tables = tables_df['name'].tolist()
        
        found_tables = [t for t in meta_tables if t in existing_tables]
        
        total_rows = 0
        for table in found_tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                total_rows += count
            except:
                pass
        
        conn.close()
        return len(found_tables) > 0, total_rows, found_tables
    except:
        return False, 0, []


def check_twitter_data_exists(duckdb_path: str) -> Tuple[bool, int, list]:
    """Check if Twitter data exists in the database."""
    twitter_tables = ['twitter_profile', 'twitter_tweets', 'twitter_daily_metrics']
    
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        tables_df = conn.execute("SHOW TABLES").fetchdf()
        existing_tables = tables_df['name'].tolist()
        
        found_tables = [t for t in twitter_tables if t in existing_tables]
        
        total_rows = 0
        for table in found_tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                total_rows += count
            except:
                pass
        
        conn.close()
        return len(found_tables) > 0, total_rows, found_tables
    except:
        return False, 0, []


@st.cache_resource
def load_twitter_configuration():
    """Load Twitter configuration (cached)."""
    try:
        from etl.twitter_config import get_twitter_config
        return get_twitter_config(), None
    except Exception as e:
        return None, str(e)


# ============================================
# GA4 Dashboard Page
# ============================================
def render_ga4_dashboard(config, duckdb_path: str):
    """Render the GA4 Analytics dashboard."""
    
    st.header("📊 Google Analytics 4 Dashboard")
    
    # Check if data exists
    table_info = get_table_info(duckdb_path)
    ga4_tables = [t for t in table_info.keys() if t.startswith('ga4_')]
    
    if not ga4_tables or sum(table_info.get(t, 0) for t in ga4_tables) == 0:
        st.info("""
        **No GA4 data available yet.**
        
        Run the ETL pipeline to populate the database:
        ```bash
        python scripts/run_etl.py
        ```
        
        Or for comprehensive data:
        ```bash
        python scripts/run_etl_comprehensive.py --lifetime
        ```
        """)
        return
    
    # Date range selector
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        date_range = st.selectbox(
            "Date Range",
            options=["Last 7 days", "Last 14 days", "Last 30 days", "Last 90 days", "All time"],
            index=2,
            key="ga4_date_range"
        )
    
    # Calculate date filter
    date_filters = {
        "Last 7 days": "date >= CURRENT_DATE - INTERVAL '7 days'",
        "Last 14 days": "date >= CURRENT_DATE - INTERVAL '14 days'",
        "Last 30 days": "date >= CURRENT_DATE - INTERVAL '30 days'",
        "Last 90 days": "date >= CURRENT_DATE - INTERVAL '90 days'",
        "All time": "1=1"
    }
    date_filter = date_filters.get(date_range, "1=1")
    
    # Summary Metrics
    st.subheader("Key Metrics")
    
    if 'ga4_sessions' in table_info:
        summary_query = f"""
        SELECT 
            SUM(sessions) as total_sessions,
            SUM(active_users) as total_users,
            SUM(new_users) as total_new_users,
            SUM(screen_page_views) as total_page_views,
            AVG(engagement_rate) as avg_engagement_rate
        FROM ga4_sessions
        WHERE {date_filter}
        """
        
        summary_df = load_duckdb_data(duckdb_path, summary_query)
        
        if summary_df is not None and not summary_df.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Sessions", f"{int(summary_df['total_sessions'].iloc[0] or 0):,}")
            with col2:
                st.metric("Active Users", f"{int(summary_df['total_users'].iloc[0] or 0):,}")
            with col3:
                st.metric("Page Views", f"{int(summary_df['total_page_views'].iloc[0] or 0):,}")
            with col4:
                engagement = summary_df['avg_engagement_rate'].iloc[0] or 0
                st.metric("Avg Engagement", f"{engagement:.1%}")
    
    st.divider()
    
    # Sessions Over Time
    st.subheader("Sessions Over Time")
    
    if 'ga4_sessions' in table_info:
        time_query = f"""
        SELECT 
            date,
            SUM(sessions) as sessions,
            SUM(active_users) as active_users
        FROM ga4_sessions
        WHERE {date_filter}
        GROUP BY date
        ORDER BY date
        """
        
        time_df = load_duckdb_data(duckdb_path, time_query)
        
        if time_df is not None and not time_df.empty:
            st.line_chart(time_df.set_index('date')[['sessions', 'active_users']])
    
    # Traffic Sources and Devices
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Top Traffic Sources")
        
        if 'ga4_sessions' in table_info:
            source_query = f"""
            SELECT 
                session_source as source,
                SUM(sessions) as sessions
            FROM ga4_sessions
            WHERE {date_filter} AND session_source IS NOT NULL AND session_source != '(direct)'
            GROUP BY session_source
            ORDER BY sessions DESC
            LIMIT 10
            """
            
            source_df = load_duckdb_data(duckdb_path, source_query)
            
            if source_df is not None and not source_df.empty:
                st.bar_chart(source_df.set_index('source'))
    
    with col2:
        st.subheader("Device Categories")
        
        if 'ga4_sessions' in table_info:
            device_query = f"""
            SELECT 
                device_category,
                SUM(sessions) as sessions
            FROM ga4_sessions
            WHERE {date_filter} AND device_category IS NOT NULL
            GROUP BY device_category
            ORDER BY sessions DESC
            """
            
            device_df = load_duckdb_data(duckdb_path, device_query)
            
            if device_df is not None and not device_df.empty:
                st.bar_chart(device_df.set_index('device_category'))
    
    # Raw Data
    with st.expander("📋 View Raw GA4 Data"):
        table_choice = st.selectbox(
            "Select Table",
            options=ga4_tables,
            key="ga4_table_choice"
        )
        
        if table_choice:
            raw_query = f"SELECT * FROM {table_choice} LIMIT 1000"
            raw_df = load_duckdb_data(duckdb_path, raw_query)
            
            if raw_df is not None:
                st.dataframe(raw_df, use_container_width=True)


# ============================================
# GSC Dashboard Page
# ============================================
def render_gsc_dashboard(gsc_config, duckdb_path: str):
    """Render the Google Search Console dashboard."""
    
    st.header("🔍 Google Search Console Dashboard")
    
    # Check if GSC data exists
    has_data, total_rows, gsc_tables = check_gsc_data_exists(duckdb_path)
    
    if not has_data:
        st.info("""
        **No GSC data available yet.**
        
        Run the GSC ETL pipeline to populate the database:
        ```bash
        python scripts/run_etl_gsc.py --lifetime
        ```
        
        First, test your GSC connection:
        ```bash
        python scripts/test_gsc_connection.py
        ```
        """)
        
        if gsc_config:
            st.caption(f"Configured site: {gsc_config.site_url}")
        return
    
    # Show available data summary
    st.success(f"GSC data loaded: {total_rows:,} total rows across {len(gsc_tables)} tables")
    
    # Date range filter
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        date_range = st.selectbox(
            "Date Range",
            options=["Last 7 days", "Last 14 days", "Last 30 days", "Last 90 days", "All time"],
            index=2,
            key="gsc_date_range"
        )
    
    days_map = {"Last 7 days": 7, "Last 14 days": 14, "Last 30 days": 30, "Last 90 days": 90, "All time": 9999}
    days = days_map.get(date_range, 30)
    date_cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    st.divider()
    
    # Key SEO Metrics
    st.subheader("📈 Key SEO Metrics")
    
    if 'gsc_daily_totals' in gsc_tables:
        totals_query = f"""
        SELECT 
            SUM(CAST(clicks AS INTEGER)) as total_clicks,
            SUM(CAST(impressions AS INTEGER)) as total_impressions,
            AVG(CAST(ctr AS DOUBLE)) as avg_ctr,
            AVG(CAST(position AS DOUBLE)) as avg_position
        FROM gsc_daily_totals
        WHERE date >= '{date_cutoff}'
        """
        
        totals_df = load_duckdb_data(duckdb_path, totals_query)
        
        if totals_df is not None and not totals_df.empty:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Clicks", f"{int(totals_df['total_clicks'].iloc[0] or 0):,}")
            with col2:
                st.metric("Total Impressions", f"{int(totals_df['total_impressions'].iloc[0] or 0):,}")
            with col3:
                st.metric("Average CTR", f"{float(totals_df['avg_ctr'].iloc[0] or 0):.2%}")
            with col4:
                st.metric("Avg Position", f"{float(totals_df['avg_position'].iloc[0] or 0):.1f}")
    
    st.divider()
    
    # Performance Over Time
    st.subheader("📊 Performance Over Time")
    
    if 'gsc_daily_totals' in gsc_tables:
        time_query = f"""
        SELECT date, CAST(clicks AS INTEGER) as clicks, CAST(impressions AS INTEGER) as impressions
        FROM gsc_daily_totals WHERE date >= '{date_cutoff}' ORDER BY date
        """
        time_df = load_duckdb_data(duckdb_path, time_query)
        if time_df is not None and not time_df.empty:
            st.line_chart(time_df.set_index('date'))
    
    st.divider()
    
    # Top Queries and Pages
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔑 Top Search Queries")
        if 'gsc_queries' in gsc_tables:
            queries_query = f"""
            SELECT query, SUM(CAST(clicks AS INTEGER)) as clicks, SUM(CAST(impressions AS INTEGER)) as impressions
            FROM gsc_queries WHERE date >= '{date_cutoff}' AND query IS NOT NULL
            GROUP BY query ORDER BY clicks DESC LIMIT 15
            """
            queries_df = load_duckdb_data(duckdb_path, queries_query)
            if queries_df is not None and not queries_df.empty:
                st.dataframe(queries_df, use_container_width=True, hide_index=True)
    
    with col2:
        st.subheader("📄 Top Pages")
        if 'gsc_pages' in gsc_tables:
            pages_query = f"""
            SELECT page, SUM(CAST(clicks AS INTEGER)) as clicks, SUM(CAST(impressions AS INTEGER)) as impressions
            FROM gsc_pages WHERE date >= '{date_cutoff}' AND page IS NOT NULL
            GROUP BY page ORDER BY clicks DESC LIMIT 15
            """
            pages_df = load_duckdb_data(duckdb_path, pages_query)
            if pages_df is not None and not pages_df.empty:
                display_df = pages_df.copy()
                display_df['page'] = display_df['page'].apply(lambda x: x.split('/')[-1] if x and len(x) > 40 else x)
                st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    # Raw Data Explorer
    with st.expander("📋 Explore Raw GSC Data"):
        table_choice = st.selectbox("Select Table", options=gsc_tables, key="gsc_table_choice")
        if table_choice:
            raw_df = load_duckdb_data(duckdb_path, f"SELECT * FROM {table_choice} LIMIT 1000")
            if raw_df is not None:
                st.dataframe(raw_df, use_container_width=True)


# ============================================
# Google Ads Dashboard Page
# ============================================
def render_gads_dashboard(gads_config, duckdb_path: str):
    """Render the Google Ads dashboard."""
    
    st.header("💰 Google Ads Dashboard")
    
    # Check if Google Ads data exists
    has_data, total_rows, gads_tables = check_gads_data_exists(duckdb_path)
    
    if not has_data:
        st.info("""
        **No Google Ads data available yet.**
        
        Run the Google Ads ETL pipeline to populate the database:
        ```bash
        python scripts/run_etl_gads.py --lifetime
        ```
        
        First, test your Google Ads connection:
        ```bash
        python scripts/test_gads_connection.py
        ```
        """)
        
        if gads_config:
            st.caption(f"Configured Customer ID: {gads_config.customer_id}")
        return
    
    # Show available data summary
    st.success(f"Google Ads data loaded: {total_rows:,} total rows across {len(gads_tables)} tables")
    
    # Date range filter
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        date_range = st.selectbox(
            "Date Range",
            options=["Last 7 days", "Last 14 days", "Last 30 days", "Last 90 days", "All time"],
            index=2,
            key="gads_date_range"
        )
    
    days_map = {"Last 7 days": 7, "Last 14 days": 14, "Last 30 days": 30, "Last 90 days": 90, "All time": 9999}
    days = days_map.get(date_range, 30)
    date_cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    st.divider()
    
    # ========================================
    # Key PPC Metrics
    # ========================================
    st.subheader("📈 Key PPC Metrics")
    
    if 'gads_daily_summary' in gads_tables:
        summary_query = f"""
        SELECT 
            SUM(impressions) as total_impressions,
            SUM(clicks) as total_clicks,
            SUM(cost) as total_cost,
            AVG(ctr) as avg_ctr,
            SUM(conversions) as total_conversions,
            SUM(conversions_value) as total_conversion_value
        FROM gads_daily_summary
        WHERE date >= '{date_cutoff}'
        """
        
        summary_df = load_duckdb_data(duckdb_path, summary_query)
        
        if summary_df is not None and not summary_df.empty:
            col1, col2, col3, col4, col5, col6 = st.columns(6)
            
            with col1:
                impressions = int(summary_df['total_impressions'].iloc[0] or 0)
                st.metric("Impressions", f"{impressions:,}")
            
            with col2:
                clicks = int(summary_df['total_clicks'].iloc[0] or 0)
                st.metric("Clicks", f"{clicks:,}")
            
            with col3:
                cost = float(summary_df['total_cost'].iloc[0] or 0)
                st.metric("Cost", f"${cost:,.2f}")
            
            with col4:
                ctr = float(summary_df['avg_ctr'].iloc[0] or 0)
                st.metric("Avg CTR", f"{ctr:.2%}")
            
            with col5:
                conversions = float(summary_df['total_conversions'].iloc[0] or 0)
                st.metric("Conversions", f"{conversions:,.1f}")
            
            with col6:
                conv_value = float(summary_df['total_conversion_value'].iloc[0] or 0)
                st.metric("Conv. Value", f"${conv_value:,.2f}")
    
    st.divider()
    
    # ========================================
    # Performance Over Time
    # ========================================
    st.subheader("📊 Performance Over Time")
    
    if 'gads_daily_summary' in gads_tables:
        time_query = f"""
        SELECT 
            date,
            SUM(clicks) as clicks,
            SUM(cost) as cost,
            SUM(conversions) as conversions
        FROM gads_daily_summary
        WHERE date >= '{date_cutoff}'
        GROUP BY date
        ORDER BY date
        """
        
        time_df = load_duckdb_data(duckdb_path, time_query)
        
        if time_df is not None and not time_df.empty:
            tab1, tab2, tab3 = st.tabs(["Clicks", "Cost", "Conversions"])
            
            with tab1:
                st.line_chart(time_df.set_index('date')['clicks'])
            
            with tab2:
                st.line_chart(time_df.set_index('date')['cost'])
            
            with tab3:
                st.line_chart(time_df.set_index('date')['conversions'])
    
    st.divider()
    
    # ========================================
    # Campaign Performance
    # ========================================
    st.subheader("🎯 Campaign Performance")
    
    if 'gads_campaigns' in gads_tables:
        campaigns_query = f"""
        SELECT 
            campaign_name,
            campaign_status,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(cost) as cost,
            AVG(ctr) as ctr,
            SUM(conversions) as conversions
        FROM gads_campaigns
        WHERE date >= '{date_cutoff}' AND campaign_name IS NOT NULL
        GROUP BY campaign_name, campaign_status
        ORDER BY cost DESC
        LIMIT 20
        """
        
        campaigns_df = load_duckdb_data(duckdb_path, campaigns_query)
        
        if campaigns_df is not None and not campaigns_df.empty:
            # Format for display
            display_df = campaigns_df.copy()
            display_df['cost'] = display_df['cost'].apply(lambda x: f"${x:,.2f}" if x else "$0.00")
            display_df['ctr'] = display_df['ctr'].apply(lambda x: f"{x:.2%}" if x else "0%")
            display_df['conversions'] = display_df['conversions'].apply(lambda x: f"{x:.1f}" if x else "0")
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            # Campaign cost chart
            chart_df = campaigns_df[['campaign_name', 'clicks', 'conversions']].copy()
            chart_df = chart_df.set_index('campaign_name')
            st.bar_chart(chart_df)
    
    st.divider()
    
    # ========================================
    # Keyword Performance
    # ========================================
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔑 Top Keywords")
        
        if 'gads_keywords' in gads_tables:
            keywords_query = f"""
            SELECT 
                keyword_text,
                keyword_match_type,
                SUM(impressions) as impressions,
                SUM(clicks) as clicks,
                SUM(cost) as cost,
                SUM(conversions) as conversions
            FROM gads_keywords
            WHERE date >= '{date_cutoff}' AND keyword_text IS NOT NULL
            GROUP BY keyword_text, keyword_match_type
            ORDER BY cost DESC
            LIMIT 15
            """
            
            keywords_df = load_duckdb_data(duckdb_path, keywords_query)
            
            if keywords_df is not None and not keywords_df.empty:
                display_df = keywords_df.copy()
                display_df['cost'] = display_df['cost'].apply(lambda x: f"${x:,.2f}" if x else "$0.00")
                st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    with col2:
        st.subheader("📱 Device Performance")
        
        if 'gads_devices' in gads_tables:
            devices_query = f"""
            SELECT 
                device,
                SUM(impressions) as impressions,
                SUM(clicks) as clicks,
                SUM(cost) as cost,
                AVG(ctr) as ctr,
                SUM(conversions) as conversions
            FROM gads_devices
            WHERE date >= '{date_cutoff}' AND device IS NOT NULL
            GROUP BY device
            ORDER BY cost DESC
            """
            
            devices_df = load_duckdb_data(duckdb_path, devices_query)
            
            if devices_df is not None and not devices_df.empty:
                # Device chart
                st.bar_chart(devices_df.set_index('device')[['clicks', 'conversions']])
                
                # Detailed table
                display_df = devices_df.copy()
                display_df['cost'] = display_df['cost'].apply(lambda x: f"${x:,.2f}" if x else "$0.00")
                display_df['ctr'] = display_df['ctr'].apply(lambda x: f"{x:.2%}" if x else "0%")
                st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    st.divider()
    
    # ========================================
    # Ad Group Performance
    # ========================================
    st.subheader("📂 Ad Group Performance")
    
    if 'gads_ad_groups' in gads_tables:
        ad_groups_query = f"""
        SELECT 
            campaign_name,
            ad_group_name,
            ad_group_status,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(cost) as cost,
            SUM(conversions) as conversions
        FROM gads_ad_groups
        WHERE date >= '{date_cutoff}' AND ad_group_name IS NOT NULL
        GROUP BY campaign_name, ad_group_name, ad_group_status
        ORDER BY cost DESC
        LIMIT 20
        """
        
        ad_groups_df = load_duckdb_data(duckdb_path, ad_groups_query)
        
        if ad_groups_df is not None and not ad_groups_df.empty:
            display_df = ad_groups_df.copy()
            display_df['cost'] = display_df['cost'].apply(lambda x: f"${x:,.2f}" if x else "$0.00")
            st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    st.divider()
    
    # ========================================
    # Geographic Performance
    # ========================================
    st.subheader("🌍 Geographic Performance")
    
    if 'gads_geographic' in gads_tables:
        geo_query = f"""
        SELECT 
            country_criterion_id,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(cost) as cost,
            SUM(conversions) as conversions
        FROM gads_geographic
        WHERE date >= '{date_cutoff}' AND country_criterion_id IS NOT NULL
        GROUP BY country_criterion_id
        ORDER BY cost DESC
        LIMIT 15
        """
        
        geo_df = load_duckdb_data(duckdb_path, geo_query)
        
        if geo_df is not None and not geo_df.empty:
            st.bar_chart(geo_df.set_index('country_criterion_id')['clicks'])
    
    # ========================================
    # Hourly Performance
    # ========================================
    st.subheader("🕐 Hourly Performance")
    
    if 'gads_hourly' in gads_tables:
        hourly_query = f"""
        SELECT 
            hour,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(cost) as cost,
            SUM(conversions) as conversions
        FROM gads_hourly
        WHERE date >= '{date_cutoff}' AND hour IS NOT NULL
        GROUP BY hour
        ORDER BY hour
        """
        
        hourly_df = load_duckdb_data(duckdb_path, hourly_query)
        
        if hourly_df is not None and not hourly_df.empty:
            st.line_chart(hourly_df.set_index('hour')[['clicks', 'conversions']])
    
    # ========================================
    # Raw Data Explorer
    # ========================================
    with st.expander("📋 Explore Raw Google Ads Data"):
        table_choice = st.selectbox(
            "Select Table",
            options=gads_tables,
            key="gads_table_choice"
        )
        
        if table_choice:
            raw_df = load_duckdb_data(duckdb_path, f"SELECT * FROM {table_choice} LIMIT 1000")
            if raw_df is not None:
                st.dataframe(raw_df, use_container_width=True)


# ============================================
# Meta Ads Dashboard Page (MBA-Level Marketing Analytics)
# ============================================
def render_meta_dashboard(meta_config, duckdb_path: str):
    """
    Render the Meta (Facebook) Ads MBA-level marketing dashboard.
    
    Features comprehensive marketing analytics including:
    - Executive KPIs with period comparisons
    - Campaign performance analysis
    - Ad Set (targeting) effectiveness
    - Creative performance analysis
    - Geographic and demographic insights
    - ROI and efficiency metrics
    - Budget pacing and optimization recommendations
    """
    
    st.header("📘 Meta Ads - Marketing Analytics Dashboard")
    
    # Check if Meta data exists
    has_data, total_rows, meta_tables = check_meta_data_exists(duckdb_path)
    
    if not has_data:
        st.info("""
        **No Meta Ads data available yet.**
        
        Run the Meta Ads ETL pipeline to populate the database:
        ```bash
        python scripts/run_etl_meta.py --lifetime
        ```
        
        First, test your Meta connection:
        ```bash
        python scripts/test_meta_connection.py
        ```
        """)
        
        if meta_config:
            st.caption(f"Configured accounts: {', '.join(meta_config.ad_account_ids)}")
        return
    
    # Show data summary
    st.success(f"📊 Meta Ads data loaded: **{total_rows:,}** total rows across **{len(meta_tables)}** tables")
    
    # ========================================
    # Date Range Filter
    # ========================================
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    
    with col1:
        date_range = st.selectbox(
            "📅 Analysis Period",
            options=["Last 7 days", "Last 14 days", "Last 30 days", "Last 90 days", "Last 180 days", "All time"],
            index=2,
            key="meta_date_range"
        )
    
    # Calculate date filter
    days_map = {
        "Last 7 days": 7, "Last 14 days": 14, "Last 30 days": 30,
        "Last 90 days": 90, "Last 180 days": 180, "All time": 9999
    }
    days = days_map.get(date_range, 30)
    date_cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    prev_date_cutoff = (datetime.now() - timedelta(days=days*2)).strftime('%Y-%m-%d')
    
    # Get account selector if multiple accounts
    with col2:
        accounts_query = "SELECT DISTINCT ad_account_id FROM meta_daily_account"
        accounts_df = load_duckdb_data(duckdb_path, accounts_query)
        
        if accounts_df is not None and len(accounts_df) > 1:
            account_options = ["All Accounts"] + accounts_df['ad_account_id'].tolist()
            selected_account = st.selectbox("Account", account_options, key="meta_account")
            account_filter = "" if selected_account == "All Accounts" else f"AND ad_account_id = '{selected_account}'"
        else:
            selected_account = "All Accounts"
            account_filter = ""
    
    st.divider()
    
    # ========================================
    # SECTION 1: EXECUTIVE KPI DASHBOARD
    # ========================================
    st.subheader("🎯 Executive Summary")
    
    # Current period metrics
    kpi_query = f"""
    SELECT 
        SUM(impressions) as impressions,
        SUM(reach) as reach,
        SUM(clicks) as clicks,
        SUM(spend) as spend,
        CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 100.0 / SUM(impressions) ELSE 0 END as ctr,
        CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END as cpc,
        CASE WHEN SUM(impressions) > 0 THEN SUM(spend) * 1000.0 / SUM(impressions) ELSE 0 END as cpm,
        CASE WHEN SUM(reach) > 0 THEN SUM(impressions) * 1.0 / SUM(reach) ELSE 0 END as frequency,
        SUM(app_installs) as app_installs,
        SUM(purchases) as purchases,
        SUM(purchase_value) as revenue,
        CASE WHEN SUM(app_installs) > 0 THEN SUM(spend) / SUM(app_installs) ELSE 0 END as cpi
    FROM meta_daily_account
    WHERE date >= '{date_cutoff}' {account_filter}
    """
    
    # Previous period metrics for comparison
    prev_kpi_query = f"""
    SELECT 
        SUM(impressions) as impressions,
        SUM(spend) as spend,
        SUM(clicks) as clicks,
        SUM(app_installs) as app_installs
    FROM meta_daily_account
    WHERE date >= '{prev_date_cutoff}' AND date < '{date_cutoff}' {account_filter}
    """
    
    kpi_df = load_duckdb_data(duckdb_path, kpi_query)
    prev_kpi_df = load_duckdb_data(duckdb_path, prev_kpi_query)
    
    if kpi_df is not None and not kpi_df.empty and kpi_df['spend'].iloc[0]:
        row = kpi_df.iloc[0]
        prev_row = prev_kpi_df.iloc[0] if prev_kpi_df is not None and not prev_kpi_df.empty else None
        
        # Calculate deltas
        def calc_delta(current, previous):
            if previous and previous > 0:
                return ((current - previous) / previous) * 100
            return None
        
        # Row 1: Core metrics
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            spend = row['spend'] or 0
            prev_spend = prev_row['spend'] if prev_row is not None else None
            delta = calc_delta(spend, prev_spend)
            st.metric(
                "💰 Total Spend",
                f"${spend:,.2f}",
                delta=f"{delta:+.1f}%" if delta else None,
                delta_color="inverse"
            )
        
        with col2:
            impressions = int(row['impressions'] or 0)
            delta = calc_delta(impressions, prev_row['impressions'] if prev_row is not None else None)
            st.metric(
                "👁️ Impressions",
                f"{impressions:,}",
                delta=f"{delta:+.1f}%" if delta else None
            )
        
        with col3:
            reach = int(row['reach'] or 0)
            st.metric("👥 Unique Reach", f"{reach:,}")
        
        with col4:
            clicks = int(row['clicks'] or 0)
            delta = calc_delta(clicks, prev_row['clicks'] if prev_row is not None else None)
            st.metric(
                "🖱️ Clicks",
                f"{clicks:,}",
                delta=f"{delta:+.1f}%" if delta else None
            )
        
        with col5:
            ctr = row['ctr'] or 0
            st.metric("📈 CTR", f"{ctr:.2f}%")
        
        with col6:
            cpc = row['cpc'] or 0
            st.metric("💵 CPC", f"${cpc:.2f}")
        
        # Row 2: Performance & Conversion metrics
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            cpm = row['cpm'] or 0
            st.metric("📊 CPM", f"${cpm:.2f}")
        
        with col2:
            frequency = row['frequency'] or 0
            st.metric("🔄 Frequency", f"{frequency:.2f}")
        
        with col3:
            installs = int(row['app_installs'] or 0)
            delta = calc_delta(installs, prev_row['app_installs'] if prev_row is not None else None)
            st.metric(
                "📱 App Installs",
                f"{installs:,}",
                delta=f"{delta:+.1f}%" if delta else None
            )
        
        with col4:
            cpi = row['cpi'] or 0
            st.metric("💳 Cost/Install", f"${cpi:.2f}")
        
        with col5:
            purchases = int(row['purchases'] or 0)
            st.metric("🛒 Purchases", f"{purchases:,}")
        
        with col6:
            revenue = row['revenue'] or 0
            roas = (revenue / spend * 100) if spend > 0 else 0
            st.metric("📈 ROAS", f"{roas:.1f}%")
    
    st.divider()
    
    # ========================================
    # SECTION 2: PERFORMANCE TRENDS
    # ========================================
    st.subheader("📈 Performance Trends")
    
    trend_query = f"""
    SELECT 
        date,
        SUM(impressions) as impressions,
        SUM(clicks) as clicks,
        SUM(spend) as spend,
        SUM(app_installs) as app_installs,
        CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 100.0 / SUM(impressions) ELSE 0 END as ctr,
        CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END as cpc
    FROM meta_daily_account
    WHERE date >= '{date_cutoff}' {account_filter}
    GROUP BY date
    ORDER BY date
    """
    
    trend_df = load_duckdb_data(duckdb_path, trend_query)
    
    if trend_df is not None and not trend_df.empty:
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Spend & Clicks", "👁️ Impressions", "📱 Conversions", "📈 Efficiency"])
        
        with tab1:
            col1, col2 = st.columns(2)
            with col1:
                st.line_chart(trend_df.set_index('date')['spend'], use_container_width=True)
                st.caption("Daily Spend ($)")
            with col2:
                st.line_chart(trend_df.set_index('date')['clicks'], use_container_width=True)
                st.caption("Daily Clicks")
        
        with tab2:
            st.area_chart(trend_df.set_index('date')['impressions'], use_container_width=True)
            st.caption("Daily Impressions")
        
        with tab3:
            st.bar_chart(trend_df.set_index('date')['app_installs'], use_container_width=True)
            st.caption("Daily App Installs")
        
        with tab4:
            col1, col2 = st.columns(2)
            with col1:
                st.line_chart(trend_df.set_index('date')['ctr'], use_container_width=True)
                st.caption("Click-Through Rate (%)")
            with col2:
                st.line_chart(trend_df.set_index('date')['cpc'], use_container_width=True)
                st.caption("Cost Per Click ($)")
    
    st.divider()
    
    # ========================================
    # SECTION 3: CAMPAIGN PERFORMANCE
    # ========================================
    st.subheader("🎯 Campaign Performance Analysis")
    
    if 'meta_campaign_insights' in meta_tables:
        campaign_query = f"""
        SELECT 
            campaign_name,
            campaign_id,
            SUM(impressions) as impressions,
            SUM(reach) as reach,
            SUM(clicks) as clicks,
            SUM(spend) as spend,
            CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 100.0 / SUM(impressions) ELSE 0 END as ctr,
            CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END as cpc,
            CASE WHEN SUM(impressions) > 0 THEN SUM(spend) * 1000.0 / SUM(impressions) ELSE 0 END as cpm,
            SUM(app_installs) as app_installs,
            CASE WHEN SUM(app_installs) > 0 THEN SUM(spend) / SUM(app_installs) ELSE 0 END as cpi,
            SUM(purchases) as purchases,
            SUM(purchase_value) as revenue
        FROM meta_campaign_insights
        WHERE date >= '{date_cutoff}' {account_filter}
        GROUP BY campaign_name, campaign_id
        ORDER BY spend DESC
        """
        
        campaign_df = load_duckdb_data(duckdb_path, campaign_query)
        
        if campaign_df is not None and not campaign_df.empty:
            # Campaign efficiency quadrant
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Format for display
                display_df = campaign_df.copy()
                display_df['spend'] = display_df['spend'].apply(lambda x: f"${x:,.2f}")
                display_df['ctr'] = display_df['ctr'].apply(lambda x: f"{x:.2f}%")
                display_df['cpc'] = display_df['cpc'].apply(lambda x: f"${x:.2f}")
                display_df['cpm'] = display_df['cpm'].apply(lambda x: f"${x:.2f}")
                display_df['cpi'] = display_df['cpi'].apply(lambda x: f"${x:.2f}" if x > 0 else "-")
                display_df['impressions'] = display_df['impressions'].apply(lambda x: f"{int(x):,}")
                display_df['clicks'] = display_df['clicks'].apply(lambda x: f"{int(x):,}")
                display_df['app_installs'] = display_df['app_installs'].apply(lambda x: f"{int(x):,}" if x > 0 else "-")
                
                st.dataframe(
                    display_df[['campaign_name', 'spend', 'impressions', 'clicks', 'ctr', 'cpc', 'app_installs', 'cpi']],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "campaign_name": "Campaign",
                        "spend": "Spend",
                        "impressions": "Impressions",
                        "clicks": "Clicks",
                        "ctr": "CTR",
                        "cpc": "CPC",
                        "app_installs": "Installs",
                        "cpi": "CPI"
                    }
                )
            
            with col2:
                st.markdown("**📊 Spend Distribution**")
                spend_data = campaign_df[['campaign_name', 'spend']].head(10)
                spend_data = spend_data[spend_data['spend'] > 0]
                if not spend_data.empty:
                    st.bar_chart(spend_data.set_index('campaign_name')['spend'])
        
        # Campaign time series
        st.markdown("**📈 Campaign Performance Over Time**")
        
        campaign_trend_query = f"""
        SELECT 
            date,
            campaign_name,
            SUM(spend) as spend,
            SUM(clicks) as clicks
        FROM meta_campaign_insights
        WHERE date >= '{date_cutoff}' {account_filter}
        GROUP BY date, campaign_name
        ORDER BY date
        """
        
        campaign_trend_df = load_duckdb_data(duckdb_path, campaign_trend_query)
        
        if campaign_trend_df is not None and not campaign_trend_df.empty:
            # Pivot for time series
            pivot_df = campaign_trend_df.pivot_table(
                index='date', 
                columns='campaign_name', 
                values='spend', 
                aggfunc='sum'
            ).fillna(0)
            
            if not pivot_df.empty:
                st.line_chart(pivot_df, use_container_width=True)
    
    st.divider()
    
    # ========================================
    # SECTION 4: AD SET PERFORMANCE
    # ========================================
    st.subheader("🎨 Ad Set (Targeting) Analysis")
    
    if 'meta_adset_insights' in meta_tables:
        adset_query = f"""
        SELECT 
            adset_name,
            campaign_name,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(spend) as spend,
            CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 100.0 / SUM(impressions) ELSE 0 END as ctr,
            CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END as cpc,
            SUM(app_installs) as app_installs,
            CASE WHEN SUM(app_installs) > 0 THEN SUM(spend) / SUM(app_installs) ELSE 0 END as cpi
        FROM meta_adset_insights
        WHERE date >= '{date_cutoff}' {account_filter}
        GROUP BY adset_name, campaign_name
        ORDER BY spend DESC
        LIMIT 20
        """
        
        adset_df = load_duckdb_data(duckdb_path, adset_query)
        
        if adset_df is not None and not adset_df.empty:
            # Format for display
            display_df = adset_df.copy()
            display_df['spend'] = display_df['spend'].apply(lambda x: f"${x:,.2f}")
            display_df['ctr'] = display_df['ctr'].apply(lambda x: f"{x:.2f}%")
            display_df['cpc'] = display_df['cpc'].apply(lambda x: f"${x:.2f}")
            display_df['cpi'] = display_df['cpi'].apply(lambda x: f"${x:.2f}" if x > 0 else "-")
            display_df['impressions'] = display_df['impressions'].apply(lambda x: f"{int(x):,}")
            display_df['clicks'] = display_df['clicks'].apply(lambda x: f"{int(x):,}")
            display_df['app_installs'] = display_df['app_installs'].apply(lambda x: f"{int(x):,}" if x > 0 else "-")
            
            st.dataframe(
                display_df[['adset_name', 'campaign_name', 'spend', 'clicks', 'ctr', 'cpc', 'app_installs', 'cpi']],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "adset_name": "Ad Set",
                    "campaign_name": "Campaign",
                    "spend": "Spend",
                    "clicks": "Clicks",
                    "ctr": "CTR",
                    "cpc": "CPC",
                    "app_installs": "Installs",
                    "cpi": "CPI"
                }
            )
    
    st.divider()
    
    # ========================================
    # SECTION 5: GEOGRAPHIC ANALYSIS
    # ========================================
    st.subheader("🌍 Geographic Performance")
    
    col1, col2 = st.columns(2)
    
    if 'meta_geographic' in meta_tables:
        # Note: Geographic data is aggregated (not daily), so no date filter needed
        geo_where = f"WHERE 1=1 {account_filter}" if account_filter else ""
        geo_query = f"""
        SELECT 
            country,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(spend) as spend,
            CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 100.0 / SUM(impressions) ELSE 0 END as ctr,
            CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END as cpc,
            SUM(app_installs) as app_installs,
            CASE WHEN SUM(app_installs) > 0 THEN SUM(spend) / SUM(app_installs) ELSE 0 END as cpi
        FROM meta_geographic
        {geo_where}
        GROUP BY country
        ORDER BY spend DESC
        """
        
        geo_df = load_duckdb_data(duckdb_path, geo_query)
        
        if geo_df is not None and not geo_df.empty:
            with col1:
                st.markdown("**🗺️ Spend by Country**")
                st.bar_chart(geo_df.set_index('country')['spend'].head(10))
            
            with col2:
                st.markdown("**📊 Country Metrics**")
                display_df = geo_df.copy()
                display_df['spend'] = display_df['spend'].apply(lambda x: f"${x:,.2f}")
                display_df['ctr'] = display_df['ctr'].apply(lambda x: f"{x:.2f}%")
                display_df['cpc'] = display_df['cpc'].apply(lambda x: f"${x:.2f}")
                display_df['cpi'] = display_df['cpi'].apply(lambda x: f"${x:.2f}" if x > 0 else "-")
                display_df['clicks'] = display_df['clicks'].apply(lambda x: f"{int(x):,}")
                display_df['app_installs'] = display_df['app_installs'].apply(lambda x: f"{int(x):,}" if x > 0 else "-")
                
                st.dataframe(
                    display_df[['country', 'spend', 'clicks', 'ctr', 'cpc', 'app_installs', 'cpi']].head(10),
                    use_container_width=True,
                    hide_index=True
                )
    
    st.divider()
    
    # ========================================
    # SECTION 6: DEVICE & PLATFORM ANALYSIS
    # ========================================
    st.subheader("📱 Device & Platform Analysis")
    
    col1, col2 = st.columns(2)
    
    if 'meta_devices' in meta_tables:
        # Note: Device data is aggregated (not daily), so no date filter needed
        device_where = f"WHERE 1=1 {account_filter}" if account_filter else ""
        device_query = f"""
        SELECT 
            device_platform,
            publisher_platform,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(spend) as spend,
            CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 100.0 / SUM(impressions) ELSE 0 END as ctr,
            CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END as cpc,
            SUM(app_installs) as app_installs
        FROM meta_devices
        {device_where}
        GROUP BY device_platform, publisher_platform
        ORDER BY spend DESC
        """
        
        device_df = load_duckdb_data(duckdb_path, device_query)
        
        if device_df is not None and not device_df.empty:
            with col1:
                st.markdown("**📲 Device Platform**")
                device_agg = device_df.groupby('device_platform')['spend'].sum().reset_index()
                st.bar_chart(device_agg.set_index('device_platform'))
            
            with col2:
                st.markdown("**📡 Publisher Platform**")
                pub_agg = device_df.groupby('publisher_platform')['spend'].sum().reset_index()
                st.bar_chart(pub_agg.set_index('publisher_platform'))
            
            # Detailed table
            st.markdown("**📊 Detailed Platform Metrics**")
            display_df = device_df.copy()
            display_df['spend'] = display_df['spend'].apply(lambda x: f"${x:,.2f}")
            display_df['ctr'] = display_df['ctr'].apply(lambda x: f"{x:.2f}%")
            display_df['cpc'] = display_df['cpc'].apply(lambda x: f"${x:.2f}")
            display_df['impressions'] = display_df['impressions'].apply(lambda x: f"{int(x):,}")
            display_df['clicks'] = display_df['clicks'].apply(lambda x: f"{int(x):,}")
            
            st.dataframe(
                display_df[['device_platform', 'publisher_platform', 'spend', 'impressions', 'clicks', 'ctr', 'cpc']],
                use_container_width=True,
                hide_index=True
            )
    
    st.divider()
    
    # ========================================
    # SECTION 7: DEMOGRAPHICS ANALYSIS
    # ========================================
    st.subheader("👥 Demographics Analysis")
    
    if 'meta_demographics' in meta_tables:
        # Note: Demographics data is aggregated (not daily), so no date filter needed
        demo_where = f"WHERE 1=1 {account_filter}" if account_filter else ""
        demo_query = f"""
        SELECT 
            age,
            gender,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(spend) as spend,
            CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 100.0 / SUM(impressions) ELSE 0 END as ctr,
            CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END as cpc,
            SUM(app_installs) as app_installs
        FROM meta_demographics
        {demo_where}
        GROUP BY age, gender
        ORDER BY spend DESC
        """
        
        demo_df = load_duckdb_data(duckdb_path, demo_query)
        
        if demo_df is not None and not demo_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**👤 Spend by Age Group**")
                age_agg = demo_df.groupby('age')['spend'].sum().reset_index()
                # Sort by age properly
                age_order = ['13-17', '18-24', '25-34', '35-44', '45-54', '55-64', '65+']
                age_agg['age'] = pd.Categorical(age_agg['age'], categories=age_order, ordered=True)
                age_agg = age_agg.sort_values('age')
                st.bar_chart(age_agg.set_index('age'))
            
            with col2:
                st.markdown("**⚧️ Spend by Gender**")
                gender_agg = demo_df.groupby('gender')['spend'].sum().reset_index()
                st.bar_chart(gender_agg.set_index('gender'))
            
            # Demographics heatmap-style table
            st.markdown("**📊 Age x Gender Performance Matrix**")
            
            # Pivot for matrix view
            matrix_df = demo_df.pivot_table(
                index='age',
                columns='gender',
                values='spend',
                aggfunc='sum'
            ).fillna(0)
            
            if not matrix_df.empty:
                # Format as currency
                formatted_matrix = matrix_df.applymap(lambda x: f"${x:,.2f}")
                st.dataframe(formatted_matrix, use_container_width=True)
    
    st.divider()
    
    # ========================================
    # SECTION 8: RAW DATA EXPLORER
    # ========================================
    with st.expander("📋 Explore Raw Meta Ads Data"):
        table_choice = st.selectbox(
            "Select Table",
            options=meta_tables,
            key="meta_table_choice"
        )
        
        if table_choice:
            raw_df = load_duckdb_data(duckdb_path, f"SELECT * FROM {table_choice} ORDER BY date DESC LIMIT 1000")
            if raw_df is not None:
                st.dataframe(raw_df, use_container_width=True)
    
    # ========================================
    # SECTION 9: MBA INSIGHTS & RECOMMENDATIONS
    # ========================================
    st.divider()
    st.subheader("💡 Strategic Insights & Recommendations")
    
    if kpi_df is not None and not kpi_df.empty and kpi_df['spend'].iloc[0]:
        row = kpi_df.iloc[0]
        
        insights = []
        
        # CTR analysis
        ctr = row['ctr'] or 0
        if ctr < 0.5:
            insights.append("⚠️ **Low CTR Alert**: CTR is below 0.5%. Consider refreshing ad creatives or refining targeting.")
        elif ctr > 1.5:
            insights.append("✅ **Strong CTR**: CTR exceeds 1.5%, indicating good audience-creative fit.")
        
        # Frequency analysis
        frequency = row['frequency'] or 0
        if frequency > 3:
            insights.append("⚠️ **High Frequency Warning**: Frequency > 3 may cause ad fatigue. Consider expanding audience or refreshing creatives.")
        
        # CPI analysis (if app installs)
        cpi = row['cpi'] or 0
        installs = row['app_installs'] or 0
        if installs > 0:
            if cpi > 5:
                insights.append(f"⚠️ **CPI Optimization Needed**: Cost per install (${cpi:.2f}) is high. Review targeting and creatives.")
            elif cpi < 2:
                insights.append(f"✅ **Efficient CPI**: Cost per install (${cpi:.2f}) is efficient. Consider scaling budget.")
        
        # Budget efficiency
        spend = row['spend'] or 0
        clicks = row['clicks'] or 0
        if spend > 0 and clicks > 0:
            efficiency_ratio = clicks / spend
            if efficiency_ratio < 0.5:
                insights.append("📊 **Budget Efficiency**: Consider reallocating budget to higher-performing campaigns.")
        
        if insights:
            for insight in insights:
                st.markdown(insight)
        else:
            st.info("📊 Performance metrics are within normal ranges. Continue monitoring for trends.")


# ============================================
# Twitter/X Dashboard Page
# ============================================
def render_twitter_dashboard(twitter_config, duckdb_path: str):
    """
    Render the Twitter/X organic analytics dashboard.
    
    Displays:
    - Profile metrics (followers, following, tweets)
    - Tweet performance (impressions, engagements)
    - Daily metrics trends
    - Top performing tweets
    """
    
    st.header("🐦 Twitter/X - Page Analytics Dashboard")
    
    # Check if data exists
    has_data, total_rows, twitter_tables = check_twitter_data_exists(duckdb_path)
    
    if not has_data or total_rows == 0:
        st.info("""
        **No Twitter data available yet.**
        
        To populate data:
        1. Ensure Twitter API credentials are configured in `.env`
        2. Run the ETL: `python scripts/run_etl_twitter.py`
        
        **Note:** Twitter API requires a paid subscription ($100+/month) for read access.
        """)
        
        # Show ETL instructions
        with st.expander("📋 Setup Instructions"):
            st.markdown("""
            ### Twitter API Setup
            
            1. **Get API Access**: Sign up at [developer.twitter.com](https://developer.twitter.com)
            2. **Subscribe to Basic tier** ($100/month) for read access
            3. **Configure credentials** in `.env`:
               ```
               ENABLE_TWITTER=1
               TWITTER_BEARER_TOKEN=your_bearer_token
               TWITTER_CONSUMER_KEY=your_consumer_key
               TWITTER_CONSUMER_SECRET=your_consumer_secret
               TWITTER_ACCESS_TOKEN=your_access_token
               TWITTER_ACCESS_TOKEN_SECRET=your_access_token_secret
               TWITTER_USERNAME=YourUsername
               ```
            4. **Test connection**: `python scripts/test_twitter_connection.py`
            5. **Run ETL**: `python scripts/run_etl_twitter.py`
            """)
        return
    
    # Date filter
    col1, col2 = st.columns([3, 1])
    with col2:
        date_range = st.selectbox(
            "Time Period",
            options=["Last 7 Days", "Last 14 Days", "Last 30 Days", "All Time"],
            index=2,
            key="twitter_date_range"
        )
    
    # Calculate date cutoff
    if date_range == "Last 7 Days":
        date_cutoff = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    elif date_range == "Last 14 Days":
        date_cutoff = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
    elif date_range == "Last 30 Days":
        date_cutoff = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    else:
        date_cutoff = "2020-01-01"
    
    # ========================================
    # SECTION 1: PROFILE OVERVIEW
    # ========================================
    st.subheader("👤 Profile Overview")
    
    if 'twitter_profile' in twitter_tables:
        profile_query = """
        SELECT *
        FROM twitter_profile
        ORDER BY snapshot_date DESC
        LIMIT 1
        """
        profile_df = load_duckdb_data(duckdb_path, profile_query)
        
        if profile_df is not None and not profile_df.empty:
            row = profile_df.iloc[0]
            
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("👥 Followers", f"{int(row['followers_count']):,}")
            with col2:
                st.metric("➡️ Following", f"{int(row['following_count']):,}")
            with col3:
                st.metric("📝 Total Tweets", f"{int(row['tweet_count']):,}")
            with col4:
                st.metric("📋 Listed", f"{int(row['listed_count']):,}")
            with col5:
                verified = "✅ Yes" if row.get('verified', False) else "❌ No"
                st.metric("✓ Verified", verified)
            
            # Profile info
            with st.expander("📄 Profile Details"):
                st.markdown(f"**Username:** @{row['username']}")
                st.markdown(f"**Name:** {row['name']}")
                st.markdown(f"**Bio:** {row.get('description', 'N/A')}")
                st.markdown(f"**Location:** {row.get('location', 'N/A')}")
                st.markdown(f"**Account Created:** {row.get('created_at', 'N/A')[:10] if row.get('created_at') else 'N/A'}")
                st.markdown(f"**Last Updated:** {row['snapshot_date']}")
    
    st.divider()
    
    # ========================================
    # SECTION 2: ENGAGEMENT METRICS
    # ========================================
    st.subheader("📊 Engagement Metrics")
    
    if 'twitter_tweets' in twitter_tables:
        # Aggregate metrics
        metrics_query = f"""
        SELECT 
            COUNT(*) as total_tweets,
            SUM(impressions) as total_impressions,
            SUM(likes) as total_likes,
            SUM(retweets) as total_retweets,
            SUM(replies) as total_replies,
            SUM(quotes) as total_quotes,
            SUM(bookmarks) as total_bookmarks,
            SUM(likes + retweets + replies + quotes + bookmarks) as total_engagements,
            AVG(likes) as avg_likes,
            AVG(retweets) as avg_retweets
        FROM twitter_tweets
        WHERE created_date >= '{date_cutoff}'
        """
        
        metrics_df = load_duckdb_data(duckdb_path, metrics_query)
        
        if metrics_df is not None and not metrics_df.empty:
            row = metrics_df.iloc[0]
            
            col1, col2, col3, col4, col5, col6 = st.columns(6)
            
            with col1:
                st.metric("📝 Tweets", f"{int(row['total_tweets'] or 0):,}")
            with col2:
                st.metric("👁️ Impressions", f"{int(row['total_impressions'] or 0):,}")
            with col3:
                st.metric("❤️ Likes", f"{int(row['total_likes'] or 0):,}")
            with col4:
                st.metric("🔄 Retweets", f"{int(row['total_retweets'] or 0):,}")
            with col5:
                st.metric("💬 Replies", f"{int(row['total_replies'] or 0):,}")
            with col6:
                engagements = int(row['total_engagements'] or 0)
                impressions = int(row['total_impressions'] or 1)
                engagement_rate = (engagements / impressions * 100) if impressions > 0 else 0
                st.metric("📈 Eng. Rate", f"{engagement_rate:.2f}%")
    
    st.divider()
    
    # ========================================
    # SECTION 3: DAILY TRENDS
    # ========================================
    st.subheader("📈 Daily Performance Trends")
    
    if 'twitter_daily_metrics' in twitter_tables:
        trend_query = f"""
        SELECT 
            date,
            tweet_count,
            impressions,
            likes,
            retweets,
            replies,
            quotes,
            total_engagements,
            engagement_rate
        FROM twitter_daily_metrics
        WHERE date >= '{date_cutoff}'
        ORDER BY date
        """
        
        trend_df = load_duckdb_data(duckdb_path, trend_query)
        
        if trend_df is not None and not trend_df.empty:
            tab1, tab2, tab3, tab4 = st.tabs(["📊 Impressions", "❤️ Engagements", "📝 Tweets", "📈 Eng. Rate"])
            
            with tab1:
                st.bar_chart(trend_df.set_index('date')['impressions'], use_container_width=True)
                st.caption("Daily Impressions")
            
            with tab2:
                # Stacked engagement chart
                eng_df = trend_df.set_index('date')[['likes', 'retweets', 'replies', 'quotes']]
                st.bar_chart(eng_df, use_container_width=True)
                st.caption("Daily Engagements by Type")
            
            with tab3:
                st.bar_chart(trend_df.set_index('date')['tweet_count'], use_container_width=True)
                st.caption("Daily Tweet Count")
            
            with tab4:
                st.line_chart(trend_df.set_index('date')['engagement_rate'], use_container_width=True)
                st.caption("Daily Engagement Rate (%)")
        else:
            st.info("No daily metrics data available for the selected period.")
    
    st.divider()
    
    # ========================================
    # SECTION 4: TOP PERFORMING TWEETS
    # ========================================
    st.subheader("🏆 Top Performing Tweets")
    
    if 'twitter_tweets' in twitter_tables:
        col1, col2 = st.columns(2)
        
        with col1:
            sort_by = st.selectbox(
                "Sort by",
                options=["impressions", "likes", "retweets", "replies", "total_engagement"],
                format_func=lambda x: {
                    "impressions": "👁️ Impressions",
                    "likes": "❤️ Likes",
                    "retweets": "🔄 Retweets",
                    "replies": "💬 Replies",
                    "total_engagement": "📊 Total Engagement"
                }.get(x, x),
                key="twitter_sort"
            )
        
        with col2:
            tweet_type_filter = st.selectbox(
                "Tweet Type",
                options=["All", "original", "reply", "quote"],
                format_func=lambda x: {
                    "All": "All Tweets",
                    "original": "Original Tweets",
                    "reply": "Replies",
                    "quote": "Quote Tweets"
                }.get(x, x),
                key="twitter_type"
            )
        
        # Build query
        type_filter = f"AND tweet_type = '{tweet_type_filter}'" if tweet_type_filter != "All" else ""
        
        if sort_by == "total_engagement":
            order_by = "(likes + retweets + replies + quotes + bookmarks)"
        else:
            order_by = sort_by
        
        top_tweets_query = f"""
        SELECT 
            tweet_id,
            text,
            tweet_type,
            created_date,
            impressions,
            likes,
            retweets,
            replies,
            quotes,
            bookmarks,
            (likes + retweets + replies + quotes + bookmarks) as total_engagement
        FROM twitter_tweets
        WHERE created_date >= '{date_cutoff}' {type_filter}
        ORDER BY {order_by} DESC
        LIMIT 10
        """
        
        top_df = load_duckdb_data(duckdb_path, top_tweets_query)
        
        if top_df is not None and not top_df.empty:
            for _, tweet in top_df.iterrows():
                with st.container():
                    # Tweet header
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        text = tweet['text']
                        if len(text) > 200:
                            text = text[:200] + "..."
                        st.markdown(f"**{tweet['created_date']}** • _{tweet['tweet_type']}_")
                        st.markdown(text)
                    
                    with col2:
                        st.caption(f"👁️ {int(tweet['impressions'] or 0):,}")
                    
                    # Metrics row
                    mcol1, mcol2, mcol3, mcol4, mcol5 = st.columns(5)
                    mcol1.caption(f"❤️ {int(tweet['likes']):,}")
                    mcol2.caption(f"🔄 {int(tweet['retweets']):,}")
                    mcol3.caption(f"💬 {int(tweet['replies']):,}")
                    mcol4.caption(f"📝 {int(tweet['quotes']):,}")
                    mcol5.caption(f"🔖 {int(tweet['bookmarks']):,}")
                    
                    st.divider()
        else:
            st.info("No tweets found for the selected filters.")
    
    st.divider()
    
    # ========================================
    # SECTION 5: TWEET TYPE BREAKDOWN
    # ========================================
    st.subheader("📊 Tweet Type Analysis")
    
    if 'twitter_tweets' in twitter_tables:
        type_query = f"""
        SELECT 
            tweet_type,
            COUNT(*) as count,
            SUM(impressions) as impressions,
            SUM(likes) as likes,
            SUM(retweets) as retweets,
            AVG(impressions) as avg_impressions,
            AVG(likes) as avg_likes
        FROM twitter_tweets
        WHERE created_date >= '{date_cutoff}'
        GROUP BY tweet_type
        ORDER BY count DESC
        """
        
        type_df = load_duckdb_data(duckdb_path, type_query)
        
        if type_df is not None and not type_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**📊 Tweet Count by Type**")
                st.bar_chart(type_df.set_index('tweet_type')['count'])
            
            with col2:
                st.markdown("**📈 Avg Engagement by Type**")
                type_df['avg_engagement'] = type_df['avg_likes'] + (type_df['impressions'] / type_df['count'].replace(0, 1) * 0.01)
                st.bar_chart(type_df.set_index('tweet_type')['avg_likes'])
    
    st.divider()
    
    # ========================================
    # SECTION 6: RAW DATA EXPLORER
    # ========================================
    with st.expander("🔍 Raw Data Explorer"):
        table_to_view = st.selectbox(
            "Select Table",
            options=twitter_tables,
            key="twitter_raw_table"
        )
        
        if table_to_view:
            raw_query = f"SELECT * FROM {table_to_view} ORDER BY 1 DESC LIMIT 100"
            raw_df = load_duckdb_data(duckdb_path, raw_query)
            
            if raw_df is not None:
                st.dataframe(raw_df, use_container_width=True, hide_index=True)
                st.caption(f"Showing up to 100 rows from {table_to_view}")


# ============================================
# ETL Control Panel
# ============================================
def render_etl_control_panel(duckdb_path: str):
    """
    Render the ETL Control Panel for manual data pulls.
    
    Provides buttons for:
    - Full Lifetime Pull (all historical data)
    - Daily Refresh (incremental update, last 3 days)
    - Individual source pulls
    - ETL status monitoring
    """
    
    st.header("🔧 ETL Control Panel")
    st.markdown("""
    Use this panel to manually trigger data extraction from all connected platforms.
    
    - **Lifetime Pull**: Extracts ALL historical data (replaces existing tables)
    - **Daily Refresh**: Extracts last 3 days (updates existing data without losing history)
    """)
    
    st.divider()
    
    # ========================================
    # SECTION 1: Quick Actions
    # ========================================
    st.subheader("⚡ Quick Actions")
    
    col1, col2 = st.columns(2)
    
    # Initialize session state for ETL status
    if 'etl_running' not in st.session_state:
        st.session_state.etl_running = False
    if 'etl_output' not in st.session_state:
        st.session_state.etl_output = ""
    if 'etl_status' not in st.session_state:
        st.session_state.etl_status = None
    
    with col1:
        st.markdown("### 📥 Full Lifetime Pull")
        st.caption("Extract ALL historical data from all platforms. This replaces existing data.")
        
        if st.button(
            "🚀 Run Lifetime ETL",
            disabled=st.session_state.etl_running,
            use_container_width=True,
            type="primary"
        ):
            st.session_state.etl_running = True
            st.session_state.etl_status = "running"
            
            with st.spinner("Running Lifetime ETL... This may take several minutes."):
                try:
                    result = subprocess.run(
                        [sys.executable, "scripts/run_etl_unified.py", 
                         "--source", "all", "--lifetime"],
                        capture_output=True,
                        text=True,
                        cwd=str(project_root),
                        timeout=1800  # 30 minute timeout
                    )
                    
                    st.session_state.etl_output = result.stdout + "\n" + result.stderr
                    
                    if result.returncode == 0:
                        st.session_state.etl_status = "success"
                        st.success("✅ Lifetime ETL completed successfully!")
                    else:
                        st.session_state.etl_status = "error"
                        st.error(f"❌ ETL failed with exit code {result.returncode}")
                        
                except subprocess.TimeoutExpired:
                    st.session_state.etl_status = "timeout"
                    st.error("⏱️ ETL timed out after 30 minutes")
                except Exception as e:
                    st.session_state.etl_status = "error"
                    st.error(f"❌ ETL failed: {e}")
                finally:
                    st.session_state.etl_running = False
                    # Clear cache to refresh data
                    st.cache_data.clear()
    
    with col2:
        st.markdown("### 🔄 Daily Refresh")
        st.caption("Extract last 3 days of data. Updates existing records without losing history.")
        
        if st.button(
            "🔄 Run Daily Refresh",
            disabled=st.session_state.etl_running,
            use_container_width=True,
            type="secondary"
        ):
            st.session_state.etl_running = True
            st.session_state.etl_status = "running"
            
            with st.spinner("Running Daily Refresh... This may take a few minutes."):
                try:
                    result = subprocess.run(
                        [sys.executable, "scripts/run_etl_unified.py", 
                         "--source", "all", "--lookback-days", "3"],
                        capture_output=True,
                        text=True,
                        cwd=str(project_root),
                        timeout=900  # 15 minute timeout
                    )
                    
                    st.session_state.etl_output = result.stdout + "\n" + result.stderr
                    
                    if result.returncode == 0:
                        st.session_state.etl_status = "success"
                        st.success("✅ Daily refresh completed successfully!")
                    else:
                        st.session_state.etl_status = "error"
                        st.error(f"❌ ETL failed with exit code {result.returncode}")
                        
                except subprocess.TimeoutExpired:
                    st.session_state.etl_status = "timeout"
                    st.error("⏱️ ETL timed out after 15 minutes")
                except Exception as e:
                    st.session_state.etl_status = "error"
                    st.error(f"❌ ETL failed: {e}")
                finally:
                    st.session_state.etl_running = False
                    # Clear cache to refresh data
                    st.cache_data.clear()
    
    st.divider()
    
    # ========================================
    # SECTION 2: Individual Source ETL
    # ========================================
    st.subheader("🎯 Individual Source ETL")
    st.caption("Run ETL for specific data sources")
    
    source_col1, source_col2, source_col3, source_col4, source_col5 = st.columns(5)
    
    sources = [
        ("ga4", "📊 GA4", source_col1),
        ("gsc", "🔍 GSC", source_col2),
        ("gads", "💰 Google Ads", source_col3),
        ("meta", "📘 Meta", source_col4),
        ("twitter", "🐦 Twitter", source_col5),
    ]
    
    # Mode selection
    mode = st.radio(
        "ETL Mode",
        options=["Daily (last 3 days)", "Lifetime (all data)"],
        horizontal=True,
        key="etl_mode_radio"
    )
    
    is_lifetime = "Lifetime" in mode
    
    for source_id, source_label, col in sources:
        with col:
            if st.button(
                source_label,
                disabled=st.session_state.etl_running,
                use_container_width=True,
                key=f"etl_{source_id}"
            ):
                st.session_state.etl_running = True
                
                with st.spinner(f"Running {source_label} ETL..."):
                    try:
                        cmd = [sys.executable, "scripts/run_etl_unified.py", 
                               "--source", source_id]
                        if is_lifetime:
                            cmd.append("--lifetime")
                        else:
                            cmd.extend(["--lookback-days", "3"])
                        
                        result = subprocess.run(
                            cmd,
                            capture_output=True,
                            text=True,
                            cwd=str(project_root),
                            timeout=600  # 10 minute timeout
                        )
                        
                        st.session_state.etl_output = result.stdout + "\n" + result.stderr
                        
                        if result.returncode == 0:
                            st.success(f"✅ {source_label} ETL completed!")
                        else:
                            st.error(f"❌ {source_label} ETL failed")
                            
                    except subprocess.TimeoutExpired:
                        st.error(f"⏱️ {source_label} ETL timed out")
                    except Exception as e:
                        st.error(f"❌ {source_label} ETL failed: {e}")
                    finally:
                        st.session_state.etl_running = False
                        st.cache_data.clear()
    
    st.divider()
    
    # ========================================
    # SECTION 3: ETL Output Log
    # ========================================
    with st.expander("📋 ETL Output Log", expanded=st.session_state.etl_status == "error"):
        if st.session_state.etl_output:
            st.code(st.session_state.etl_output, language="text")
        else:
            st.info("No ETL output yet. Run an ETL job to see output.")
    
    st.divider()
    
    # ========================================
    # SECTION 4: Data Status Summary
    # ========================================
    st.subheader("📊 Current Data Status")
    
    table_info = get_table_info(duckdb_path)
    
    if table_info:
        # Group by source
        source_groups = {
            'GA4': ('ga4_', '📊'),
            'GSC': ('gsc_', '🔍'),
            'Google Ads': ('gads_', '💰'),
            'Meta Ads': ('meta_', '📘'),
            'Twitter': ('twitter_', '🐦'),
        }
        
        data_rows = []
        for source_name, (prefix, icon) in source_groups.items():
            source_tables = {k: v for k, v in table_info.items() if k.startswith(prefix)}
            total_rows = sum(source_tables.values())
            table_count = len(source_tables)
            
            # Get date range if possible
            date_range = "N/A"
            if total_rows > 0:
                # Try to get date range from a table that has a date column
                for table in source_tables.keys():
                    try:
                        conn = duckdb.connect(duckdb_path, read_only=True)
                        result = conn.execute(f"SELECT MIN(date), MAX(date) FROM {table}").fetchone()
                        conn.close()
                        if result and result[0]:
                            date_range = f"{result[0]} to {result[1]}"
                            break
                    except:
                        continue
            
            data_rows.append({
                'Source': f"{icon} {source_name}",
                'Tables': table_count,
                'Total Rows': f"{total_rows:,}",
                'Date Range': date_range,
                'Status': '✅ Has Data' if total_rows > 0 else '⚠️ No Data'
            })
        
        status_df = pd.DataFrame(data_rows)
        st.dataframe(status_df, use_container_width=True, hide_index=True)
        
        # Detailed table breakdown
        with st.expander("📋 Detailed Table Breakdown"):
            for table_name, row_count in sorted(table_info.items()):
                st.text(f"  {table_name}: {row_count:,} rows")
    else:
        st.warning("No data tables found in the database. Run a Lifetime ETL to populate data.")
    
    st.divider()
    
    # ========================================
    # SECTION 5: CLI Reference
    # ========================================
    with st.expander("💻 Command Line Reference"):
        st.markdown("""
        You can also run ETL from the command line:
        
        **Lifetime Pull (all data):**
        ```bash
        python scripts/run_etl_unified.py --source all --lifetime
        ```
        
        **Daily Refresh (last 3 days):**
        ```bash
        python scripts/run_etl_unified.py --source all --lookback-days 3
        ```
        
        **Specific Source:**
        ```bash
        python scripts/run_etl_unified.py --source ga4 --lookback-days 30
        python scripts/run_etl_unified.py --source gads --lifetime
        python scripts/run_etl_unified.py --source meta --start-date 2024-01-01
        ```
        
        **Test Connections:**
        ```bash
        python scripts/test_connections_unified.py --all
        python scripts/test_connections_unified.py --source gads
        ```
        """)


# ============================================
# Settings Page
# ============================================
def render_settings_page(ga4_config, gsc_config, gads_config, duckdb_path: str):
    """Render the settings and status page."""
    
    st.header("⚙️ Settings & Status")
    
    # Configuration Status
    st.subheader("Configuration Status")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**GA4 Configuration**")
        if ga4_config:
            st.success("✅ GA4 Configured")
            st.text(f"Property ID: {ga4_config.ga4_property_id}")
        else:
            st.error("❌ GA4 Not Configured")
    
    with col2:
        st.markdown("**GSC Configuration**")
        if gsc_config:
            st.success("✅ GSC Configured")
            st.text(f"Site URL: {gsc_config.site_url}")
        else:
            st.warning("⚠️ GSC Not Configured")
    
    with col3:
        st.markdown("**Google Ads Configuration**")
        if gads_config:
            st.success("✅ Google Ads Configured")
            st.text(f"Customer ID: {gads_config.customer_id}")
        else:
            st.warning("⚠️ Google Ads Not Configured")
    
    st.divider()
    
    # Database Status
    st.subheader("Database Status")
    
    if Path(duckdb_path).exists():
        st.success(f"✅ Database exists: {duckdb_path}")
        
        table_info = get_table_info(duckdb_path)
        
        if table_info:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown("**GA4 Tables**")
                ga4_tables = {k: v for k, v in table_info.items() if k.startswith('ga4_')}
                for table, count in ga4_tables.items():
                    st.text(f"  {table}: {count:,} rows")
            
            with col2:
                st.markdown("**GSC Tables**")
                gsc_tables = {k: v for k, v in table_info.items() if k.startswith('gsc_')}
                for table, count in gsc_tables.items():
                    st.text(f"  {table}: {count:,} rows")
            
            with col3:
                st.markdown("**Google Ads Tables**")
                gads_tables = {k: v for k, v in table_info.items() if k.startswith('gads_')}
                if gads_tables:
                    for table, count in gads_tables.items():
                        st.text(f"  {table}: {count:,} rows")
                else:
                    st.text("  No Google Ads tables yet")
    else:
        st.warning(f"⚠️ Database not found: {duckdb_path}")
    
    st.divider()
    
    # Connection Tests
    st.subheader("Connection Tests")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("Test GA4", use_container_width=True):
            if ga4_config:
                with st.spinner("Testing GA4..."):
                    try:
                        from google.analytics.data_v1beta import BetaAnalyticsDataClient
                        from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
                        client = BetaAnalyticsDataClient()
                        request = RunReportRequest(
                            property=f"properties/{ga4_config.ga4_property_id}",
                            dimensions=[Dimension(name="date")],
                            metrics=[Metric(name="sessions")],
                            date_ranges=[DateRange(start_date="yesterday", end_date="yesterday")],
                        )
                        client.run_report(request)
                        st.success("✅ GA4 Connected!")
                    except Exception as e:
                        st.error(f"❌ {e}")
            else:
                st.warning("GA4 not configured")
    
    with col2:
        if st.button("Test GSC", use_container_width=True):
            if gsc_config:
                with st.spinner("Testing GSC..."):
                    try:
                        from etl.gsc_extractor import GSCExtractor
                        extractor = GSCExtractor(str(gsc_config.credentials_path), gsc_config.site_url)
                        success, msg = extractor.test_connection()
                        if success:
                            st.success("✅ GSC Connected!")
                        else:
                            st.error(f"❌ {msg}")
                    except Exception as e:
                        st.error(f"❌ {e}")
            else:
                st.warning("GSC not configured")
    
    with col3:
        if st.button("Test Google Ads", use_container_width=True):
            if gads_config:
                with st.spinner("Testing Google Ads..."):
                    try:
                        from etl.gads_config import get_gads_client
                        from etl.gads_extractor import GAdsExtractor
                        client = get_gads_client()
                        extractor = GAdsExtractor(client, gads_config.customer_id, gads_config.login_customer_id)
                        success, msg = extractor.test_connection()
                        if success:
                            st.success("✅ Google Ads Connected!")
                        else:
                            st.error(f"❌ {msg}")
                    except Exception as e:
                        st.error(f"❌ {e}")
            else:
                st.warning("Google Ads not configured")
    
    st.divider()
    
    # ETL Commands Reference
    st.subheader("ETL Commands Reference")
    
    st.markdown("""
    **GA4 Data Extraction:**
    ```bash
    python scripts/run_etl.py                              # Last 30 days
    python scripts/run_etl_comprehensive.py --lifetime     # All metrics, lifetime
    ```
    
    **GSC Data Extraction:**
    ```bash
    python scripts/test_gsc_connection.py                  # Test connection
    python scripts/run_etl_gsc.py --lifetime               # All lifetime data
    ```
    
    **Google Ads Data Extraction:**
    ```bash
    python scripts/test_gads_connection.py                 # Test connection
    python scripts/run_etl_gads.py --lifetime              # All lifetime data
    python scripts/run_etl_gads.py --lookback-days 90      # Last 90 days
    ```
    """)


# ============================================
# Main Application
# ============================================
def main():
    """Main application entry point."""
    
    # Load configurations
    ga4_config, ga4_error = load_ga4_configuration()
    gsc_config, gsc_error = load_gsc_configuration()
    gads_config, gads_error = load_gads_configuration()
    meta_config, meta_error = load_meta_configuration()
    
    # Determine DuckDB path
    if ga4_config:
        duckdb_path = str(ga4_config.duckdb_path)
    elif gsc_config:
        duckdb_path = str(gsc_config.duckdb_path)
    elif gads_config:
        duckdb_path = str(gads_config.duckdb_path)
    elif meta_config:
        duckdb_path = str(meta_config.duckdb_path)
    else:
        duckdb_path = str(project_root / "data" / "warehouse.duckdb")
    
    # Sidebar
    with st.sidebar:
        st.title("🎯 rs_analytics")
        st.caption("Analytics & Marketing Dashboard")
        
        st.divider()
        
        # Navigation
        page = st.radio(
            "Navigation",
            options=[
                "📊 GA4 Analytics", 
                "🔍 Search Console (SEO)", 
                "💰 Google Ads (PPC)", 
                "📘 Meta Ads",
                "🐦 Twitter/X",
                "🔧 ETL Control",
                "⚙️ Settings"
            ],
            index=0
        )
        
        st.divider()
        
        # Quick Status
        st.subheader("Data Status")
        
        table_info = get_table_info(duckdb_path)
        ga4_rows = sum(v for k, v in table_info.items() if k.startswith('ga4_'))
        gsc_rows = sum(v for k, v in table_info.items() if k.startswith('gsc_'))
        gads_rows = sum(v for k, v in table_info.items() if k.startswith('gads_'))
        meta_rows = sum(v for k, v in table_info.items() if k.startswith('meta_'))
        twitter_rows = sum(v for k, v in table_info.items() if k.startswith('twitter_'))
        
        if ga4_rows > 0:
            st.success(f"GA4: {ga4_rows:,} rows")
        else:
            st.warning("GA4: No data")
        
        if gsc_rows > 0:
            st.success(f"GSC: {gsc_rows:,} rows")
        else:
            st.warning("GSC: No data")
        
        if gads_rows > 0:
            st.success(f"Google Ads: {gads_rows:,} rows")
        else:
            st.warning("Google Ads: No data")
        
        if meta_rows > 0:
            st.success(f"Meta Ads: {meta_rows:,} rows")
        else:
            st.warning("Meta Ads: No data")
        
        if twitter_rows > 0:
            st.success(f"Twitter: {twitter_rows:,} rows")
        else:
            st.warning("Twitter: No data")
        
        st.divider()
        
        # Refresh button
        st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    # Main Content
    if page == "📊 GA4 Analytics":
        if ga4_config:
            render_ga4_dashboard(ga4_config, duckdb_path)
        else:
            st.error("GA4 Configuration Error")
            with st.expander("Error Details"):
                st.code(ga4_error)
    
    elif page == "🔍 Search Console (SEO)":
        render_gsc_dashboard(gsc_config, duckdb_path)
    
    elif page == "💰 Google Ads (PPC)":
        render_gads_dashboard(gads_config, duckdb_path)
    
    elif page == "📘 Meta Ads":
        render_meta_dashboard(meta_config, duckdb_path)
    
    elif page == "🐦 Twitter/X":
        twitter_config, twitter_error = load_twitter_configuration()
        render_twitter_dashboard(twitter_config, duckdb_path)
    
    elif page == "🔧 ETL Control":
        render_etl_control_panel(duckdb_path)
    
    elif page == "⚙️ Settings":
        render_settings_page(ga4_config, gsc_config, gads_config, duckdb_path)


if __name__ == "__main__":
    main()
