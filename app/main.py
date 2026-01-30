"""
rs_analytics Streamlit Dashboard

Main entry point for the Streamlit analytics dashboard.
This app provides:
- Real-time GA4 data visualization
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
from datetime import datetime, timedelta
from pathlib import Path

import streamlit as st

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
# Configuration Validation (Fail Fast)
# ============================================
@st.cache_resource
def load_configuration():
    """
    Load and validate configuration at startup.
    
    Uses Streamlit's cache_resource to ensure this only runs once
    per app session. If configuration is invalid, returns the error
    message instead of the config object.
    
    Returns:
        Tuple of (config, error_message)
        - If valid: (Config object, None)
        - If invalid: (None, error message string)
    """
    try:
        from etl.config import get_config, ConfigurationError
        config = get_config()
        return config, None
    except ConfigurationError as e:
        return None, str(e)
    except Exception as e:
        return None, f"Unexpected error loading configuration: {e}"


def check_configuration():
    """
    Check configuration and display error if invalid.
    
    This is called at the start of every page render.
    If configuration is invalid, it displays a clear error
    and stops execution (fail-fast behavior).
    
    Returns:
        Config object if valid, None if displayed error
    """
    config, error = load_configuration()
    
    if error:
        st.error("Configuration Error - Application cannot start")
        
        # Display the full error message in an expandable section
        with st.expander("Error Details", expanded=True):
            st.code(error, language="text")
        
        st.info("""
        **How to fix:**
        1. Copy `.env.example` to `.env`
        2. Fill in all required values
        3. Ensure paths are absolute
        4. Restart the Streamlit app
        
        See README.md for detailed setup instructions.
        """)
        
        # Stop execution - fail fast
        st.stop()
    
    return config


# ============================================
# GA4 Connection Testing
# ============================================
def test_ga4_connection_ui(config):
    """
    Test GA4 connection and display results in UI.
    
    Args:
        config: Validated configuration object
    """
    with st.spinner("Testing GA4 connection..."):
        try:
            from google.analytics.data_v1beta import BetaAnalyticsDataClient
            from google.analytics.data_v1beta.types import (
                DateRange,
                Dimension,
                Metric,
                RunReportRequest,
            )
            
            # Create client using GOOGLE_APPLICATION_CREDENTIALS
            client = BetaAnalyticsDataClient()
            
            # Request yesterday's data
            request = RunReportRequest(
                property=f"properties/{config.ga4_property_id}",
                dimensions=[Dimension(name="date")],
                metrics=[
                    Metric(name="sessions"),
                    Metric(name="activeUsers"),
                    Metric(name="screenPageViews"),
                ],
                date_ranges=[DateRange(start_date="yesterday", end_date="yesterday")],
            )
            
            response = client.run_report(request)
            
            # Extract metrics
            if response.rows:
                row = response.rows[0]
                sessions = int(row.metric_values[0].value)
                active_users = int(row.metric_values[1].value)
                page_views = int(row.metric_values[2].value)
                
                st.success("GA4 Connection Successful!")
                
                # Display yesterday's metrics in columns
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Sessions (Yesterday)", sessions)
                with col2:
                    st.metric("Active Users", active_users)
                with col3:
                    st.metric("Page Views", page_views)
            else:
                st.success("GA4 Connection Successful!")
                st.info("No data returned for yesterday (this may be normal for new properties)")
                
        except Exception as e:
            error_msg = str(e)
            st.error("GA4 Connection Failed")
            
            # Provide specific guidance based on error type
            # Note: We don't expose credential details, only actionable fixes
            if "403" in error_msg or "permission" in error_msg.lower():
                st.warning("""
                **Permission Denied**
                
                The service account does not have access to this GA4 property.
                
                **How to fix:**
                1. Go to Google Analytics → Admin → Property Access Management
                2. Click '+' to add a new user
                3. Enter the service account email (found in your JSON credentials file)
                4. Select 'Viewer' role
                5. Wait 2-5 minutes and try again
                """)
            elif "API" in error_msg and "not been used" in error_msg.lower():
                st.warning("""
                **API Not Enabled**
                
                The Google Analytics Data API is not enabled in your GCP project.
                
                **How to fix:**
                1. Go to: https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com
                2. Select your project
                3. Click 'Enable'
                4. Wait 2-5 minutes and try again
                """)
            elif "property" in error_msg.lower() and "not found" in error_msg.lower():
                st.warning(f"""
                **Property Not Found**
                
                GA4 Property ID `{config.ga4_property_id}` was not found.
                
                **How to fix:**
                1. Verify your GA4_PROPERTY_ID in .env
                2. Find correct ID: GA4 → Admin → Property Settings
                3. Update .env and restart the app
                """)
            else:
                with st.expander("Error Details"):
                    # Only show error message, never credentials
                    st.code(error_msg, language="text")


# ============================================
# Data Loading
# ============================================
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_duckdb_data(duckdb_path: str, query: str):
    """
    Load data from DuckDB.
    
    Args:
        duckdb_path: Path to DuckDB database
        query: SQL query to execute
        
    Returns:
        Pandas DataFrame with results, or None if error
    """
    try:
        import duckdb
        import pandas as pd
        
        conn = duckdb.connect(duckdb_path, read_only=True)
        df = conn.execute(query).fetchdf()
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None


def check_duckdb_status(config):
    """
    Check if DuckDB database exists and has data.
    
    Args:
        config: Validated configuration object
        
    Returns:
        Tuple of (exists: bool, has_data: bool, row_count: int)
    """
    import duckdb
    
    db_path = str(config.duckdb_path)
    
    if not Path(db_path).exists():
        return False, False, 0
    
    try:
        conn = duckdb.connect(db_path, read_only=True)
        
        # Check if table exists and get row count
        result = conn.execute("""
            SELECT COUNT(*) as cnt 
            FROM information_schema.tables 
            WHERE table_name = 'ga4_sessions'
        """).fetchone()
        
        if result[0] == 0:
            conn.close()
            return True, False, 0
        
        row_count = conn.execute("SELECT COUNT(*) FROM ga4_sessions").fetchone()[0]
        conn.close()
        
        return True, row_count > 0, row_count
        
    except Exception:
        return True, False, 0


# ============================================
# Main Application
# ============================================
def main():
    """Main application entry point."""
    
    # Validate configuration first (fail-fast)
    config = check_configuration()
    
    # ========================================
    # Sidebar
    # ========================================
    with st.sidebar:
        st.title("rs_analytics")
        st.caption("Analytics Dashboard")
        
        st.divider()
        
        # Configuration Status (no sensitive details)
        st.subheader("Configuration")
        st.text(f"GA4 Property: {config.ga4_property_id}")
        st.text(f"Lookback Days: {config.lookback_days}")
        st.text(f"BQ Mirror: {'Enabled' if config.enable_bq_mirror else 'Disabled'}")
        
        st.divider()
        
        # Test Connection Button
        st.subheader("Connection Test")
        if st.button("Test GA4 Connection", use_container_width=True):
            st.session_state['test_connection'] = True
        
        st.divider()
        
        # Data Status
        st.subheader("Data Status")
        db_exists, has_data, row_count = check_duckdb_status(config)
        
        if db_exists:
            st.success("DuckDB: Connected")
            if has_data:
                st.text(f"Rows: {row_count:,}")
            else:
                st.warning("No data yet - run ETL first")
        else:
            st.warning("DuckDB: Not created yet")
            st.caption("Run scripts/run_etl.py to populate data")
        
        st.divider()
        
        # Footer
        st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")
        if st.button("Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    # ========================================
    # Main Content
    # ========================================
    st.title("Analytics Dashboard")
    
    # Show connection test results if requested
    if st.session_state.get('test_connection'):
        with st.container():
            st.subheader("GA4 Connection Test")
            test_ga4_connection_ui(config)
            if st.button("Close"):
                st.session_state['test_connection'] = False
                st.rerun()
        st.divider()
    
    # Check if we have data to display
    db_exists, has_data, row_count = check_duckdb_status(config)
    
    if not has_data:
        st.info("""
        **No data available yet.**
        
        Run the ETL pipeline to populate the database:
        ```bash
        python scripts/run_etl.py
        ```
        
        Or first test your credentials:
        ```bash
        python scripts/test_ga4_connection.py
        ```
        """)
        return
    
    # ========================================
    # Dashboard Metrics
    # ========================================
    
    # Date range selector
    col1, col2 = st.columns([2, 1])
    with col1:
        date_range = st.selectbox(
            "Date Range",
            options=["Last 7 days", "Last 14 days", "Last 30 days", "All time"],
            index=0
        )
    
    # Calculate date filter
    if date_range == "Last 7 days":
        date_filter = "date >= CURRENT_DATE - INTERVAL '7 days'"
    elif date_range == "Last 14 days":
        date_filter = "date >= CURRENT_DATE - INTERVAL '14 days'"
    elif date_range == "Last 30 days":
        date_filter = "date >= CURRENT_DATE - INTERVAL '30 days'"
    else:
        date_filter = "1=1"
    
    # Load summary metrics
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
    
    summary_df = load_duckdb_data(str(config.duckdb_path), summary_query)
    
    if summary_df is not None and not summary_df.empty:
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Sessions",
                f"{int(summary_df['total_sessions'].iloc[0] or 0):,}"
            )
        
        with col2:
            st.metric(
                "Active Users",
                f"{int(summary_df['total_users'].iloc[0] or 0):,}"
            )
        
        with col3:
            st.metric(
                "Page Views",
                f"{int(summary_df['total_page_views'].iloc[0] or 0):,}"
            )
        
        with col4:
            engagement = summary_df['avg_engagement_rate'].iloc[0] or 0
            st.metric(
                "Avg Engagement",
                f"{engagement:.1%}"
            )
    
    st.divider()
    
    # ========================================
    # Sessions Over Time Chart
    # ========================================
    st.subheader("Sessions Over Time")
    
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
    
    time_df = load_duckdb_data(str(config.duckdb_path), time_query)
    
    if time_df is not None and not time_df.empty:
        st.line_chart(time_df.set_index('date')[['sessions', 'active_users']])
    
    # ========================================
    # Traffic Sources
    # ========================================
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Top Sources")
        
        source_query = f"""
        SELECT 
            session_source as source,
            SUM(sessions) as sessions
        FROM ga4_sessions
        WHERE {date_filter} AND session_source != '(direct)'
        GROUP BY session_source
        ORDER BY sessions DESC
        LIMIT 10
        """
        
        source_df = load_duckdb_data(str(config.duckdb_path), source_query)
        
        if source_df is not None and not source_df.empty:
            st.bar_chart(source_df.set_index('source'))
    
    with col2:
        st.subheader("Device Categories")
        
        device_query = f"""
        SELECT 
            device_category,
            SUM(sessions) as sessions
        FROM ga4_sessions
        WHERE {date_filter}
        GROUP BY device_category
        ORDER BY sessions DESC
        """
        
        device_df = load_duckdb_data(str(config.duckdb_path), device_query)
        
        if device_df is not None and not device_df.empty:
            st.bar_chart(device_df.set_index('device_category'))
    
    # ========================================
    # Raw Data (Expandable)
    # ========================================
    with st.expander("View Raw Data"):
        raw_query = f"""
        SELECT *
        FROM ga4_sessions
        WHERE {date_filter}
        ORDER BY date DESC, sessions DESC
        LIMIT 1000
        """
        
        raw_df = load_duckdb_data(str(config.duckdb_path), raw_query)
        
        if raw_df is not None:
            st.dataframe(raw_df, use_container_width=True)


if __name__ == "__main__":
    main()
