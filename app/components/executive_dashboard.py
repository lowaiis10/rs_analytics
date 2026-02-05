"""
Executive Dashboard Component for RS Analytics

This module provides a unified executive dashboard view combining metrics
from all data sources (Google Ads, Meta Ads, GA4, GSC) into actionable KPIs.

Layout (7 rows):
- Row 0: Header (date selector, data freshness)
- Row 1: Core Health KPIs (6 tiles)
- Row 2: Target Tracking (RAG bars)
- Row 3: Channel Contribution (table)
- Row 4: Trend Reality Check (chart)
- Row 5: What Changed (narrative cards)
- Row 6: Risk Signals (alerts)
- Row 7: Data Trust (status strip)
"""

import os
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any, List, Tuple

import streamlit as st
import pandas as pd
import duckdb
import numpy as np


# ============================================
# Safe Data Conversion Utilities
# ============================================

def safe_int(value, default: int = 0) -> int:
    """
    Safely convert a value to integer, handling NaN, None, and invalid values.
    
    Args:
        value: Value to convert (can be int, float, str, None, NaN)
        default: Default value if conversion fails
    
    Returns:
        Integer value or default
    """
    if value is None:
        return default
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        if pd.isna(value) or np.isnan(value):
            return default
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return default
    return default


def safe_float(value, default: float = 0.0) -> float:
    """
    Safely convert a value to float, handling NaN, None, and invalid values.
    
    Args:
        value: Value to convert
        default: Default value if conversion fails
    
    Returns:
        Float value or default
    """
    if value is None:
        return default
    if isinstance(value, (int, float, np.integer, np.floating)):
        if pd.isna(value):
            return default
        try:
            if np.isnan(float(value)):
                return default
        except (TypeError, ValueError):
            pass
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    return default


# ============================================
# Configuration
# ============================================

# Default targets (can be overridden via env vars)
DEFAULT_TARGETS = {
    'conversions': int(os.getenv('EXEC_TARGET_CONVERSIONS', 1000)),
    'cpa': float(os.getenv('EXEC_TARGET_CPA', 5.00)),
    'budget': float(os.getenv('EXEC_TARGET_BUDGET', 10000)),
}


# ============================================
# Data Loading Helpers
# ============================================

def load_data(duckdb_path: str, query: str) -> Optional[pd.DataFrame]:
    """Load data from DuckDB."""
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        df = conn.execute(query).fetchdf()
        conn.close()
        return df
    except Exception as e:
        st.error(f"Query error: {e}")
        return None


def get_date_range(days: int, comparison_type: str = "Previous Period") -> Tuple[str, str, str, str]:
    """
    Get current and previous period date ranges based on comparison type.
    
    Args:
        days: Number of days for the current period
        comparison_type: Type of comparison - "Previous Period", "WoW", or "MoM"
    
    Returns:
        (start_date, end_date, prev_start_date, prev_end_date)
    
    Comparison Types:
        - Previous Period: Compare current N days vs the N days immediately before
        - WoW (Week over Week): Compare current period vs same period one week ago
        - MoM (Month over Month): Compare current period vs same period one month ago
    """
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)
    
    if comparison_type == "WoW":
        # Week over Week: Compare to 7 days earlier
        # E.g., if current is Jan 8-14, compare to Jan 1-7
        prev_end_date = end_date - timedelta(days=7)
        prev_start_date = start_date - timedelta(days=7)
    elif comparison_type == "MoM":
        # Month over Month: Compare to ~30 days earlier (one month)
        # E.g., if current is Feb 1-7, compare to Jan 1-7
        prev_end_date = end_date - timedelta(days=30)
        prev_start_date = start_date - timedelta(days=30)
    else:
        # Previous Period (default): Compare to the period immediately before
        # E.g., if current is Jan 15-21, compare to Jan 8-14
        prev_end_date = start_date - timedelta(days=1)
        prev_start_date = prev_end_date - timedelta(days=days)
    
    return (
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d'),
        prev_start_date.strftime('%Y-%m-%d'),
        prev_end_date.strftime('%Y-%m-%d')
    )


# ============================================
# Metric Aggregation Functions
# ============================================

def get_paid_metrics(duckdb_path: str, start_date: str, end_date: str) -> Dict[str, Any]:
    """Get aggregated paid advertising metrics."""
    query = f"""
    SELECT 
        SUM(spend) as spend,
        SUM(clicks) as clicks,
        SUM(impressions) as impressions,
        SUM(COALESCE(conversions, 0) + COALESCE(app_installs, 0)) as conversions,
        SUM(COALESCE(conversion_value, 0)) as revenue
    FROM fact_paid_daily
    WHERE date_day >= '{start_date}' AND date_day <= '{end_date}'
    """
    df = load_data(duckdb_path, query)
    if df is not None and not df.empty:
        return df.iloc[0].to_dict()
    return {'spend': 0, 'clicks': 0, 'impressions': 0, 'conversions': 0, 'revenue': 0}


def get_web_metrics(duckdb_path: str, start_date: str, end_date: str) -> Dict[str, Any]:
    """Get aggregated web analytics metrics."""
    query = f"""
    SELECT 
        SUM(sessions) as sessions,
        SUM(users) as users,
        SUM(new_users) as new_users,
        AVG(bounce_rate) as bounce_rate
    FROM fact_web_daily
    WHERE date_day >= '{start_date}' AND date_day <= '{end_date}'
    """
    df = load_data(duckdb_path, query)
    if df is not None and not df.empty:
        return df.iloc[0].to_dict()
    return {'sessions': 0, 'users': 0, 'new_users': 0, 'bounce_rate': 0}


def get_organic_metrics(duckdb_path: str, start_date: str, end_date: str) -> Dict[str, Any]:
    """Get aggregated organic search metrics."""
    query = f"""
    SELECT 
        SUM(clicks) as clicks,
        SUM(impressions) as impressions,
        AVG(ctr) as ctr,
        AVG(position) as position
    FROM fact_organic_daily
    WHERE date_day >= '{start_date}' AND date_day <= '{end_date}'
    """
    df = load_data(duckdb_path, query)
    if df is not None and not df.empty:
        return df.iloc[0].to_dict()
    return {'clicks': 0, 'impressions': 0, 'ctr': 0, 'position': 0}


def get_channel_breakdown(duckdb_path: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Get performance breakdown by channel/platform."""
    query = f"""
    SELECT 
        platform as channel,
        SUM(spend) as spend,
        SUM(clicks) as clicks,
        SUM(impressions) as impressions,
        SUM(COALESCE(conversions, 0) + COALESCE(app_installs, 0)) as conversions,
        CASE WHEN SUM(COALESCE(conversions, 0) + COALESCE(app_installs, 0)) > 0 
             THEN SUM(spend) / SUM(COALESCE(conversions, 0) + COALESCE(app_installs, 0))
             ELSE NULL END as cpa
    FROM fact_paid_daily
    WHERE date_day >= '{start_date}' AND date_day <= '{end_date}'
    GROUP BY platform
    ORDER BY spend DESC
    """
    df = load_data(duckdb_path, query)
    return df if df is not None else pd.DataFrame()


def get_trend_data(duckdb_path: str, days: int = 30) -> pd.DataFrame:
    """Get daily trend data for the specified period."""
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)
    
    query = f"""
    WITH paid AS (
        SELECT 
            date_day,
            SUM(spend) as paid_spend,
            SUM(COALESCE(conversions, 0) + COALESCE(app_installs, 0)) as conversions
        FROM fact_paid_daily
        WHERE date_day >= '{start_date}'
        GROUP BY date_day
    ),
    web AS (
        SELECT 
            date_day,
            SUM(sessions) as sessions
        FROM fact_web_daily
        WHERE date_day >= '{start_date}'
        GROUP BY date_day
    ),
    organic AS (
        SELECT 
            date_day,
            SUM(clicks) as organic_clicks
        FROM fact_organic_daily
        WHERE date_day >= '{start_date}'
        GROUP BY date_day
    )
    SELECT 
        COALESCE(p.date_day, w.date_day, o.date_day) as date_day,
        COALESCE(p.paid_spend, 0) as paid_spend,
        COALESCE(w.sessions, 0) as sessions,
        COALESCE(p.conversions, 0) as conversions,
        COALESCE(o.organic_clicks, 0) as organic_clicks
    FROM paid p
    FULL OUTER JOIN web w ON p.date_day = w.date_day
    FULL OUTER JOIN organic o ON COALESCE(p.date_day, w.date_day) = o.date_day
    ORDER BY date_day
    """
    df = load_data(duckdb_path, query)
    return df if df is not None else pd.DataFrame()


def get_sparkline_data(duckdb_path: str, metric: str, days: int = 7) -> List[float]:
    """Get last N days of data for sparkline visualization."""
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)
    
    metric_queries = {
        'spend': f"""
            SELECT date_day, SUM(spend) as value
            FROM fact_paid_daily
            WHERE date_day >= '{start_date}'
            GROUP BY date_day
            ORDER BY date_day
        """,
        'sessions': f"""
            SELECT date_day, SUM(sessions) as value
            FROM fact_web_daily
            WHERE date_day >= '{start_date}'
            GROUP BY date_day
            ORDER BY date_day
        """,
        'conversions': f"""
            SELECT date_day, SUM(COALESCE(conversions, 0) + COALESCE(app_installs, 0)) as value
            FROM fact_paid_daily
            WHERE date_day >= '{start_date}'
            GROUP BY date_day
            ORDER BY date_day
        """,
        'organic_clicks': f"""
            SELECT date_day, SUM(clicks) as value
            FROM fact_organic_daily
            WHERE date_day >= '{start_date}'
            GROUP BY date_day
            ORDER BY date_day
        """
    }
    
    if metric not in metric_queries:
        return []
    
    df = load_data(duckdb_path, metric_queries[metric])
    if df is not None and not df.empty:
        return df['value'].tolist()
    return []


def get_data_freshness(duckdb_path: str) -> Dict[str, Dict[str, Any]]:
    """Get last data update timestamp for each source."""
    sources = {
        'GA4': 'ga4_sessions',
        'GSC': 'gsc_daily_totals',
        'Google Ads': 'gads_campaigns',
        'Meta Ads': 'meta_daily_account'
    }
    
    freshness = {}
    for name, table in sources.items():
        try:
            # Check if table has extracted_at column
            query = f"""
                SELECT MAX(date) as last_date
                FROM {table}
            """
            df = load_data(duckdb_path, query)
            if df is not None and not df.empty and df.iloc[0]['last_date']:
                last_date = df.iloc[0]['last_date']
                # Handle different date formats
                if isinstance(last_date, str):
                    if len(last_date) == 8:  # YYYYMMDD
                        last_date = datetime.strptime(last_date, '%Y%m%d').date()
                    else:  # YYYY-MM-DD
                        last_date = datetime.strptime(last_date[:10], '%Y-%m-%d').date()
                
                days_ago = (datetime.now().date() - last_date).days
                freshness[name] = {
                    'last_date': last_date,
                    'days_ago': days_ago,
                    'status': 'ok' if days_ago <= 2 else 'warning' if days_ago <= 5 else 'error'
                }
            else:
                freshness[name] = {'last_date': None, 'days_ago': None, 'status': 'no_data'}
        except Exception:
            freshness[name] = {'last_date': None, 'days_ago': None, 'status': 'error'}
    
    return freshness


def calculate_delta(current: float, previous: float) -> Optional[float]:
    """Calculate percentage change between periods."""
    if previous and previous > 0:
        return ((current - previous) / previous) * 100
    return None


def generate_insights(
    current_paid: Dict, 
    prev_paid: Dict,
    current_organic: Dict,
    prev_organic: Dict,
    channel_df: pd.DataFrame
) -> List[Dict[str, str]]:
    """Generate auto-generated insights based on data changes."""
    insights = []
    
    # Check spend change
    if current_paid.get('spend') and prev_paid.get('spend'):
        spend_delta = calculate_delta(current_paid['spend'], prev_paid['spend'])
        if spend_delta and abs(spend_delta) > 10:
            direction = "increased" if spend_delta > 0 else "decreased"
            insights.append({
                'type': 'spend',
                'icon': '💰',
                'title': f"Paid Spend {direction} {abs(spend_delta):.1f}%",
                'detail': f"From ${prev_paid['spend']:,.0f} to ${current_paid['spend']:,.0f}",
                'action': "Review budget allocation" if spend_delta > 20 else "Monitor closely"
            })
    
    # Check conversion change
    if current_paid.get('conversions') and prev_paid.get('conversions'):
        conv_delta = calculate_delta(current_paid['conversions'], prev_paid['conversions'])
        if conv_delta and abs(conv_delta) > 10:
            direction = "up" if conv_delta > 0 else "down"
            insights.append({
                'type': 'conversions',
                'icon': '🎯',
                'title': f"Conversions {direction} {abs(conv_delta):.1f}%",
                'detail': f"From {prev_paid['conversions']:,.0f} to {current_paid['conversions']:,.0f}",
                'action': "Scale winning campaigns" if conv_delta > 0 else "Investigate drop"
            })
    
    # Check organic traffic change
    if current_organic.get('clicks') and prev_organic.get('clicks'):
        organic_delta = calculate_delta(current_organic['clicks'], prev_organic['clicks'])
        if organic_delta and abs(organic_delta) > 10:
            direction = "growing" if organic_delta > 0 else "declining"
            insights.append({
                'type': 'organic',
                'icon': '🔍',
                'title': f"Organic clicks {direction} {abs(organic_delta):.1f}%",
                'detail': f"From {prev_organic['clicks']:,.0f} to {current_organic['clicks']:,.0f}",
                'action': "SEO momentum building" if organic_delta > 0 else "Check ranking changes"
            })
    
    # Check CPA efficiency
    if current_paid.get('spend') and current_paid.get('conversions') and current_paid['conversions'] > 0:
        current_cpa = current_paid['spend'] / current_paid['conversions']
        if prev_paid.get('spend') and prev_paid.get('conversions') and prev_paid['conversions'] > 0:
            prev_cpa = prev_paid['spend'] / prev_paid['conversions']
            cpa_delta = calculate_delta(current_cpa, prev_cpa)
            if cpa_delta and abs(cpa_delta) > 10:
                direction = "increased" if cpa_delta > 0 else "improved"
                color = "🔴" if cpa_delta > 0 else "🟢"
                insights.append({
                    'type': 'cpa',
                    'icon': color,
                    'title': f"CPA {direction} {abs(cpa_delta):.1f}%",
                    'detail': f"From ${prev_cpa:.2f} to ${current_cpa:.2f}",
                    'action': "Optimize targeting" if cpa_delta > 0 else "Increase spend on winners"
                })
    
    # Channel performance insight
    if not channel_df.empty and len(channel_df) > 1:
        top_channel = channel_df.iloc[0]
        if top_channel['cpa'] and top_channel['cpa'] > 0:
            insights.append({
                'type': 'channel',
                'icon': '📊',
                'title': f"{top_channel['channel'].replace('_', ' ').title()} leads spend",
                'detail': f"${top_channel['spend']:,.0f} spend, ${top_channel['cpa']:.2f} CPA",
                'action': "Compare efficiency across platforms"
            })
    
    return insights[:4]  # Return max 4 insights


def detect_risk_signals(
    current_paid: Dict,
    prev_paid: Dict,
    targets: Dict
) -> List[Dict[str, str]]:
    """Detect risk signals and anomalies."""
    signals = []
    
    # Spend growing faster than conversions
    if (current_paid.get('spend') and prev_paid.get('spend') and 
        current_paid.get('conversions') and prev_paid.get('conversions')):
        
        spend_growth = calculate_delta(current_paid['spend'], prev_paid['spend']) or 0
        conv_growth = calculate_delta(current_paid['conversions'], prev_paid['conversions']) or 0
        
        if spend_growth > 10 and conv_growth < spend_growth - 10:
            signals.append({
                'type': 'warning',
                'icon': '⚠️',
                'message': f"Spend growing faster than conversions (+{spend_growth:.0f}% vs +{conv_growth:.0f}%)"
            })
    
    # CPA above target
    if current_paid.get('spend') and current_paid.get('conversions') and current_paid['conversions'] > 0:
        current_cpa = current_paid['spend'] / current_paid['conversions']
        if current_cpa > targets['cpa'] * 1.2:  # 20% above target
            signals.append({
                'type': 'warning',
                'icon': '🔴',
                'message': f"CPA (${current_cpa:.2f}) is {((current_cpa / targets['cpa']) - 1) * 100:.0f}% above target"
            })
    
    # Budget utilization
    if current_paid.get('spend') and targets['budget'] > 0:
        utilization = current_paid['spend'] / targets['budget']
        if utilization > 0.9:
            signals.append({
                'type': 'warning',
                'icon': '💸',
                'message': f"Budget {utilization * 100:.0f}% utilized"
            })
    
    # Positive signals
    if current_paid.get('conversions') and targets['conversions'] > 0:
        progress = current_paid['conversions'] / targets['conversions']
        if progress >= 1.0:
            signals.append({
                'type': 'success',
                'icon': '✅',
                'message': f"Conversion target achieved ({progress * 100:.0f}%)"
            })
    
    return signals


# ============================================
# Render Functions
# ============================================

def render_header(days_options: List[int] = [7, 14, 30, 90]) -> Tuple[int, str]:
    """
    Render Row 0 - Header with date selector and data freshness.
    
    Returns:
        (selected_days, comparison_type)
    """
    # This function is now deprecated - logic moved to main render function
    # Keeping for backwards compatibility
    return 30, "Previous Period"


def render_data_freshness(duckdb_path: str):
    """Render data freshness indicators as status badges."""
    freshness = get_data_freshness(duckdb_path)
    
    cols = st.columns(len(freshness))
    for i, (source, info) in enumerate(freshness.items()):
        with cols[i]:
            if info['status'] == 'ok':
                icon = "✅"
                text = f"{info['days_ago']}d ago" if info['days_ago'] else "today"
                st.success(f"**{source}**: {text}", icon=icon)
            elif info['status'] == 'warning':
                icon = "⚠️"
                text = f"{info['days_ago']}d lag"
                st.warning(f"**{source}**: {text}", icon=icon)
            elif info['status'] == 'no_data':
                icon = "❌"
                text = "no data"
                st.error(f"**{source}**: {text}", icon=icon)
            else:
                icon = "❓"
                text = "error"
                st.error(f"**{source}**: {text}", icon=icon)


def render_kpi_tiles(duckdb_path: str, start_date: str, end_date: str, 
                     prev_start: str, prev_end: str):
    """Render Row 1 - Core Health KPIs (6 tiles)."""
    
    # Get current and previous period metrics
    current_paid = get_paid_metrics(duckdb_path, start_date, end_date)
    prev_paid = get_paid_metrics(duckdb_path, prev_start, prev_end)
    
    current_web = get_web_metrics(duckdb_path, start_date, end_date)
    prev_web = get_web_metrics(duckdb_path, prev_start, prev_end)
    
    current_organic = get_organic_metrics(duckdb_path, start_date, end_date)
    prev_organic = get_organic_metrics(duckdb_path, prev_start, prev_end)
    
    # Calculate derived metrics
    current_cpa = (current_paid['spend'] / current_paid['conversions'] 
                   if current_paid['conversions'] and current_paid['conversions'] > 0 else None)
    prev_cpa = (prev_paid['spend'] / prev_paid['conversions']
                if prev_paid['conversions'] and prev_paid['conversions'] > 0 else None)
    
    current_roas = (current_paid['revenue'] / current_paid['spend'] 
                    if current_paid['spend'] and current_paid['spend'] > 0 else None)
    
    # Create 6 columns for KPI tiles
    cols = st.columns(6)
    
    # Tile 1: Paid Spend
    with cols[0]:
        spend_delta = calculate_delta(current_paid['spend'] or 0, prev_paid['spend'] or 0)
        st.metric(
            label="💰 Paid Spend",
            value=f"${current_paid['spend']:,.0f}" if current_paid['spend'] else "$0",
            delta=f"{spend_delta:+.1f}%" if spend_delta else None,
            delta_color="inverse"  # Lower spend can be good
        )
    
    # Tile 2: Total Sessions
    with cols[1]:
        sessions_delta = calculate_delta(safe_float(current_web['sessions']), safe_float(prev_web['sessions']))
        st.metric(
            label="👁️ Sessions",
            value=f"{safe_int(current_web['sessions']):,}",
            delta=f"{sessions_delta:+.1f}%" if sessions_delta else None
        )
    
    # Tile 3: Conversions (North Star)
    with cols[2]:
        conv_delta = calculate_delta(safe_float(current_paid['conversions']), safe_float(prev_paid['conversions']))
        st.metric(
            label="🎯 Conversions",
            value=f"{safe_int(current_paid['conversions']):,}",
            delta=f"{conv_delta:+.1f}%" if conv_delta else None
        )
    
    # Tile 4: Blended CPA
    with cols[3]:
        cpa_delta = calculate_delta(current_cpa or 0, prev_cpa or 0) if current_cpa and prev_cpa else None
        st.metric(
            label="📉 CPA",
            value=f"${current_cpa:.2f}" if current_cpa else "-",
            delta=f"{cpa_delta:+.1f}%" if cpa_delta else None,
            delta_color="inverse"  # Lower CPA is better
        )
    
    # Tile 5: Organic Clicks
    with cols[4]:
        organic_delta = calculate_delta(safe_float(current_organic['clicks']), safe_float(prev_organic['clicks']))
        st.metric(
            label="🔍 Organic Clicks",
            value=f"{safe_int(current_organic['clicks']):,}",
            delta=f"{organic_delta:+.1f}%" if organic_delta else None
        )
    
    # Tile 6: Revenue/ROAS
    with cols[5]:
        revenue_val = safe_float(current_paid['revenue'])
        if revenue_val > 0:
            st.metric(
                label="💵 Revenue",
                value=f"${revenue_val:,.0f}",
                delta=f"{current_roas:.1f}x ROAS" if current_roas else None
            )
        else:
            # Show clicks as fallback
            clicks_delta = calculate_delta(safe_float(current_paid['clicks']), safe_float(prev_paid['clicks']))
            st.metric(
                label="🖱️ Paid Clicks",
                value=f"{safe_int(current_paid['clicks']):,}",
                delta=f"{clicks_delta:+.1f}%" if clicks_delta else None
            )
    
    return current_paid, prev_paid, current_organic, prev_organic


def render_target_tracking(current_paid: Dict, targets: Dict):
    """Render Row 2 - Target tracking with RAG bars."""
    
    st.subheader("Target Progress")
    
    cols = st.columns(3)
    
    # Conversions vs Target
    with cols[0]:
        conv_progress = (current_paid['conversions'] / targets['conversions'] 
                        if targets['conversions'] > 0 else 0)
        conv_progress = min(conv_progress, 1.5)  # Cap at 150%
        
        if conv_progress >= 1.0:
            color = "🟢"
        elif conv_progress >= 0.7:
            color = "🟡"
        else:
            color = "🔴"
        
        st.markdown(f"**{color} Conversions**")
        st.progress(min(conv_progress, 1.0))
        st.caption(f"{int(current_paid['conversions'] or 0):,} / {targets['conversions']:,} ({conv_progress * 100:.0f}%)")
    
    # CPA vs Target (inverted - lower is better)
    with cols[1]:
        current_cpa = (current_paid['spend'] / current_paid['conversions']
                      if current_paid['conversions'] and current_paid['conversions'] > 0 else 0)
        
        if current_cpa <= targets['cpa']:
            color = "🟢"
            cpa_progress = 1.0
        elif current_cpa <= targets['cpa'] * 1.2:
            color = "🟡"
            cpa_progress = 0.7
        else:
            color = "🔴"
            cpa_progress = 0.3
        
        st.markdown(f"**{color} CPA Target**")
        st.progress(cpa_progress)
        st.caption(f"${current_cpa:.2f} / ${targets['cpa']:.2f} target")
    
    # Spend vs Budget
    with cols[2]:
        spend_progress = (current_paid['spend'] / targets['budget']
                         if targets['budget'] > 0 else 0)
        
        if spend_progress <= 0.9:
            color = "🟢"
        elif spend_progress <= 1.0:
            color = "🟡"
        else:
            color = "🔴"
        
        st.markdown(f"**{color} Budget**")
        st.progress(min(spend_progress, 1.0))
        st.caption(f"${current_paid['spend']:,.0f} / ${targets['budget']:,.0f} ({spend_progress * 100:.0f}%)")


def render_channel_table(duckdb_path: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Render Row 3 - Channel contribution table."""
    
    st.subheader("Channel Performance")
    
    channel_df = get_channel_breakdown(duckdb_path, start_date, end_date)
    
    if channel_df.empty:
        st.info("No channel data available for this period.")
        return channel_df
    
    # Add organic as a row
    organic = get_organic_metrics(duckdb_path, start_date, end_date)
    if organic['clicks'] and organic['clicks'] > 0:
        organic_row = pd.DataFrame([{
            'channel': 'Organic Search',
            'spend': 0,
            'clicks': organic['clicks'],
            'impressions': organic['impressions'],
            'conversions': 0,  # GSC doesn't track conversions
            'cpa': None
        }])
        channel_df = pd.concat([channel_df, organic_row], ignore_index=True)
    
    # Add web sessions from GA4
    web = get_web_metrics(duckdb_path, start_date, end_date)
    
    # Format for display
    display_df = channel_df.copy()
    display_df['channel'] = display_df['channel'].apply(
        lambda x: x.replace('_', ' ').title() if isinstance(x, str) else x
    )
    display_df['spend'] = display_df['spend'].apply(
        lambda x: f"${x:,.0f}" if x and x > 0 else "-"
    )
    display_df['clicks'] = display_df['clicks'].apply(
        lambda x: f"{int(x):,}" if x else "-"
    )
    display_df['impressions'] = display_df['impressions'].apply(
        lambda x: f"{int(x):,}" if x else "-"
    )
    display_df['conversions'] = display_df['conversions'].apply(
        lambda x: f"{int(x):,}" if x and x > 0 else "-"
    )
    display_df['cpa'] = display_df['cpa'].apply(
        lambda x: f"${x:.2f}" if x and x > 0 else "-"
    )
    
    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "channel": "Channel",
            "spend": "Spend",
            "clicks": "Clicks",
            "impressions": "Impressions",
            "conversions": "Conversions",
            "cpa": "CPA"
        }
    )
    
    return channel_df


def render_trend_chart(duckdb_path: str, days: int = 30):
    """Render Row 4 - Trend reality check chart."""
    
    st.subheader("Performance Trends")
    
    trend_df = get_trend_data(duckdb_path, days)
    
    if trend_df.empty:
        st.info("No trend data available.")
        return
    
    # Convert date_day to datetime for proper plotting
    trend_df['date_day'] = pd.to_datetime(trend_df['date_day'])
    trend_df = trend_df.set_index('date_day')
    
    # Create tabs for different views
    tab1, tab2, tab3 = st.tabs(["Spend & Conversions", "Sessions", "All Metrics"])
    
    with tab1:
        chart_df = trend_df[['paid_spend', 'conversions']].copy()
        chart_df.columns = ['Paid Spend ($)', 'Conversions']
        st.line_chart(chart_df)
    
    with tab2:
        chart_df = trend_df[['sessions', 'organic_clicks']].copy()
        chart_df.columns = ['Sessions (GA4)', 'Organic Clicks (GSC)']
        st.line_chart(chart_df)
    
    with tab3:
        # Normalize data for comparison
        normalized_df = trend_df.copy()
        for col in normalized_df.columns:
            max_val = normalized_df[col].max()
            if max_val > 0:
                normalized_df[col] = normalized_df[col] / max_val * 100
        
        normalized_df.columns = ['Paid Spend', 'Sessions', 'Conversions', 'Organic Clicks']
        st.line_chart(normalized_df)
        st.caption("Note: Metrics normalized to 0-100 scale for comparison")


def render_insights(insights: List[Dict[str, str]]):
    """Render Row 5 - What Changed narrative cards."""
    
    st.subheader("What Changed")
    
    if not insights:
        st.info("No significant changes detected in this period.")
        return
    
    cols = st.columns(min(len(insights), 4))
    
    for i, insight in enumerate(insights):
        with cols[i % 4]:
            st.markdown(f"""
            <div style="padding: 1rem; border-radius: 0.5rem; background-color: rgba(100, 100, 100, 0.1); margin-bottom: 0.5rem;">
                <div style="font-size: 1.5rem;">{insight['icon']}</div>
                <div style="font-weight: bold; margin: 0.5rem 0;">{insight['title']}</div>
                <div style="font-size: 0.9rem; color: gray;">{insight['detail']}</div>
                <div style="font-size: 0.8rem; margin-top: 0.5rem; font-style: italic;">→ {insight['action']}</div>
            </div>
            """, unsafe_allow_html=True)


def render_risk_signals(signals: List[Dict[str, str]]):
    """Render Row 6 - Risk signals and alerts."""
    
    if not signals:
        return
    
    st.subheader("Signals & Alerts")
    
    cols = st.columns(min(len(signals), 4))
    
    for i, signal in enumerate(signals):
        with cols[i % 4]:
            if signal['type'] == 'warning':
                st.warning(f"{signal['icon']} {signal['message']}")
            elif signal['type'] == 'success':
                st.success(f"{signal['icon']} {signal['message']}")
            else:
                st.info(f"{signal['icon']} {signal['message']}")


def render_data_trust_footer(duckdb_path: str):
    """Render Row 7 - Data trust status strip."""
    
    with st.expander("Data Trust & Operations", expanded=False):
        freshness = get_data_freshness(duckdb_path)
        
        st.markdown("**Last Data Update by Source:**")
        
        cols = st.columns(len(freshness))
        for i, (source, info) in enumerate(freshness.items()):
            with cols[i]:
                if info['last_date']:
                    st.markdown(f"**{source}**")
                    st.caption(f"Last: {info['last_date']}")
                    if info['days_ago'] and info['days_ago'] > 2:
                        st.caption(f"⚠️ {info['days_ago']} days lag")
                else:
                    st.markdown(f"**{source}**")
                    st.caption("No data")
        
        st.markdown("---")
        st.caption("""
        **Data Lag Notes:**
        - GA4: 24-48 hour processing delay
        - GSC: 2-3 day data delay  
        - Google Ads: Same-day data
        - Meta Ads: Same-day data
        """)


# ============================================
# Main Render Function
# ============================================

def render_executive_dashboard(duckdb_path: str):
    """
    Render the complete Executive Dashboard.
    
    Args:
        duckdb_path: Path to the DuckDB database
    """
    st.title("Executive Dashboard")
    st.markdown("*Unified view of marketing performance across all platforms*")
    
    # Row 0: Header - Date selector and data freshness
    st.divider()
    
    # ========================================
    # Date Range Selection - Using Calendar Component
    # ========================================
    
    from app.components.date_picker import render_date_range_picker
    
    # Render date range picker with comparison
    start_date, end_date, prev_start_date, prev_end_date = render_date_range_picker(
        key="executive_dashboard",
        default_days=30,
        max_days=365,
        show_comparison=True
    )
    
    # Convert dates to strings for SQL queries
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    prev_start = prev_start_date.strftime('%Y-%m-%d') if prev_start_date else None
    prev_end = prev_end_date.strftime('%Y-%m-%d') if prev_end_date else None
    
    # Data freshness indicators - separate row for better visibility
    st.markdown("##### 🔄 Data Status")
    render_data_freshness(duckdb_path)
    
    st.divider()
    
    # Row 1: Core KPIs
    current_paid, prev_paid, current_organic, prev_organic = render_kpi_tiles(
        duckdb_path, start_date, end_date, prev_start, prev_end
    )
    
    st.divider()
    
    # Row 2: Target Tracking
    render_target_tracking(current_paid, DEFAULT_TARGETS)
    
    st.divider()
    
    # Row 3: Channel Contribution
    channel_df = render_channel_table(duckdb_path, start_date, end_date)
    
    st.divider()
    
    # Row 4: Trend Chart
    # Calculate days from date range
    days = (end_date - start_date).days + 1
    render_trend_chart(duckdb_path, days=days)
    
    st.divider()
    
    # Row 5: Insights
    insights = generate_insights(
        current_paid, prev_paid, 
        current_organic, prev_organic,
        channel_df
    )
    render_insights(insights)
    
    # Row 6: Risk Signals
    signals = detect_risk_signals(current_paid, prev_paid, DEFAULT_TARGETS)
    render_risk_signals(signals)
    
    st.divider()
    
    # Row 7: Data Trust Footer
    render_data_trust_footer(duckdb_path)
