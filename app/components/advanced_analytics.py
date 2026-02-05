"""
Advanced Analytics Dashboard Components for RS Analytics

This module provides advanced analytical features combining traditional analysis
with ML-powered insights for deeper campaign optimization:

PERFORMANCE ANALYSIS:
    1. Campaign Efficiency Quadrant - Visual classification of campaigns
    2. Budget Optimization - Recommend optimal budget allocation
    
TEMPORAL ANALYSIS:
    3. Dayparting Optimization - Find optimal hours for ad delivery
    4. Creative Fatigue Detection - Meta Ads creative performance decay

KEYWORD INTELLIGENCE:
    5. Keyword Clustering - Group similar keywords using ML
    6. Keyword Performance Analysis - Wasted spend and opportunity detection
    7. SEO/PPC Cannibalization - Cross-channel keyword overlap

PREDICTIVE & MONITORING:
    8. Anomaly Detection - Automated alerts for metric anomalies
    9. Conversion Forecasting - Predict future conversions

Each component includes:
    - Clear explainer of the technique and why it matters
    - Visual representation of results
    - Actionable recommendations

Author: rs_analytics
Created: 2026-02-05
"""

from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any, List, Tuple
import streamlit as st
import pandas as pd
import numpy as np
import duckdb
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ML imports (with fallbacks for missing packages)
try:
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler
    from sklearn.decomposition import PCA
    from sklearn.metrics import silhouette_score
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False


# ============================================
# Data Loading Helpers
# ============================================

def load_data(duckdb_path: str, query: str, suppress_error: bool = False) -> Optional[pd.DataFrame]:
    """Load data from DuckDB with error handling."""
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        df = conn.execute(query).fetchdf()
        conn.close()
        return df
    except Exception as e:
        if not suppress_error:
            error_msg = str(e).lower()
            if "does not exist" not in error_msg and "not found" not in error_msg:
                st.error(f"Query error: {e}")
        return None


def check_table_exists(duckdb_path: str, table_name: str) -> bool:
    """Check if a table exists in the database."""
    try:
        conn = duckdb.connect(duckdb_path, read_only=True)
        result = conn.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()
        conn.close()
        return result[0] > 0 if result else False
    except Exception:
        return False


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide two numbers, handling zero division."""
    if denominator is None or denominator == 0 or numerator is None:
        return default
    return numerator / denominator


# ============================================
# Component 1: Campaign Efficiency Quadrant
# ============================================

def render_campaign_efficiency_quadrant(duckdb_path: str, start_date: date, end_date: date):
    """Render Campaign Efficiency Quadrant Analysis."""
    
    st.header("🎯 Campaign Efficiency Quadrant")
    
    # Explainer
    with st.expander("📚 **How This Works** - Click to learn", expanded=False):
        st.markdown("""
        ### What is the Efficiency Quadrant?
        
        A visual framework that classifies campaigns into 4 categories based on two dimensions:
        - **X-axis**: Spend (volume indicator)
        - **Y-axis**: CPA (efficiency indicator)
        
        ### Quadrant Classification:
        
        | Quadrant | Characteristics | Action |
        |----------|-----------------|--------|
        | ⭐ **Stars** | High spend + Low CPA | Scale aggressively |
        | ❓ **Question Marks** | Low spend + Low CPA | Test scaling potential |
        | 🐄 **Cash Cows** | High spend + High CPA | Optimize for efficiency |
        | 🐕 **Dogs** | Low spend + High CPA | Cut or restructure |
        
        ### Why This Matters:
        Instead of scanning through tables, instantly see which campaigns need action.
        """)
    
    st.caption("*Instantly see which campaigns to scale, optimize, or cut*")
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    # Query campaign data
    query = f"""
    WITH gads_campaigns AS (
        SELECT 
            'Google Ads' as platform,
            campaign_name,
            SUM(cost) as spend,
            SUM(clicks) as clicks,
            SUM(impressions) as impressions,
            SUM(conversions) as conversions,
            CASE WHEN SUM(conversions) > 0 
                 THEN SUM(cost) / SUM(conversions) 
                 ELSE NULL END as cpa
        FROM gads_campaigns
        WHERE date >= '{start_str}' AND date <= '{end_str}'
        GROUP BY campaign_name
        HAVING SUM(cost) > 0
    ),
    meta_campaigns AS (
        SELECT 
            'Meta Ads' as platform,
            campaign_name,
            SUM(spend) as spend,
            SUM(clicks) as clicks,
            SUM(impressions) as impressions,
            SUM(COALESCE(app_installs, 0) + COALESCE(purchases, 0)) as conversions,
            CASE WHEN SUM(COALESCE(app_installs, 0) + COALESCE(purchases, 0)) > 0 
                 THEN SUM(spend) / SUM(COALESCE(app_installs, 0) + COALESCE(purchases, 0))
                 ELSE NULL END as cpa
        FROM meta_campaign_insights
        WHERE date >= '{start_str}' AND date <= '{end_str}'
        GROUP BY campaign_name
        HAVING SUM(spend) > 0
    )
    SELECT * FROM gads_campaigns
    UNION ALL
    SELECT * FROM meta_campaigns
    ORDER BY spend DESC
    """
    
    df = load_data(duckdb_path, query, suppress_error=True)
    
    if df is None or df.empty:
        st.info("No campaign data available. Run ETL pipelines to populate data.")
        return
    
    df_with_conversions = df[df['cpa'].notna() & (df['conversions'] > 0)].copy()
    df_no_conversions = df[df['cpa'].isna() | (df['conversions'] == 0)].copy()
    
    if df_with_conversions.empty:
        st.warning("No campaigns with conversions in this period.")
        return
    
    # Calculate quadrant thresholds
    median_spend = df_with_conversions['spend'].median()
    median_cpa = df_with_conversions['cpa'].median()
    
    def classify_campaign(row):
        high_spend = row['spend'] >= median_spend
        low_cpa = row['cpa'] <= median_cpa
        
        if high_spend and low_cpa:
            return '⭐ Stars (Scale)'
        elif not high_spend and low_cpa:
            return '❓ Question Marks (Test)'
        elif high_spend and not low_cpa:
            return '🐄 Cash Cows (Optimize)'
        else:
            return '🐕 Dogs (Cut)'
    
    df_with_conversions['quadrant'] = df_with_conversions.apply(classify_campaign, axis=1)
    
    # Create scatter plot
    fig = px.scatter(
        df_with_conversions,
        x='spend', y='cpa', size='conversions', color='platform',
        hover_name='campaign_name',
        hover_data={'spend': ':$,.0f', 'cpa': ':$,.2f', 'conversions': ':,.0f'},
        title='Campaign Performance Quadrant',
        color_discrete_map={'Google Ads': '#4285F4', 'Meta Ads': '#1877F2'}
    )
    
    fig.add_hline(y=median_cpa, line_dash="dash", line_color="gray",
                  annotation_text=f"Median CPA: ${median_cpa:.2f}")
    fig.add_vline(x=median_spend, line_dash="dash", line_color="gray",
                  annotation_text=f"Median Spend: ${median_spend:,.0f}")
    
    fig.update_layout(height=500, xaxis_title="Spend ($)", yaxis_title="CPA ($)")
    st.plotly_chart(fig, use_container_width=True)
    
    # Summary and recommendations
    quadrant_summary = df_with_conversions.groupby('quadrant').agg({
        'campaign_name': 'count', 'spend': 'sum', 'conversions': 'sum', 'cpa': 'mean'
    }).reset_index()
    quadrant_summary.columns = ['Quadrant', 'Campaigns', 'Total Spend', 'Total Conversions', 'Avg CPA']
    quadrant_summary['Total Spend'] = quadrant_summary['Total Spend'].apply(lambda x: f"${x:,.0f}")
    quadrant_summary['Avg CPA'] = quadrant_summary['Avg CPA'].apply(lambda x: f"${x:.2f}")
    
    st.dataframe(quadrant_summary, use_container_width=True, hide_index=True)
    
    # Action recommendations
    st.subheader("💡 Recommended Actions")
    col1, col2 = st.columns(2)
    
    stars = df_with_conversions[df_with_conversions['quadrant'] == '⭐ Stars (Scale)']
    dogs = df_with_conversions[df_with_conversions['quadrant'] == '🐕 Dogs (Cut)']
    
    with col1:
        if not stars.empty:
            st.success(f"**Scale ({len(stars)} campaigns):**")
            for _, row in stars.head(3).iterrows():
                st.markdown(f"• {row['campaign_name'][:40]}... - CPA: ${row['cpa']:.2f}")
    
    with col2:
        if not dogs.empty:
            st.error(f"**Consider Cutting ({len(dogs)} campaigns):**")
            for _, row in dogs.head(3).iterrows():
                st.markdown(f"• {row['campaign_name'][:40]}... - CPA: ${row['cpa']:.2f}")


# ============================================
# Component 2: Budget Optimization
# ============================================

def render_budget_optimization(duckdb_path: str, start_date: date, end_date: date):
    """Render Budget Optimization recommendations."""
    
    st.header("💰 Budget Optimization")
    
    with st.expander("📚 **How This Works** - Click to learn", expanded=False):
        st.markdown("""
        ### What is Budget Optimization?
        
        Mathematically determines the ideal spend allocation across campaigns 
        to maximize conversions (or minimize CPA) given a fixed total budget.
        
        ### Our Approach:
        
        **1. Efficiency Scoring**
        ```
        Efficiency = Conversions / Cost
        ```
        
        **2. Relative Performance**
        - Compare each campaign to the account average
        - Score > 1.0 = better than average
        
        **3. Recommendations:**
        
        | Efficiency | Action |
        |------------|--------|
        | >1.5x avg | 🚀 Scale (+30-50%) |
        | 0.8-1.5x | ✅ Maintain |
        | 0.5-0.8x | ⚠️ Reduce (-20-30%) |
        | <0.5x | 🛑 Pause |
        
        ### Why This Matters:
        Data-driven allocation typically improves ROAS by **15-40%** vs gut-based decisions.
        """)
    
    st.caption("*Optimize budget allocation across campaigns for maximum ROI*")
    
    campaign_query = f"""
    SELECT 
        campaign_name,
        campaign_type,
        SUM(cost) as cost,
        SUM(conversions) as conversions,
        SUM(conversions_value) as conversions_value,
        SUM(clicks) as clicks,
        SUM(impressions) as impressions
    FROM gads_campaigns_v
    WHERE date_day BETWEEN '{start_date}' AND '{end_date}'
        AND campaign_name IS NOT NULL
    GROUP BY campaign_name, campaign_type
    HAVING SUM(cost) > 0
    ORDER BY cost DESC
    """
    
    df = load_data(duckdb_path, campaign_query, suppress_error=True)
    
    if df is None or df.empty:
        st.info("No Google Ads campaign data available.")
        return
    
    # Calculate efficiency metrics
    df['efficiency'] = df['conversions'] / df['cost'].replace(0, np.nan)
    df['cpa'] = df['cost'] / df['conversions'].replace(0, np.nan)
    
    avg_efficiency = df['efficiency'].mean()
    df['efficiency_ratio'] = df['efficiency'] / avg_efficiency if avg_efficiency > 0 else 1
    
    def classify_campaign(ratio):
        if ratio > 1.5: return '🚀 Scale'
        elif ratio > 0.8: return '✅ Maintain'
        elif ratio > 0.5: return '⚠️ Reduce'
        else: return '🛑 Pause'
    
    df['recommendation'] = df['efficiency_ratio'].apply(classify_campaign)
    
    # Summary metrics
    total_budget = df['cost'].sum()
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Spend", f"${total_budget:,.2f}")
    with col2:
        st.metric("Total Conversions", f"{df['conversions'].sum():,.1f}")
    with col3:
        overall_cpa = total_budget / df['conversions'].sum() if df['conversions'].sum() > 0 else 0
        st.metric("Overall CPA", f"${overall_cpa:,.2f}")
    
    # Efficiency chart
    fig = px.bar(
        df.sort_values('efficiency_ratio', ascending=True),
        x='efficiency_ratio', y='campaign_name', orientation='h',
        color='recommendation',
        color_discrete_map={'🚀 Scale': 'green', '✅ Maintain': 'blue', '⚠️ Reduce': 'orange', '🛑 Pause': 'red'},
        title="Campaign Efficiency (vs Average)"
    )
    fig.add_vline(x=1.0, line_dash="dash", line_color="gray", annotation_text="Average")
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    # Calculate recommended changes
    df['budget_change_pct'] = df['efficiency_ratio'].apply(
        lambda x: min(50, (x - 1) * 40) if x > 1 else max(-50, (x - 1) * 40)
    )
    df['recommended_budget'] = df['cost'] * (1 + df['budget_change_pct'] / 100)
    total_recommended = df['recommended_budget'].sum()
    df['recommended_budget_normalized'] = df['recommended_budget'] * (total_budget / total_recommended)
    df['budget_delta'] = df['recommended_budget_normalized'] - df['cost']
    
    # Recommendations table
    display_df = df[['campaign_name', 'cost', 'conversions', 'cpa', 'efficiency_ratio', 'recommendation', 'budget_delta']].copy()
    display_df = display_df.sort_values('efficiency_ratio', ascending=False)
    display_df['cost'] = display_df['cost'].apply(lambda x: f"${x:,.2f}")
    display_df['cpa'] = display_df['cpa'].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "—")
    display_df['efficiency_ratio'] = display_df['efficiency_ratio'].apply(lambda x: f"{x:.2f}x")
    display_df['budget_delta'] = display_df['budget_delta'].apply(lambda x: f"${x:+,.2f}")
    display_df.columns = ['Campaign', 'Current Spend', 'Conv.', 'CPA', 'Efficiency', 'Action', 'Budget Change']
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)


# ============================================
# Component 3: Dayparting Optimization
# ============================================

def render_dayparting_optimization(duckdb_path: str, start_date: date, end_date: date):
    """Render Dayparting Optimization analysis."""
    
    st.header("🕐 Dayparting Optimization")
    
    with st.expander("📚 **How This Works** - Click to learn", expanded=False):
        st.markdown("""
        ### What is Dayparting?
        
        Adjusting bids or pausing ads based on time of day and day of week to ensure 
        budget is spent when users are most likely to convert.
        
        ### Our Analysis:
        
        **Performance Index**
        ```
        Index = Hour_Conv_Rate / Daily_Avg_Conv_Rate
        ```
        - Index > 1.0 = better than average
        - Index < 1.0 = worse than average
        
        ### Recommendations:
        
        | Performance | Action |
        |-------------|--------|
        | 🟢 Top 20% | Increase bids 20-40% |
        | 🟡 Average | Keep current bids |
        | 🔴 Bottom 20% | Reduce bids 20-40% |
        
        ### Real-World Impact:
        Typical improvement: **15-30% better CPA**
        """)
    
    st.caption("*Find the best hours and days for your ad campaigns*")
    
    hourly_query = f"""
    SELECT 
        date_day,
        CAST(hour AS INTEGER) as hour,
        SUM(impressions) as impressions,
        SUM(clicks) as clicks,
        SUM(cost) as cost,
        SUM(conversions) as conversions
    FROM gads_hourly_v
    WHERE date_day BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY date_day, hour
    ORDER BY date_day, hour
    """
    
    df = load_data(duckdb_path, hourly_query, suppress_error=True)
    
    if df is None or df.empty:
        st.info("No hourly Google Ads data available.")
        return
    
    df['date_day'] = pd.to_datetime(df['date_day'])
    df['day_of_week'] = df['date_day'].dt.day_name()
    df['day_num'] = df['date_day'].dt.dayofweek
    df['efficiency'] = df['conversions'] / df['cost'].replace(0, np.nan)
    
    # Aggregate by hour and day
    heatmap_data = df.groupby(['day_num', 'hour']).agg({
        'clicks': 'sum', 'conversions': 'sum', 'cost': 'sum', 'impressions': 'sum'
    }).reset_index()
    heatmap_data['efficiency'] = heatmap_data['conversions'] / heatmap_data['cost'].replace(0, np.nan)
    
    # Create heatmap
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    pivot_data = heatmap_data.pivot(index='day_num', columns='hour', values='conversions').fillna(0)
    pivot_data.index = [day_order[i] for i in pivot_data.index]
    
    fig = px.imshow(
        pivot_data,
        labels=dict(x="Hour of Day", y="Day of Week", color="Conversions"),
        x=[f"{h:02d}:00" for h in range(24)],
        y=day_order,
        color_continuous_scale='RdYlGn',
        aspect='auto'
    )
    fig.update_layout(title="Conversions by Hour and Day", height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    # Best/worst hours
    hourly_agg = heatmap_data.groupby('hour').agg({
        'conversions': 'sum', 'cost': 'sum', 'clicks': 'sum'
    }).reset_index()
    hourly_agg['efficiency'] = hourly_agg['conversions'] / hourly_agg['cost'].replace(0, np.nan)
    hourly_agg = hourly_agg.dropna(subset=['efficiency'])
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**🌟 Best Hours**")
        for _, row in hourly_agg.nlargest(5, 'efficiency').iterrows():
            st.success(f"**{int(row['hour']):02d}:00** - {row['efficiency']:.3f} conv/$")
    with col2:
        st.markdown("**⚠️ Worst Hours**")
        for _, row in hourly_agg.nsmallest(5, 'efficiency').iterrows():
            if row['efficiency'] > 0:
                st.warning(f"**{int(row['hour']):02d}:00** - {row['efficiency']:.3f} conv/$")


# ============================================
# Component 4: Creative Fatigue Detection
# ============================================

def render_creative_fatigue_analysis(duckdb_path: str, start_date: date, end_date: date):
    """Render Creative Fatigue Detection for Meta Ads."""
    
    st.header("🎨 Creative Fatigue Analysis")
    
    with st.expander("📚 **How This Works** - Click to learn", expanded=False):
        st.markdown("""
        ### What is Creative Fatigue?
        
        When ad performance degrades over time because users have seen the creative 
        too many times. Key indicators:
        
        - CTR decline >20% while frequency >3
        - Same creative running >14 days
        - Declining conversion rate over time
        
        ### Fatigue Score (0-100):
        - **CTR Decline** (40 points max)
        - **High Frequency** (30 points max)  
        - **Days Running** (30 points max)
        
        | Score | Status |
        |-------|--------|
        | 60+ | 🔴 High Fatigue - Refresh now |
        | 30-60 | 🟡 Moderate - Plan refresh |
        | <30 | 🟢 Healthy |
        
        ### Why This Matters:
        Creative fatigue is a top reason for campaign performance decline.
        Early detection allows proactive creative refresh.
        """)
    
    st.caption("*Detect ad creative performance decay before it impacts results*")
    
    query = f"""
    SELECT 
        date, campaign_id, campaign_name,
        SUM(impressions) as impressions, SUM(reach) as reach,
        SUM(clicks) as clicks, SUM(spend) as spend,
        AVG(frequency) as frequency, AVG(ctr) as ctr,
        SUM(COALESCE(app_installs, 0) + COALESCE(purchases, 0)) as conversions
    FROM meta_campaign_insights
    WHERE date >= '{start_date}' AND date <= '{end_date}'
    GROUP BY date, campaign_id, campaign_name
    ORDER BY campaign_id, date
    """
    
    df = load_data(duckdb_path, query, suppress_error=True)
    
    if df is None or df.empty:
        st.info("No Meta Ads campaign data available.")
        return
    
    fatigue_results = []
    
    for campaign_id in df['campaign_id'].unique():
        campaign_df = df[df['campaign_id'] == campaign_id].sort_values('date')
        
        if len(campaign_df) < 3:
            continue
        
        campaign_name = campaign_df['campaign_name'].iloc[0]
        total_days = len(campaign_df)
        avg_frequency = campaign_df['frequency'].mean()
        total_spend = campaign_df['spend'].sum()
        
        midpoint = len(campaign_df) // 2
        first_half_ctr = campaign_df.iloc[:midpoint]['ctr'].mean()
        second_half_ctr = campaign_df.iloc[midpoint:]['ctr'].mean()
        ctr_decline = ((first_half_ctr - second_half_ctr) / first_half_ctr * 100) if first_half_ctr > 0 else 0
        
        # Calculate fatigue score
        fatigue_score = 0
        if ctr_decline > 0:
            fatigue_score += min(ctr_decline * 2, 40)
        if avg_frequency > 2:
            fatigue_score += min((avg_frequency - 2) * 10, 30)
        if total_days > 14:
            fatigue_score += min((total_days - 14) * 2, 30)
        
        fatigue_results.append({
            'campaign_name': campaign_name,
            'days_running': total_days,
            'avg_frequency': avg_frequency,
            'ctr_decline_pct': ctr_decline,
            'total_spend': total_spend,
            'fatigue_score': fatigue_score
        })
    
    if not fatigue_results:
        st.info("Not enough data to calculate fatigue metrics.")
        return
    
    fatigue_df = pd.DataFrame(fatigue_results).sort_values('fatigue_score', ascending=False)
    fatigue_df['status'] = fatigue_df['fatigue_score'].apply(
        lambda x: '🔴 High Fatigue' if x >= 60 else ('🟡 Moderate' if x >= 30 else '🟢 Healthy')
    )
    
    # Summary
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("🔴 High Fatigue", len(fatigue_df[fatigue_df['fatigue_score'] >= 60]))
    with col2:
        st.metric("🟡 Moderate", len(fatigue_df[(fatigue_df['fatigue_score'] >= 30) & (fatigue_df['fatigue_score'] < 60)]))
    with col3:
        st.metric("🟢 Healthy", len(fatigue_df[fatigue_df['fatigue_score'] < 30]))
    
    # Visualization
    fig = px.scatter(
        fatigue_df, x='avg_frequency', y='ctr_decline_pct', size='total_spend',
        color='fatigue_score', hover_name='campaign_name',
        color_continuous_scale='RdYlGn_r',
        title='Creative Fatigue: Frequency vs CTR Decline'
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    # Detailed table
    display_df = fatigue_df[['status', 'campaign_name', 'days_running', 'avg_frequency', 'ctr_decline_pct', 'fatigue_score']].copy()
    display_df['avg_frequency'] = display_df['avg_frequency'].apply(lambda x: f"{x:.1f}")
    display_df['ctr_decline_pct'] = display_df['ctr_decline_pct'].apply(lambda x: f"{x:+.1f}%")
    display_df['fatigue_score'] = display_df['fatigue_score'].apply(lambda x: f"{x:.0f}/100")
    display_df.columns = ['Status', 'Campaign', 'Days', 'Freq', 'CTR Change', 'Score']
    
    st.dataframe(display_df, use_container_width=True, hide_index=True)


# ============================================
# Component 5: Keyword Clustering
# ============================================

def render_keyword_clustering(duckdb_path: str, start_date: date, end_date: date):
    """Render Keyword Clustering using ML."""
    
    st.header("🔤 Keyword Clustering")
    
    with st.expander("📚 **How This Works** - Click to learn", expanded=False):
        st.markdown("""
        ### What is Keyword Clustering?
        
        Groups similar keywords together using machine learning to reveal:
        - **Hidden themes** in your search traffic
        - **Content gaps** where you have impressions but no pages
        - **Optimization opportunities** across similar keywords
        
        ### Technique: K-Means Clustering
        
        Features used:
        - Normalized CTR
        - Normalized Position
        - Normalized Clicks
        - Normalized Impressions
        
        ### Cluster Types:
        
        | Type | Characteristics | Action |
        |------|-----------------|--------|
        | 🌟 Stars | High CTR, Good position | Scale & protect |
        | 📈 Opportunities | High impressions, low CTR | Improve meta descriptions |
        | 🎯 Quick Wins | Position 4-10, decent CTR | Push to top 3 |
        | 🔍 Long Tail | Low volume, high intent | Group into themes |
        """)
    
    st.caption("*Group similar keywords to find optimization opportunities*")
    
    if not SKLEARN_AVAILABLE:
        st.warning("Scikit-learn not installed. Run: `pip install scikit-learn`")
        return
    
    data_source = st.radio("Data Source", ['gsc', 'gads'],
                           format_func=lambda x: 'GSC (Organic)' if x == 'gsc' else 'Google Ads (Paid)',
                           horizontal=True)
    
    if data_source == 'gsc':
        query = f"""
        SELECT query as keyword, SUM(clicks) as clicks, SUM(impressions) as impressions,
               AVG(ctr) as ctr, AVG(position) as position
        FROM gsc_queries_v
        WHERE date_day BETWEEN '{start_date}' AND '{end_date}' AND query IS NOT NULL
        GROUP BY query HAVING SUM(impressions) >= 10
        ORDER BY impressions DESC LIMIT 500
        """
    else:
        query = f"""
        SELECT keyword_text as keyword, SUM(clicks) as clicks, SUM(impressions) as impressions,
               AVG(ctr) as ctr, SUM(conversions) as conversions
        FROM gads_keywords_v
        WHERE date_day BETWEEN '{start_date}' AND '{end_date}' AND keyword_text IS NOT NULL
        GROUP BY keyword_text HAVING SUM(impressions) >= 10
        ORDER BY impressions DESC LIMIT 500
        """
    
    df = load_data(duckdb_path, query, suppress_error=True)
    
    if df is None or len(df) < 10:
        st.info("Need at least 10 keywords for clustering.")
        return
    
    st.info(f"Analyzing **{len(df):,}** keywords")
    
    feature_cols = ['clicks', 'impressions', 'ctr']
    if 'position' in df.columns:
        feature_cols.append('position')
    
    df_clean = df.dropna(subset=feature_cols)
    
    scaler = StandardScaler()
    features_normalized = scaler.fit_transform(df_clean[feature_cols])
    
    n_clusters = st.slider("Number of Clusters", 3, 10, 5)
    
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    df_clean['cluster'] = kmeans.fit_predict(features_normalized)
    
    # PCA for visualization
    pca = PCA(n_components=2)
    pca_result = pca.fit_transform(features_normalized)
    df_clean['pca_x'] = pca_result[:, 0]
    df_clean['pca_y'] = pca_result[:, 1]
    
    fig = px.scatter(
        df_clean, x='pca_x', y='pca_y', color='cluster',
        hover_data=['keyword', 'clicks', 'impressions', 'ctr'],
        title="Keyword Clusters (PCA Visualization)"
    )
    fig.update_layout(height=450)
    st.plotly_chart(fig, use_container_width=True)
    
    # Cluster summary
    cluster_summary = df_clean.groupby('cluster').agg({
        'keyword': 'count', 'clicks': 'sum', 'impressions': 'sum', 'ctr': 'mean'
    }).reset_index()
    cluster_summary.columns = ['Cluster', 'Keywords', 'Clicks', 'Impressions', 'Avg CTR']
    cluster_summary['Avg CTR'] = cluster_summary['Avg CTR'].apply(lambda x: f"{x:.2%}")
    
    st.dataframe(cluster_summary, use_container_width=True, hide_index=True)
    
    # Show keywords for selected cluster
    selected_cluster = st.selectbox("Explore Cluster", sorted(df_clean['cluster'].unique()))
    cluster_keywords = df_clean[df_clean['cluster'] == selected_cluster].nlargest(15, 'impressions')
    
    display_kw = cluster_keywords[['keyword', 'clicks', 'impressions', 'ctr']].copy()
    display_kw['ctr'] = display_kw['ctr'].apply(lambda x: f"{x:.2%}")
    st.dataframe(display_kw, use_container_width=True, hide_index=True)


# ============================================
# Component 6: Keyword Performance Analysis
# ============================================

def render_keyword_analysis(duckdb_path: str, start_date: date, end_date: date):
    """Render Keyword Performance Analysis."""
    
    st.header("🔑 Keyword Performance Analysis")
    
    with st.expander("📚 **How This Works** - Click to learn", expanded=False):
        st.markdown("""
        ### Keyword Analysis Components:
        
        **1. Wasted Spend**
        Keywords spending money without generating conversions.
        Action: Add as negatives or pause.
        
        **2. Scaling Opportunities**
        Low-volume keywords with good efficiency.
        Action: Increase bids, expand match types.
        
        **3. Match Type Analysis**
        Compare Exact vs Phrase vs Broad performance.
        Action: Optimize match type mix.
        """)
    
    st.caption("*Find wasted spend and scaling opportunities*")
    
    query = f"""
    SELECT keyword_text, keyword_match_type, campaign_name,
           SUM(impressions) as impressions, SUM(clicks) as clicks,
           SUM(cost) as spend, SUM(conversions) as conversions,
           CASE WHEN SUM(clicks) > 0 THEN SUM(conversions) / SUM(clicks) * 100 ELSE 0 END as conv_rate
    FROM gads_keywords
    WHERE date >= '{start_date}' AND date <= '{end_date}' AND keyword_text IS NOT NULL
    GROUP BY keyword_text, keyword_match_type, campaign_name
    HAVING SUM(impressions) > 0
    ORDER BY spend DESC
    """
    
    df = load_data(duckdb_path, query, suppress_error=True)
    
    if df is None or df.empty:
        st.info("No keyword data available.")
        return
    
    tab1, tab2, tab3 = st.tabs(["💸 Wasted Spend", "🚀 Opportunities", "📊 Match Types"])
    
    with tab1:
        wasted_df = df[(df['spend'] > 0) & ((df['conversions'] == 0) | df['conversions'].isna())].sort_values('spend', ascending=False)
        
        if not wasted_df.empty:
            total_wasted = wasted_df['spend'].sum()
            st.error(f"**Total wasted: ${total_wasted:,.2f}** across {len(wasted_df)} keywords")
            
            display_wasted = wasted_df.head(15)[['keyword_text', 'keyword_match_type', 'spend', 'clicks']].copy()
            display_wasted['spend'] = display_wasted['spend'].apply(lambda x: f"${x:,.2f}")
            display_wasted.columns = ['Keyword', 'Match Type', 'Wasted Spend', 'Clicks']
            st.dataframe(display_wasted, use_container_width=True, hide_index=True)
        else:
            st.success("No keywords with wasted spend!")
    
    with tab2:
        median_spend = df[df['spend'] > 0]['spend'].median()
        median_cpa = df[df['conversions'] > 0]['spend'].sum() / df['conversions'].sum() if df['conversions'].sum() > 0 else None
        
        if median_cpa:
            opportunities = df[(df['conversions'] > 0) & (df['spend'] / df['conversions'] <= median_cpa) & (df['spend'] < median_spend)].sort_values('conv_rate', ascending=False)
            
            if not opportunities.empty:
                st.success(f"**{len(opportunities)} scaling candidates**")
                display_opp = opportunities.head(15)[['keyword_text', 'spend', 'conversions', 'conv_rate']].copy()
                display_opp['spend'] = display_opp['spend'].apply(lambda x: f"${x:,.2f}")
                display_opp['conv_rate'] = display_opp['conv_rate'].apply(lambda x: f"{x:.1f}%")
                display_opp.columns = ['Keyword', 'Spend', 'Conv', 'Conv Rate']
                st.dataframe(display_opp, use_container_width=True, hide_index=True)
    
    with tab3:
        match_summary = df.groupby('keyword_match_type').agg({
            'keyword_text': 'count', 'spend': 'sum', 'conversions': 'sum', 'clicks': 'sum'
        }).reset_index()
        match_summary['CPA'] = match_summary['spend'] / match_summary['conversions'].replace(0, np.nan)
        match_summary.columns = ['Match Type', 'Keywords', 'Spend', 'Conv', 'Clicks', 'CPA']
        match_summary['Spend'] = match_summary['Spend'].apply(lambda x: f"${x:,.0f}")
        match_summary['CPA'] = match_summary['CPA'].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "—")
        
        st.dataframe(match_summary, use_container_width=True, hide_index=True)


# ============================================
# Component 7: SEO/PPC Cannibalization
# ============================================

def render_seo_ppc_cannibalization(duckdb_path: str, start_date: date, end_date: date):
    """Render SEO/PPC Cannibalization Analysis."""
    
    st.header("🔄 SEO/PPC Cannibalization")
    
    with st.expander("📚 **How This Works** - Click to learn", expanded=False):
        st.markdown("""
        ### What is Cannibalization?
        
        Paying for ad clicks on keywords where you already rank well organically.
        You're essentially bidding against yourself.
        
        ### Risk Classification:
        
        | Organic Position | Risk | Action |
        |------------------|------|--------|
        | 1-3 | 🔴 High | Consider pausing ads |
        | 4-5 | 🟡 Medium | Test reducing bids |
        | 6+ | 🟢 Low | Keep ads running |
        
        ### Why This Matters:
        - Stop paying for clicks you'd get organically
        - Typical savings: **15-30%** of branded spend
        - Reallocate to conquest keywords
        """)
    
    st.caption("*Find keywords where you're paying for organic traffic*")
    
    query = f"""
    WITH organic AS (
        SELECT LOWER(query) as keyword, SUM(clicks) as organic_clicks,
               SUM(impressions) as organic_impressions, AVG(position) as avg_position
        FROM gsc_queries
        WHERE date >= '{start_date}' AND date <= '{end_date}'
        GROUP BY LOWER(query) HAVING SUM(impressions) > 100
    ),
    paid AS (
        SELECT LOWER(keyword_text) as keyword, SUM(clicks) as paid_clicks,
               SUM(cost) as paid_spend, SUM(conversions) as paid_conversions
        FROM gads_keywords
        WHERE date >= '{start_date}' AND date <= '{end_date}'
        GROUP BY LOWER(keyword_text) HAVING SUM(impressions) > 0
    )
    SELECT o.keyword, o.organic_clicks, o.avg_position,
           p.paid_clicks, p.paid_spend, p.paid_conversions
    FROM organic o
    JOIN paid p ON o.keyword = p.keyword
    ORDER BY p.paid_spend DESC
    """
    
    df = load_data(duckdb_path, query, suppress_error=True)
    
    if df is None or df.empty:
        st.info("No overlapping keywords found. Need both GSC and Google Ads data.")
        return
    
    def classify_risk(pos):
        if pos <= 3: return '🔴 High'
        elif pos <= 5: return '🟡 Medium'
        else: return '🟢 Low'
    
    df['risk'] = df['avg_position'].apply(classify_risk)
    
    # Summary
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Overlapping Keywords", f"{len(df):,}")
    with col2:
        st.metric("Total Overlap Spend", f"${df['paid_spend'].sum():,.0f}")
    with col3:
        potential_savings = df[df['avg_position'] <= 3]['paid_spend'].sum()
        st.metric("Potential Savings", f"${potential_savings:,.0f}")
    
    # Scatter plot
    fig = px.scatter(
        df, x='avg_position', y='paid_spend', size='organic_clicks',
        color='risk', hover_name='keyword',
        color_discrete_map={'🔴 High': 'red', '🟡 Medium': 'orange', '🟢 Low': 'green'},
        title="Cannibalization Risk: Organic Position vs Paid Spend"
    )
    fig.add_vline(x=3, line_dash="dash", line_color="green", annotation_text="Position 3")
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    # High risk keywords
    high_risk = df[df['avg_position'] <= 3].sort_values('paid_spend', ascending=False)
    if not high_risk.empty:
        st.error(f"**{len(high_risk)} high-risk keywords** (top 3 organic, still paying for ads)")
        display_hr = high_risk.head(10)[['keyword', 'avg_position', 'organic_clicks', 'paid_spend']].copy()
        display_hr['avg_position'] = display_hr['avg_position'].apply(lambda x: f"{x:.1f}")
        display_hr['paid_spend'] = display_hr['paid_spend'].apply(lambda x: f"${x:,.2f}")
        display_hr.columns = ['Keyword', 'Organic Pos', 'Organic Clicks', 'Paid Spend']
        st.dataframe(display_hr, use_container_width=True, hide_index=True)


# ============================================
# Component 8: Anomaly Detection
# ============================================

def render_anomaly_detection(duckdb_path: str, start_date: date, end_date: date):
    """Render Anomaly Detection analysis."""
    
    st.header("🚨 Anomaly Detection")
    
    with st.expander("📚 **How This Works** - Click to learn", expanded=False):
        st.markdown("""
        ### What is Anomaly Detection?
        
        Identifies data points that deviate significantly from expected patterns.
        
        ### Technique: Z-Score with Rolling Window
        
        ```
        Z-Score = (Value - Rolling_Mean) / Rolling_StdDev
        ```
        
        - 7-day rolling window for recent trends
        - Alert threshold: |Z| > 2.5 (99% confidence)
        
        ### Why This Matters:
        - 🔴 Catch tracking failures (zero conversions)
        - 🟡 Detect performance shifts (CPA spikes)
        - 🟢 Spot opportunities (conversion spikes)
        """)
    
    st.caption("*Automatically detect unusual patterns in your metrics*")
    
    query = f"""
    SELECT date_day, SUM(cost) as cost, SUM(clicks) as clicks,
           SUM(conversions) as conversions, SUM(impressions) as impressions
    FROM gads_campaigns_v
    WHERE date_day BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY date_day ORDER BY date_day
    """
    
    df = load_data(duckdb_path, query, suppress_error=True)
    
    if df is None or len(df) < 7:
        st.info("Need at least 7 days of data for anomaly detection.")
        return
    
    metric = st.selectbox("Metric", ['cost', 'conversions', 'clicks'],
                          format_func=lambda x: {'cost': 'Spend', 'conversions': 'Conversions', 'clicks': 'Clicks'}[x])
    
    z_threshold = st.slider("Z-Score Threshold", 1.5, 4.0, 2.5, 0.1)
    
    # Calculate anomalies
    df['rolling_mean'] = df[metric].rolling(window=7, min_periods=3).mean()
    df['rolling_std'] = df[metric].rolling(window=7, min_periods=3).std()
    df['z_score'] = (df[metric] - df['rolling_mean']) / df['rolling_std'].replace(0, np.nan)
    df['is_anomaly'] = abs(df['z_score']) > z_threshold
    df['anomaly_type'] = df.apply(
        lambda r: 'spike' if r['z_score'] > z_threshold else ('drop' if r['z_score'] < -z_threshold else 'normal'), axis=1
    )
    
    # Visualization
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['date_day'], y=df[metric], mode='lines+markers', name=metric.title()))
    fig.add_trace(go.Scatter(x=df['date_day'], y=df['rolling_mean'], mode='lines', name='7-Day MA', line=dict(dash='dash', color='gray')))
    
    anomalies = df[df['is_anomaly']]
    if not anomalies.empty:
        spikes = anomalies[anomalies['anomaly_type'] == 'spike']
        drops = anomalies[anomalies['anomaly_type'] == 'drop']
        
        if not spikes.empty:
            fig.add_trace(go.Scatter(x=spikes['date_day'], y=spikes[metric], mode='markers',
                                     name='Spike ↑', marker=dict(color='red', size=15, symbol='triangle-up')))
        if not drops.empty:
            fig.add_trace(go.Scatter(x=drops['date_day'], y=drops[metric], mode='markers',
                                     name='Drop ↓', marker=dict(color='orange', size=15, symbol='triangle-down')))
    
    fig.update_layout(title=f"Anomaly Detection: {metric.title()}", height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    # Summary
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Days Analyzed", len(df))
    with col2:
        st.metric("Anomalies Found", len(anomalies))
    with col3:
        st.metric("Spikes ↑ / Drops ↓", f"{len(anomalies[anomalies['anomaly_type']=='spike'])} / {len(anomalies[anomalies['anomaly_type']=='drop'])}")
    
    if not anomalies.empty:
        st.subheader("📋 Detected Anomalies")
        display_anom = anomalies[['date_day', metric, 'rolling_mean', 'z_score', 'anomaly_type']].copy()
        display_anom['deviation'] = ((display_anom[metric] - display_anom['rolling_mean']) / display_anom['rolling_mean'] * 100)
        display_anom['z_score'] = display_anom['z_score'].apply(lambda x: f"{x:+.2f}")
        display_anom['deviation'] = display_anom['deviation'].apply(lambda x: f"{x:+.1f}%")
        display_anom['anomaly_type'] = display_anom['anomaly_type'].apply(lambda x: '↑ Spike' if x == 'spike' else '↓ Drop')
        display_anom.columns = ['Date', 'Value', 'Expected', 'Z-Score', 'Deviation', 'Type']
        st.dataframe(display_anom, use_container_width=True, hide_index=True)


# ============================================
# Component 9: Conversion Forecasting
# ============================================

def render_conversion_forecasting(duckdb_path: str, start_date: date, end_date: date):
    """Render Conversion Forecasting."""
    
    st.header("📈 Conversion Forecasting")
    
    with st.expander("📚 **How This Works** - Click to learn", expanded=False):
        st.markdown("""
        ### What is Conversion Forecasting?
        
        Predicts future conversion volumes based on historical patterns.
        
        ### Our Method:
        
        **Moving Average + Trend**
        ```
        Forecast = 7-Day_MA + (Trend × Days_Ahead)
        ```
        
        ### Confidence Intervals:
        - 95% confidence band using historical std deviation
        - Wider band = more uncertainty
        
        ### Interpreting Results:
        
        | Indicator | Meaning |
        |-----------|---------|
        | 📈 Upward trend | Performance improving |
        | 📉 Downward trend | Performance declining |
        | Narrow band | Predictable |
        | Wide band | High volatility |
        """)
    
    st.caption("*Predict future conversions based on historical trends*")
    
    query = f"""
    SELECT date_day, SUM(conversions) as conversions, SUM(cost) as cost, SUM(clicks) as clicks
    FROM gads_campaigns_v
    WHERE date_day <= '{end_date}'
    GROUP BY date_day ORDER BY date_day
    """
    
    df = load_data(duckdb_path, query, suppress_error=True)
    
    if df is None or len(df) < 14:
        st.info("Need at least 14 days of data for forecasting.")
        return
    
    df['date_day'] = pd.to_datetime(df['date_day'])
    df = df.sort_values('date_day')
    
    metric = st.selectbox("Metric to Forecast", ['conversions', 'cost', 'clicks'],
                          format_func=lambda x: {'conversions': 'Conversions', 'cost': 'Spend', 'clicks': 'Clicks'}[x])
    forecast_days = st.slider("Forecast Horizon (days)", 7, 30, 14)
    
    df['ma_7'] = df[metric].rolling(window=7).mean()
    
    # Calculate trend
    recent_data = df.tail(14)
    x = np.arange(len(recent_data))
    y = recent_data[metric].values
    mask = ~np.isnan(y)
    slope, intercept = np.polyfit(x[mask], y[mask], 1) if mask.sum() >= 7 else (0, df[metric].mean())
    
    # Generate forecast
    last_date = df['date_day'].max()
    ma_7_last = df['ma_7'].iloc[-1]
    
    forecast_dates = pd.date_range(start=last_date + timedelta(days=1), periods=forecast_days)
    forecast_values = [max(0, ma_7_last + (slope * (i + 1))) for i in range(forecast_days)]
    
    std_dev = df[metric].std()
    lower_bound = [max(0, v - 1.96 * std_dev) for v in forecast_values]
    upper_bound = [v + 1.96 * std_dev for v in forecast_values]
    
    # Visualization
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df['date_day'], y=df[metric], mode='lines', name='Historical'))
    fig.add_trace(go.Scatter(x=df['date_day'], y=df['ma_7'], mode='lines', name='7-Day MA', line=dict(dash='dash', color='gray')))
    fig.add_trace(go.Scatter(x=forecast_dates, y=forecast_values, mode='lines+markers', name='Forecast', line=dict(dash='dot', color='orange')))
    fig.add_trace(go.Scatter(
        x=list(forecast_dates) + list(forecast_dates)[::-1],
        y=upper_bound + lower_bound[::-1],
        fill='toself', fillcolor='rgba(255, 127, 14, 0.2)',
        line=dict(color='rgba(255,255,255,0)'), name='95% CI'
    ))
    fig.update_layout(title=f"{metric.title()} Forecast", height=450)
    st.plotly_chart(fig, use_container_width=True)
    
    # Summary
    col1, col2, col3 = st.columns(3)
    total_forecast = sum(forecast_values)
    with col1:
        st.metric(f"Forecasted Total ({forecast_days}d)", f"{total_forecast:,.0f}" if metric != 'cost' else f"${total_forecast:,.2f}")
    with col2:
        trend_dir = "📈 Up" if slope > 0 else ("📉 Down" if slope < 0 else "➡️ Stable")
        st.metric("Trend", trend_dir)
    with col3:
        daily_change = (slope / ma_7_last * 100) if ma_7_last > 0 else 0
        st.metric("Daily Trend", f"{daily_change:+.1f}%")


# ============================================
# Main Advanced Analytics Dashboard
# ============================================

def render_advanced_analytics_tab(duckdb_path: str):
    """Render the complete Advanced Analytics tab."""
    
    st.title("🔬 Advanced Analytics")
    st.markdown("""
    *Deep-dive analysis tools combining traditional metrics with ML-powered insights.*
    
    Each analysis includes an **📚 explainer** — click to learn how it works and why it matters.
    """)
    
    st.divider()
    
    # Date selection
    from app.components.date_picker import render_date_range_picker
    start_date, end_date, _, _ = render_date_range_picker(
        key="advanced_analytics", default_days=30, max_days=90, show_comparison=False
    )
    
    st.divider()
    
    # Analysis selection
    analysis_options = {
        'quadrant': '🎯 Campaign Efficiency Quadrant',
        'budget': '💰 Budget Optimization',
        'dayparting': '🕐 Dayparting Optimization',
        'fatigue': '🎨 Creative Fatigue (Meta)',
        'clustering': '🔤 Keyword Clustering (ML)',
        'keywords': '🔑 Keyword Performance',
        'cannibalization': '🔄 SEO/PPC Cannibalization',
        'anomaly': '🚨 Anomaly Detection',
        'forecasting': '📈 Conversion Forecasting'
    }
    
    selected = st.selectbox("Select Analysis", list(analysis_options.keys()),
                            format_func=lambda x: analysis_options[x])
    
    st.divider()
    
    # Render selected analysis
    if selected == 'quadrant':
        render_campaign_efficiency_quadrant(duckdb_path, start_date, end_date)
    elif selected == 'budget':
        render_budget_optimization(duckdb_path, start_date, end_date)
    elif selected == 'dayparting':
        render_dayparting_optimization(duckdb_path, start_date, end_date)
    elif selected == 'fatigue':
        render_creative_fatigue_analysis(duckdb_path, start_date, end_date)
    elif selected == 'clustering':
        render_keyword_clustering(duckdb_path, start_date, end_date)
    elif selected == 'keywords':
        render_keyword_analysis(duckdb_path, start_date, end_date)
    elif selected == 'cannibalization':
        render_seo_ppc_cannibalization(duckdb_path, start_date, end_date)
    elif selected == 'anomaly':
        render_anomaly_detection(duckdb_path, start_date, end_date)
    elif selected == 'forecasting':
        render_conversion_forecasting(duckdb_path, start_date, end_date)
    
    # Footer
    st.divider()
    with st.expander("📖 All Available Analyses"):
        st.markdown("""
        | Category | Analysis | Description |
        |----------|----------|-------------|
        | **Performance** | Campaign Quadrant | Visual campaign classification |
        | **Performance** | Budget Optimization | Optimal spend allocation |
        | **Temporal** | Dayparting | Best hours for ads |
        | **Temporal** | Creative Fatigue | Meta ad decay detection |
        | **Keywords** | Clustering (ML) | Group similar keywords |
        | **Keywords** | Performance | Wasted spend & opportunities |
        | **Keywords** | Cannibalization | SEO/PPC overlap |
        | **Predictive** | Anomaly Detection | Unusual pattern alerts |
        | **Predictive** | Forecasting | Predict future conversions |
        """)
