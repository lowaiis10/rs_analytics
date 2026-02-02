"""
Automated Insight Generation for rs_analytics

This submodule generates actionable insights from marketing data:
- Daily performance summaries
- Trend alerts and notifications
- Optimization recommendations
- Cross-channel analysis

Available Generators:
- DailyInsightGenerator: Generate daily insight reports
- TrendAlertGenerator: Detect and alert on significant trends
- RecommendationEngine: Generate optimization recommendations

Usage:
    from analysis.insights import generate_daily_insights
    
    insights = generate_daily_insights(
        duckdb_path="data/warehouse.duckdb",
        lookback_days=7
    )
    
    for insight in insights:
        print(f"[{insight.priority}] {insight.title}")
        print(f"  {insight.description}")
"""

from analysis.insights.generators import (
    Insight,
    InsightType,
    InsightPriority,
    generate_daily_insights,
    DailyInsightGenerator,
)

__all__ = [
    'Insight',
    'InsightType',
    'InsightPriority',
    'generate_daily_insights',
    'DailyInsightGenerator',
]
