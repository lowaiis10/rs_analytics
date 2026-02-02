"""
Insight Generators for rs_analytics

This module provides automated insight generation from marketing data.

Features:
- Performance change detection (significant increases/decreases)
- Trend identification (upward/downward/stable)
- Anomaly alerts
- Cross-channel comparisons
- Optimization recommendations

Usage:
    from analysis.insights.generators import generate_daily_insights
    
    insights = generate_daily_insights("data/warehouse.duckdb")
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd


logger = logging.getLogger(__name__)


class InsightType(Enum):
    """Types of insights that can be generated."""
    PERFORMANCE_CHANGE = "performance_change"
    TREND = "trend"
    ANOMALY = "anomaly"
    COMPARISON = "comparison"
    RECOMMENDATION = "recommendation"
    MILESTONE = "milestone"


class InsightPriority(Enum):
    """Priority levels for insights."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


@dataclass
class Insight:
    """
    Represents a single insight or finding.
    
    Attributes:
        id: Unique identifier
        type: Type of insight
        priority: Priority level
        title: Short title
        description: Detailed description
        source: Data source (ga4, gads, etc.)
        metric: Related metric name
        value: Current value
        change: Change percentage (if applicable)
        recommendation: Suggested action (if applicable)
        data: Additional data for visualization
        created_at: When insight was generated
    """
    id: str
    type: InsightType
    priority: InsightPriority
    title: str
    description: str
    source: str
    metric: Optional[str] = None
    value: Optional[float] = None
    change: Optional[float] = None
    recommendation: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert insight to dictionary."""
        return {
            'id': self.id,
            'type': self.type.value,
            'priority': self.priority.value,
            'title': self.title,
            'description': self.description,
            'source': self.source,
            'metric': self.metric,
            'value': self.value,
            'change': self.change,
            'recommendation': self.recommendation,
            'data': self.data,
            'created_at': self.created_at.isoformat(),
        }


class DailyInsightGenerator:
    """
    Generates daily insights from marketing data.
    
    Analyzes:
    - Day-over-day performance changes
    - Week-over-week trends
    - Anomalies in key metrics
    - Cross-source performance
    """
    
    # Thresholds for insight generation
    SIGNIFICANT_CHANGE_THRESHOLD = 0.20  # 20% change
    HIGH_CHANGE_THRESHOLD = 0.50  # 50% change
    CRITICAL_CHANGE_THRESHOLD = 0.80  # 80% change
    
    def __init__(self, duckdb_path: Union[str, Path]):
        """
        Initialize the insight generator.
        
        Args:
            duckdb_path: Path to DuckDB database
        """
        self.duckdb_path = Path(duckdb_path)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def generate(self, lookback_days: int = 7) -> List[Insight]:
        """
        Generate insights for the specified period.
        
        Args:
            lookback_days: Number of days to analyze
            
        Returns:
            List of generated insights
        """
        insights = []
        
        try:
            import duckdb
            conn = duckdb.connect(str(self.duckdb_path), read_only=True)
            
            # Get list of available tables
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
            table_names = [t[0] for t in tables]
            
            # Generate insights for each source
            if any('gads_' in t for t in table_names):
                insights.extend(self._analyze_gads(conn, lookback_days))
            
            if any('gsc_' in t for t in table_names):
                insights.extend(self._analyze_gsc(conn, lookback_days))
            
            if any('ga4_' in t for t in table_names):
                insights.extend(self._analyze_ga4(conn, lookback_days))
            
            if any('meta_' in t for t in table_names):
                insights.extend(self._analyze_meta(conn, lookback_days))
            
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Failed to generate insights: {e}")
        
        # Sort by priority
        priority_order = {
            InsightPriority.CRITICAL: 0,
            InsightPriority.HIGH: 1,
            InsightPriority.MEDIUM: 2,
            InsightPriority.LOW: 3,
            InsightPriority.INFO: 4,
        }
        insights.sort(key=lambda x: priority_order.get(x.priority, 5))
        
        return insights
    
    def _analyze_gads(self, conn, lookback_days: int) -> List[Insight]:
        """Analyze Google Ads data for insights."""
        insights = []
        
        try:
            # Get daily spend and conversions trend
            query = """
                SELECT 
                    date,
                    SUM(cost) as total_cost,
                    SUM(clicks) as total_clicks,
                    SUM(conversions) as total_conversions,
                    SUM(impressions) as total_impressions
                FROM gads_daily_summary
                WHERE date >= CURRENT_DATE - INTERVAL ? DAY
                GROUP BY date
                ORDER BY date
            """
            
            df = conn.execute(query, [lookback_days]).fetchdf()
            
            if len(df) >= 2:
                # Calculate changes
                latest = df.iloc[-1]
                previous = df.iloc[-2]
                
                # Check spend change
                if previous['total_cost'] > 0:
                    spend_change = (latest['total_cost'] - previous['total_cost']) / previous['total_cost']
                    
                    if abs(spend_change) >= self.CRITICAL_CHANGE_THRESHOLD:
                        direction = "increased" if spend_change > 0 else "decreased"
                        insights.append(Insight(
                            id=f"gads_spend_{datetime.now().strftime('%Y%m%d')}",
                            type=InsightType.PERFORMANCE_CHANGE,
                            priority=InsightPriority.CRITICAL,
                            title=f"Google Ads spend {direction} by {abs(spend_change)*100:.1f}%",
                            description=f"Daily spend changed from ${previous['total_cost']:.2f} to ${latest['total_cost']:.2f}",
                            source="gads",
                            metric="cost",
                            value=float(latest['total_cost']),
                            change=spend_change,
                            recommendation="Review campaign budgets and bid strategies"
                        ))
                
                # Check conversion change
                if previous['total_conversions'] > 0:
                    conv_change = (latest['total_conversions'] - previous['total_conversions']) / previous['total_conversions']
                    
                    if conv_change <= -self.SIGNIFICANT_CHANGE_THRESHOLD:
                        insights.append(Insight(
                            id=f"gads_conversions_{datetime.now().strftime('%Y%m%d')}",
                            type=InsightType.PERFORMANCE_CHANGE,
                            priority=InsightPriority.HIGH,
                            title=f"Google Ads conversions dropped by {abs(conv_change)*100:.1f}%",
                            description=f"Conversions decreased from {previous['total_conversions']:.0f} to {latest['total_conversions']:.0f}",
                            source="gads",
                            metric="conversions",
                            value=float(latest['total_conversions']),
                            change=conv_change,
                            recommendation="Check landing page performance and tracking setup"
                        ))
                    elif conv_change >= self.SIGNIFICANT_CHANGE_THRESHOLD:
                        insights.append(Insight(
                            id=f"gads_conversions_{datetime.now().strftime('%Y%m%d')}",
                            type=InsightType.PERFORMANCE_CHANGE,
                            priority=InsightPriority.MEDIUM,
                            title=f"Google Ads conversions increased by {conv_change*100:.1f}%",
                            description=f"Conversions increased from {previous['total_conversions']:.0f} to {latest['total_conversions']:.0f}",
                            source="gads",
                            metric="conversions",
                            value=float(latest['total_conversions']),
                            change=conv_change,
                        ))
                        
        except Exception as e:
            self.logger.warning(f"Failed to analyze Google Ads data: {e}")
        
        return insights
    
    def _analyze_gsc(self, conn, lookback_days: int) -> List[Insight]:
        """Analyze GSC data for insights."""
        insights = []
        
        try:
            # Get daily clicks and impressions
            query = """
                SELECT 
                    date,
                    SUM(clicks) as total_clicks,
                    SUM(impressions) as total_impressions,
                    AVG(position) as avg_position
                FROM gsc_daily_totals
                WHERE date >= CURRENT_DATE - INTERVAL ? DAY
                GROUP BY date
                ORDER BY date
            """
            
            df = conn.execute(query, [lookback_days]).fetchdf()
            
            if len(df) >= 7:
                # Compare this week vs last week
                this_week = df.tail(7)['total_clicks'].sum()
                last_week = df.head(7)['total_clicks'].sum()
                
                if last_week > 0:
                    week_change = (this_week - last_week) / last_week
                    
                    if abs(week_change) >= self.SIGNIFICANT_CHANGE_THRESHOLD:
                        direction = "up" if week_change > 0 else "down"
                        priority = InsightPriority.HIGH if abs(week_change) >= self.HIGH_CHANGE_THRESHOLD else InsightPriority.MEDIUM
                        
                        insights.append(Insight(
                            id=f"gsc_weekly_{datetime.now().strftime('%Y%m%d')}",
                            type=InsightType.TREND,
                            priority=priority,
                            title=f"Organic search clicks {direction} {abs(week_change)*100:.1f}% week-over-week",
                            description=f"Weekly clicks changed from {last_week:,.0f} to {this_week:,.0f}",
                            source="gsc",
                            metric="clicks",
                            value=float(this_week),
                            change=week_change,
                        ))
                        
        except Exception as e:
            self.logger.warning(f"Failed to analyze GSC data: {e}")
        
        return insights
    
    def _analyze_ga4(self, conn, lookback_days: int) -> List[Insight]:
        """Analyze GA4 data for insights."""
        insights = []
        # Placeholder - implement GA4 analysis
        return insights
    
    def _analyze_meta(self, conn, lookback_days: int) -> List[Insight]:
        """Analyze Meta Ads data for insights."""
        insights = []
        
        try:
            query = """
                SELECT 
                    date,
                    SUM(spend) as total_spend,
                    SUM(impressions) as total_impressions,
                    SUM(clicks) as total_clicks,
                    SUM(app_installs) as total_installs
                FROM meta_daily_account
                WHERE date >= CURRENT_DATE - INTERVAL ? DAY
                GROUP BY date
                ORDER BY date
            """
            
            df = conn.execute(query, [lookback_days]).fetchdf()
            
            if len(df) >= 2 and 'total_spend' in df.columns:
                latest = df.iloc[-1]
                previous = df.iloc[-2]
                
                # Check CPI trend if installs exist
                if 'total_installs' in df.columns:
                    if latest['total_installs'] > 0 and previous['total_installs'] > 0:
                        latest_cpi = latest['total_spend'] / latest['total_installs']
                        previous_cpi = previous['total_spend'] / previous['total_installs']
                        
                        cpi_change = (latest_cpi - previous_cpi) / previous_cpi
                        
                        if cpi_change >= self.SIGNIFICANT_CHANGE_THRESHOLD:
                            insights.append(Insight(
                                id=f"meta_cpi_{datetime.now().strftime('%Y%m%d')}",
                                type=InsightType.PERFORMANCE_CHANGE,
                                priority=InsightPriority.HIGH,
                                title=f"Meta Ads CPI increased by {cpi_change*100:.1f}%",
                                description=f"Cost per install rose from ${previous_cpi:.2f} to ${latest_cpi:.2f}",
                                source="meta",
                                metric="cpi",
                                value=latest_cpi,
                                change=cpi_change,
                                recommendation="Review targeting and creative performance"
                            ))
                            
        except Exception as e:
            self.logger.warning(f"Failed to analyze Meta Ads data: {e}")
        
        return insights


def generate_daily_insights(
    duckdb_path: Union[str, Path],
    lookback_days: int = 7
) -> List[Insight]:
    """
    Generate daily insights from marketing data.
    
    Convenience function that creates a DailyInsightGenerator
    and generates insights.
    
    Args:
        duckdb_path: Path to DuckDB database
        lookback_days: Number of days to analyze
        
    Returns:
        List of Insight objects
        
    Example:
        insights = generate_daily_insights("data/warehouse.duckdb")
        
        for insight in insights:
            print(f"[{insight.priority.value}] {insight.title}")
    """
    generator = DailyInsightGenerator(duckdb_path)
    return generator.generate(lookback_days)
