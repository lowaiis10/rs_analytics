"""
Analysis Module for rs_analytics

This module provides machine learning models and insight generation:
- Anomaly detection for metrics
- Trend analysis and forecasting
- Channel attribution modeling
- Automated insight generation
- Performance recommendations

Submodules:
- models: ML model definitions and training
- insights: Automated insight generators
- utils: Analysis utility functions

Usage:
    from analysis.insights import generate_daily_insights
    from analysis.models import AnomalyDetector, TrendForecaster
    
    # Generate insights from latest data
    insights = generate_daily_insights(duckdb_path)
    
    # Detect anomalies in metrics
    detector = AnomalyDetector()
    anomalies = detector.detect(metric_data)

Note: This module requires additional ML dependencies:
    pip install scikit-learn>=1.3.0 prophet>=1.1.0
"""

# Version of the analysis module
__version__ = '0.1.0'

# Placeholder imports - will be populated as modules are developed
# from analysis.insights import generate_daily_insights, InsightGenerator
# from analysis.models import AnomalyDetector, TrendForecaster
