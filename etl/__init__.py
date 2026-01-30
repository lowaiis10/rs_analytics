"""
ETL Package for rs_analytics

This package contains:
- config.py: Central configuration loader with validation
- Additional ETL modules for data extraction and transformation
"""

from etl.config import Config, ConfigurationError, get_config

__all__ = ["Config", "ConfigurationError", "get_config"]
