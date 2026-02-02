"""
Shared utilities for rs_analytics scripts.

This package provides common functionality for ETL scripts:
- cli: Command-line argument parsing
- db: Database (DuckDB) operations
- test_helpers: Connection testing utilities
"""

from scripts.utils.cli import (
    create_etl_parser,
    get_date_range_from_args,
    setup_script_logging,
)
from scripts.utils.db import (
    load_to_duckdb,
    load_dataframe_to_duckdb,
    get_table_row_count,
)
from scripts.utils.test_helpers import (
    print_header,
    print_success,
    print_error,
    print_info,
    print_warning,
    TestResult,
)

__all__ = [
    # CLI utilities
    'create_etl_parser',
    'get_date_range_from_args',
    'setup_script_logging',
    # Database utilities
    'load_to_duckdb',
    'load_dataframe_to_duckdb',
    'get_table_row_count',
    # Test helpers
    'print_header',
    'print_success',
    'print_error',
    'print_info',
    'print_warning',
    'TestResult',
]
