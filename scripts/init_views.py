#!/usr/bin/env python3
"""
Initialize Database Views for rs_analytics

This script creates all silver, gold, and dimension views in the DuckDB database.
It also performs grain validation to ensure data quality.

Usage:
    python scripts/init_views.py
    python scripts/init_views.py --validate-only  # Only run validation, don't create views
    python scripts/init_views.py --drop           # Drop all views first
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.utils.data_quality import (
    GRAIN_DEFINITIONS,
    validate_grain,
    validate_all_grains,
    GrainViolation
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_duckdb_path() -> str:
    """Get DuckDB path from environment or default."""
    load_dotenv()
    return os.getenv('DUCKDB_PATH', 'data/warehouse.duckdb')


def load_views_sql(views_sql_path: str = 'data/views/schema_views.sql') -> str:
    """Load the views SQL file content."""
    path = Path(views_sql_path)
    if not path.exists():
        raise FileNotFoundError(f"Views SQL file not found: {views_sql_path}")
    
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()


def split_sql_statements(sql_content: str) -> List[str]:
    """
    Split SQL content into individual statements.
    Handles CREATE OR REPLACE VIEW statements properly.
    """
    # Remove comments (lines starting with --)
    lines = []
    for line in sql_content.split('\n'):
        stripped = line.strip()
        if not stripped.startswith('--'):
            lines.append(line)
    
    content = '\n'.join(lines)
    
    # Split on semicolons but keep the statement together
    statements = []
    current = []
    
    for line in content.split('\n'):
        current.append(line)
        if line.strip().endswith(';'):
            stmt = '\n'.join(current).strip()
            if stmt and stmt != ';':
                statements.append(stmt)
            current = []
    
    # Handle any remaining content
    if current:
        stmt = '\n'.join(current).strip()
        if stmt and stmt != ';':
            statements.append(stmt)
    
    return statements


def get_existing_views(conn: duckdb.DuckDBPyConnection) -> List[str]:
    """Get list of existing views in the database."""
    try:
        result = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_type = 'VIEW'
        """).fetchall()
        return [row[0] for row in result]
    except Exception:
        return []


def drop_all_views(conn: duckdb.DuckDBPyConnection) -> int:
    """Drop all views (useful for clean recreation)."""
    views = get_existing_views(conn)
    dropped = 0
    
    for view in views:
        try:
            conn.execute(f'DROP VIEW IF EXISTS "{view}"')
            logger.info(f"  Dropped view: {view}")
            dropped += 1
        except Exception as e:
            logger.warning(f"  Could not drop view {view}: {e}")
    
    return dropped


def create_views(
    duckdb_path: str,
    views_sql_path: str = 'data/views/schema_views.sql',
    drop_existing: bool = False
) -> Tuple[int, int, List[str]]:
    """
    Create all views from the SQL file.
    
    Args:
        duckdb_path: Path to DuckDB database
        views_sql_path: Path to SQL file with view definitions
        drop_existing: If True, drop existing views first
    
    Returns:
        Tuple of (views_created, views_failed, list of error messages)
    """
    logger.info(f"Loading views from: {views_sql_path}")
    
    # Load SQL content
    sql_content = load_views_sql(views_sql_path)
    statements = split_sql_statements(sql_content)
    
    # Filter to only CREATE VIEW statements
    view_statements = [
        s for s in statements 
        if 'CREATE OR REPLACE VIEW' in s.upper() or 'CREATE VIEW' in s.upper()
    ]
    
    logger.info(f"Found {len(view_statements)} view definitions")
    
    # Connect to database
    conn = duckdb.connect(duckdb_path)
    
    try:
        # Optionally drop existing views
        if drop_existing:
            logger.info("Dropping existing views...")
            dropped = drop_all_views(conn)
            logger.info(f"Dropped {dropped} existing views")
        
        # Create views
        created = 0
        failed = 0
        errors = []
        
        logger.info("Creating views...")
        for stmt in view_statements:
            # Extract view name for logging
            view_name = "unknown"
            upper_stmt = stmt.upper()
            if 'CREATE OR REPLACE VIEW' in upper_stmt:
                start = upper_stmt.find('VIEW') + 5
                end = stmt.find(' AS', start)
                if end > start:
                    view_name = stmt[start:end].strip()
            
            try:
                conn.execute(stmt)
                logger.info(f"  Created view: {view_name}")
                created += 1
            except Exception as e:
                error_msg = f"Failed to create view {view_name}: {e}"
                logger.error(f"  {error_msg}")
                errors.append(error_msg)
                failed += 1
        
        return created, failed, errors
    
    finally:
        conn.close()


def run_validation(duckdb_path: str) -> Tuple[int, int, List[GrainViolation]]:
    """
    Run grain validation on all tables.
    
    Returns:
        Tuple of (tables_checked, violations_found, list of violations)
    """
    logger.info("Running grain validation...")
    
    conn = duckdb.connect(duckdb_path, read_only=True)
    
    try:
        # Check which tables exist
        existing_tables = set()
        tables_result = conn.execute("SHOW TABLES").fetchall()
        for row in tables_result:
            existing_tables.add(row[0])
        
        violations = []
        tables_checked = 0
        
        for table_name, key_columns in GRAIN_DEFINITIONS.items():
            if table_name not in existing_tables:
                logger.warning(f"  Table {table_name} not found, skipping")
                continue
            
            logger.info(f"  Checking grain for {table_name}...")
            table_violations = validate_grain(conn, table_name, key_columns)
            violations.extend(table_violations)
            tables_checked += 1
            
            if table_violations:
                logger.warning(f"    Found {len(table_violations)} grain violations")
            else:
                logger.info(f"    Grain valid (unique on: {', '.join(key_columns)})")
        
        return tables_checked, len(violations), violations
    
    finally:
        conn.close()


def print_summary(
    views_created: int,
    views_failed: int,
    tables_checked: int,
    violations_found: int,
    violations: List[GrainViolation]
) -> None:
    """Print a summary of the initialization results."""
    print("\n" + "=" * 60)
    print("VIEW INITIALIZATION SUMMARY")
    print("=" * 60)
    
    print(f"\nViews:")
    print(f"  Created: {views_created}")
    print(f"  Failed:  {views_failed}")
    
    print(f"\nGrain Validation:")
    print(f"  Tables checked: {tables_checked}")
    print(f"  Violations found: {violations_found}")
    
    if violations:
        print(f"\nGrain Violations (first 10):")
        for v in violations[:10]:
            print(f"  - {v.table_name}: {v.duplicate_count} duplicates on {v.key_columns}")
            if v.sample_keys:
                print(f"    Sample: {v.sample_keys[0]}")
    
    print("\n" + "=" * 60)
    
    if views_failed == 0 and violations_found == 0:
        print("SUCCESS: All views created and grains validated!")
    elif views_failed > 0:
        print("WARNING: Some views failed to create. Check errors above.")
    elif violations_found > 0:
        print("WARNING: Grain violations detected. Data quality issues exist.")
    
    print("=" * 60 + "\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Initialize database views for rs_analytics'
    )
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only run grain validation, skip view creation'
    )
    parser.add_argument(
        '--drop',
        action='store_true',
        help='Drop existing views before recreation'
    )
    parser.add_argument(
        '--db-path',
        type=str,
        default=None,
        help='Path to DuckDB database (default: from env or data/warehouse.duckdb)'
    )
    parser.add_argument(
        '--views-sql',
        type=str,
        default='data/views/schema_views.sql',
        help='Path to views SQL file'
    )
    
    args = parser.parse_args()
    
    # Get database path
    duckdb_path = args.db_path or get_duckdb_path()
    
    logger.info(f"Database: {duckdb_path}")
    logger.info(f"Views SQL: {args.views_sql}")
    
    views_created = 0
    views_failed = 0
    errors = []
    
    # Create views (unless validate-only)
    if not args.validate_only:
        try:
            views_created, views_failed, errors = create_views(
                duckdb_path=duckdb_path,
                views_sql_path=args.views_sql,
                drop_existing=args.drop
            )
        except FileNotFoundError as e:
            logger.error(str(e))
            sys.exit(1)
    else:
        logger.info("Skipping view creation (--validate-only)")
    
    # Run validation
    tables_checked, violations_found, violations = run_validation(duckdb_path)
    
    # Print summary
    print_summary(
        views_created=views_created,
        views_failed=views_failed,
        tables_checked=tables_checked,
        violations_found=violations_found,
        violations=violations
    )
    
    # Exit with error code if there were failures
    if views_failed > 0 or violations_found > 0:
        sys.exit(1)
    
    sys.exit(0)


if __name__ == '__main__':
    main()
