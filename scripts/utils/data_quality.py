"""
Data Quality Utilities for rs_analytics

This module provides data quality validation functions:
- Grain validation (checking uniqueness constraints)
- Duplicate detection
- Data quality reporting

Usage:
    from scripts.utils.data_quality import validate_grain, GRAIN_DEFINITIONS
    
    # Check if a table has unique rows based on its grain
    violations = validate_grain(conn, 'gads_campaigns', ['date', 'campaign_id'])
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import duckdb

logger = logging.getLogger(__name__)


# ============================================
# Grain Definitions
# ============================================
# These define the expected uniqueness constraints for each table.
# Each table should have exactly one row per combination of these columns.

GRAIN_DEFINITIONS: Dict[str, List[str]] = {
    # Google Ads tables
    'gads_daily_summary': ['date', 'campaign_id'],
    'gads_campaigns': ['date', 'campaign_id'],
    'gads_ad_groups': ['date', 'campaign_id', 'ad_group_id'],
    'gads_keywords': ['date', 'campaign_id', 'ad_group_id', 'keyword_id'],
    'gads_ads': ['date', 'campaign_id', 'ad_group_id', 'ad_id'],
    'gads_devices': ['date', 'campaign_id', 'device'],
    'gads_geographic': ['date', 'campaign_id', 'country_criterion_id'],
    'gads_hourly': ['date', 'campaign_id', 'hour'],
    'gads_conversions': ['date', 'campaign_id', 'conversion_action'],
    
    # Google Search Console tables
    'gsc_queries': ['_dataset', 'date', 'query'],
    'gsc_pages': ['_dataset', 'date', 'page'],
    'gsc_countries': ['_dataset', 'date', 'country'],
    'gsc_devices': ['_dataset', 'date', 'device'],
    'gsc_query_page': ['_dataset', 'query', 'page'],
    'gsc_query_country': ['_dataset', 'query', 'country'],
    'gsc_query_device': ['_dataset', 'query', 'device'],
    'gsc_page_country': ['_dataset', 'page', 'country'],
    'gsc_page_device': ['_dataset', 'page', 'device'],
    'gsc_daily_totals': ['_dataset', 'date'],
    
    # Meta Ads tables
    'meta_daily_account': ['date', 'ad_account_id'],
    'meta_campaigns': ['campaign_id'],
    'meta_campaign_insights': ['date', 'ad_account_id', 'campaign_id'],
    'meta_adsets': ['adset_id'],
    'meta_adset_insights': ['date', 'ad_account_id', 'adset_id'],
    'meta_ads': ['ad_id'],
    'meta_ad_insights': ['date', 'ad_account_id', 'ad_id'],
    'meta_geographic': ['date_start', 'ad_account_id', 'country'],
    'meta_devices': ['date_start', 'ad_account_id', 'device_platform', 'publisher_platform'],
    'meta_demographics': ['date_start', 'ad_account_id', 'age', 'gender'],
    
    # GA4 tables
    'ga4_sessions': ['date'],
    'ga4_traffic_overview': ['date', 'sessionSource', 'sessionMedium', 'sessionCampaignName'],
    'ga4_page_performance': ['date', 'pagePath'],
    'ga4_geographic_data': ['date', 'country', 'region', 'city'],
    'ga4_technology_data': ['date', 'deviceCategory', 'operatingSystem', 'browser'],
    'ga4_event_data': ['date', 'eventName'],
    
    # Twitter tables
    'twitter_profile': ['user_id', 'snapshot_date'],
    'twitter_tweets': ['tweet_id'],
    'twitter_daily_metrics': ['date', 'username'],
}


@dataclass
class GrainViolation:
    """Represents a grain violation (duplicate rows)."""
    table_name: str
    key_columns: List[str]
    duplicate_count: int
    sample_keys: List[Dict[str, Any]]  # Sample of duplicate key combinations


def validate_grain(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    key_columns: List[str],
    sample_limit: int = 5
) -> List[GrainViolation]:
    """
    Validate that a table has unique rows based on the specified key columns.
    
    Args:
        conn: DuckDB connection
        table_name: Name of the table to validate
        key_columns: List of columns that should uniquely identify each row
        sample_limit: Max number of sample duplicates to return
    
    Returns:
        List of GrainViolation objects (empty if grain is valid)
    """
    violations = []
    
    # Build the query to find duplicates
    key_cols_str = ', '.join(f'"{col}"' for col in key_columns)
    
    # Query to count duplicates
    count_query = f"""
        SELECT 
            {key_cols_str},
            COUNT(*) as row_count
        FROM "{table_name}"
        GROUP BY {key_cols_str}
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC
        LIMIT {sample_limit}
    """
    
    try:
        result = conn.execute(count_query).fetchall()
        
        if result:
            # Get column names from description
            columns = key_columns + ['row_count']
            
            # Calculate total duplicates
            total_query = f"""
                SELECT COUNT(*) as dup_groups, SUM(cnt - 1) as extra_rows
                FROM (
                    SELECT {key_cols_str}, COUNT(*) as cnt
                    FROM "{table_name}"
                    GROUP BY {key_cols_str}
                    HAVING COUNT(*) > 1
                )
            """
            total_result = conn.execute(total_query).fetchone()
            dup_groups = total_result[0] if total_result else 0
            extra_rows = total_result[1] if total_result else 0
            
            # Build sample keys
            sample_keys = []
            for row in result:
                key_dict = {col: row[i] for i, col in enumerate(key_columns)}
                key_dict['_duplicate_count'] = row[-1]
                sample_keys.append(key_dict)
            
            violations.append(GrainViolation(
                table_name=table_name,
                key_columns=key_columns,
                duplicate_count=int(extra_rows) if extra_rows else 0,
                sample_keys=sample_keys
            ))
    
    except Exception as e:
        logger.error(f"Error validating grain for {table_name}: {e}")
        # Return a violation indicating the check failed
        violations.append(GrainViolation(
            table_name=table_name,
            key_columns=key_columns,
            duplicate_count=-1,  # -1 indicates check failed
            sample_keys=[{'_error': str(e)}]
        ))
    
    return violations


def validate_all_grains(
    conn: duckdb.DuckDBPyConnection,
    tables: Optional[List[str]] = None
) -> Tuple[int, int, List[GrainViolation]]:
    """
    Validate grains for multiple tables.
    
    Args:
        conn: DuckDB connection
        tables: List of tables to validate (default: all in GRAIN_DEFINITIONS)
    
    Returns:
        Tuple of (tables_checked, total_violations, list of violations)
    """
    if tables is None:
        tables = list(GRAIN_DEFINITIONS.keys())
    
    all_violations = []
    tables_checked = 0
    
    for table_name in tables:
        if table_name not in GRAIN_DEFINITIONS:
            logger.warning(f"No grain definition for table: {table_name}")
            continue
        
        key_columns = GRAIN_DEFINITIONS[table_name]
        violations = validate_grain(conn, table_name, key_columns)
        all_violations.extend(violations)
        tables_checked += 1
    
    return tables_checked, len(all_violations), all_violations


def get_row_counts(conn: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    """Get row counts for all tables."""
    counts = {}
    
    try:
        tables = conn.execute("SHOW TABLES").fetchall()
        for row in tables:
            table_name = row[0]
            try:
                count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
                counts[table_name] = count
            except Exception as e:
                logger.warning(f"Could not count rows in {table_name}: {e}")
                counts[table_name] = -1
    except Exception as e:
        logger.error(f"Error getting table list: {e}")
    
    return counts


def check_null_keys(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    key_columns: List[str]
) -> Dict[str, int]:
    """
    Check for NULL values in key columns.
    
    Returns:
        Dict mapping column name to count of NULL values
    """
    null_counts = {}
    
    for col in key_columns:
        try:
            query = f'SELECT COUNT(*) FROM "{table_name}" WHERE "{col}" IS NULL'
            count = conn.execute(query).fetchone()[0]
            if count > 0:
                null_counts[col] = count
        except Exception as e:
            logger.warning(f"Could not check NULLs in {table_name}.{col}: {e}")
    
    return null_counts


def generate_dq_report(
    conn: duckdb.DuckDBPyConnection,
    output_format: str = 'text'
) -> str:
    """
    Generate a data quality report.
    
    Args:
        conn: DuckDB connection
        output_format: 'text' or 'markdown'
    
    Returns:
        Formatted report string
    """
    lines = []
    
    if output_format == 'markdown':
        lines.append("# Data Quality Report\n")
    else:
        lines.append("=" * 60)
        lines.append("DATA QUALITY REPORT")
        lines.append("=" * 60)
    
    # Row counts
    if output_format == 'markdown':
        lines.append("\n## Table Row Counts\n")
        lines.append("| Table | Rows |")
        lines.append("|-------|------|")
    else:
        lines.append("\nTable Row Counts:")
        lines.append("-" * 40)
    
    row_counts = get_row_counts(conn)
    for table, count in sorted(row_counts.items()):
        if output_format == 'markdown':
            lines.append(f"| {table} | {count:,} |")
        else:
            lines.append(f"  {table}: {count:,}")
    
    # Grain validation
    if output_format == 'markdown':
        lines.append("\n## Grain Validation\n")
    else:
        lines.append("\nGrain Validation:")
        lines.append("-" * 40)
    
    tables_checked, violations, violation_list = validate_all_grains(conn)
    
    if output_format == 'markdown':
        lines.append(f"- Tables checked: {tables_checked}")
        lines.append(f"- Violations found: {violations}")
    else:
        lines.append(f"  Tables checked: {tables_checked}")
        lines.append(f"  Violations found: {violations}")
    
    if violation_list:
        if output_format == 'markdown':
            lines.append("\n### Violations\n")
            for v in violation_list:
                lines.append(f"- **{v.table_name}**: {v.duplicate_count} duplicate rows")
                lines.append(f"  - Key columns: {', '.join(v.key_columns)}")
        else:
            lines.append("\n  Violations:")
            for v in violation_list:
                lines.append(f"    - {v.table_name}: {v.duplicate_count} duplicates on {v.key_columns}")
    
    return "\n".join(lines)
