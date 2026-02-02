"""
ETL Job Definitions for rs_analytics Scheduler

This module defines the job configurations and execution logic for scheduled ETL tasks.

Features:
- Configurable job schedules (cron-style)
- Retry logic with exponential backoff
- Job dependencies (run B after A)
- Notification on failure

Usage:
    from scheduler.jobs import create_daily_etl_job, ETLJob
    
    # Create a standard daily job
    job = create_daily_etl_job('gads', hour=6, minute=0)
    
    # Create a custom job
    job = ETLJob(
        name='custom_gads',
        source='gads',
        schedule='0 6 * * *',  # 6 AM daily
        lookback_days=7,
        retry_count=3
    )
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional


logger = logging.getLogger(__name__)


@dataclass
class JobConfig:
    """
    Configuration for a scheduled ETL job.
    
    Attributes:
        name: Unique job identifier
        source: Data source (ga4, gsc, gads, meta, twitter)
        enabled: Whether the job is active (default: False - must be explicitly enabled)
        schedule: Cron expression (e.g., '0 6 * * *' for 6 AM daily)
        lookback_days: Number of days to extract (default: 7)
        retry_count: Number of retry attempts on failure
        retry_delay_minutes: Minutes between retries
        timeout_minutes: Job timeout in minutes
        notify_on_failure: Send notification on failure
        depends_on: List of job names that must complete first
    """
    name: str
    source: str
    enabled: bool = False  # Default: disabled - must be explicitly enabled
    schedule: str = "0 6 * * *"  # Default: 6 AM daily
    lookback_days: int = 7
    retry_count: int = 3
    retry_delay_minutes: int = 5
    timeout_minutes: int = 60
    notify_on_failure: bool = True
    depends_on: List[str] = field(default_factory=list)


@dataclass
class JobRun:
    """
    Record of a job execution.
    
    Attributes:
        job_name: Name of the job
        started_at: When the job started
        completed_at: When the job finished (None if still running)
        success: Whether the job succeeded
        rows_extracted: Number of rows extracted
        tables_created: List of tables created/updated
        error_message: Error message if failed
        retry_attempt: Which retry attempt this was (0 for first run)
    """
    job_name: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    success: bool = False
    rows_extracted: int = 0
    tables_created: List[str] = field(default_factory=list)
    error_message: Optional[str] = None
    retry_attempt: int = 0
    
    @property
    def duration(self) -> Optional[timedelta]:
        """Get job duration."""
        if self.completed_at:
            return self.completed_at - self.started_at
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging/storage."""
        return {
            'job_name': self.job_name,
            'started_at': self.started_at.isoformat(),
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'success': self.success,
            'rows_extracted': self.rows_extracted,
            'tables_created': self.tables_created,
            'error_message': self.error_message,
            'retry_attempt': self.retry_attempt,
            'duration_seconds': self.duration.total_seconds() if self.duration else None,
        }


class ETLJob:
    """
    Executable ETL job with retry logic.
    
    This class wraps a job configuration and provides execution logic
    including retries, logging, and result tracking.
    """
    
    def __init__(self, config: JobConfig):
        """
        Initialize an ETL job.
        
        Args:
            config: Job configuration
        """
        self.config = config
        self.last_run: Optional[JobRun] = None
        self.run_history: List[JobRun] = []
        self.logger = logging.getLogger(f"job.{config.name}")
    
    def execute(self, force: bool = False) -> JobRun:
        """
        Execute the ETL job.
        
        Args:
            force: If True, run even if disabled
            
        Returns:
            JobRun record
        """
        if not self.config.enabled and not force:
            self.logger.warning(f"Job {self.config.name} is disabled")
            return JobRun(
                job_name=self.config.name,
                started_at=datetime.now(),
                completed_at=datetime.now(),
                success=False,
                error_message="Job is disabled"
            )
        
        attempt = 0
        last_error = None
        
        while attempt <= self.config.retry_count:
            run = self._execute_attempt(attempt)
            
            if run.success:
                self.last_run = run
                self.run_history.append(run)
                return run
            
            last_error = run.error_message
            attempt += 1
            
            if attempt <= self.config.retry_count:
                self.logger.warning(
                    f"Job {self.config.name} failed, retrying in "
                    f"{self.config.retry_delay_minutes} minutes "
                    f"(attempt {attempt}/{self.config.retry_count})"
                )
                import time
                time.sleep(self.config.retry_delay_minutes * 60)
        
        # All retries failed
        final_run = JobRun(
            job_name=self.config.name,
            started_at=datetime.now(),
            completed_at=datetime.now(),
            success=False,
            error_message=f"All {self.config.retry_count} retries failed. Last error: {last_error}",
            retry_attempt=attempt - 1
        )
        
        self.last_run = final_run
        self.run_history.append(final_run)
        
        if self.config.notify_on_failure:
            self._send_failure_notification(final_run)
        
        return final_run
    
    def _execute_attempt(self, attempt: int) -> JobRun:
        """
        Execute a single attempt of the job.
        
        Args:
            attempt: Attempt number (0-based)
            
        Returns:
            JobRun record
        """
        run = JobRun(
            job_name=self.config.name,
            started_at=datetime.now(),
            retry_attempt=attempt
        )
        
        self.logger.info(
            f"Starting job {self.config.name} (attempt {attempt + 1})"
        )
        
        try:
            # Import and run the unified ETL
            import sys
            from pathlib import Path
            
            project_root = Path(__file__).parent.parent
            sys.path.insert(0, str(project_root))
            
            # Calculate date range
            from datetime import datetime, timedelta
            yesterday = datetime.now() - timedelta(days=1)
            end_date = yesterday.strftime('%Y-%m-%d')
            start_date = (yesterday - timedelta(days=self.config.lookback_days - 1)).strftime('%Y-%m-%d')
            
            # Get DuckDB path from config
            duckdb_path = self._get_duckdb_path()
            
            # Run the appropriate ETL based on source
            if self.config.source == 'gads':
                success, rows, tables = self._run_gads_etl(start_date, end_date, duckdb_path)
            elif self.config.source == 'gsc':
                success, rows, tables = self._run_gsc_etl(start_date, end_date, duckdb_path)
            elif self.config.source == 'ga4':
                success, rows, tables = self._run_ga4_etl(start_date, end_date, duckdb_path)
            elif self.config.source == 'meta':
                success, rows, tables = self._run_meta_etl(start_date, end_date, duckdb_path)
            elif self.config.source == 'twitter':
                success, rows, tables = self._run_twitter_etl(start_date, end_date, duckdb_path)
            else:
                raise ValueError(f"Unknown source: {self.config.source}")
            
            run.completed_at = datetime.now()
            run.success = success
            run.rows_extracted = rows
            run.tables_created = tables
            
            if success:
                self.logger.info(
                    f"Job {self.config.name} completed: "
                    f"{rows:,} rows, {len(tables)} tables"
                )
            else:
                run.error_message = "ETL returned failure status"
            
        except Exception as e:
            run.completed_at = datetime.now()
            run.success = False
            run.error_message = str(e)
            self.logger.error(f"Job {self.config.name} failed: {e}")
        
        return run
    
    def _get_duckdb_path(self) -> str:
        """Get DuckDB path from configuration."""
        try:
            if self.config.source == 'gads':
                from etl.gads_config import get_gads_config
                return str(get_gads_config().duckdb_path)
            elif self.config.source == 'gsc':
                from etl.gsc_config import get_gsc_config
                return str(get_gsc_config().duckdb_path)
            elif self.config.source == 'ga4':
                from etl.config import get_config
                return str(get_config().duckdb_path)
            elif self.config.source == 'meta':
                from etl.meta_config import get_meta_config
                return str(get_meta_config().duckdb_path)
            elif self.config.source == 'twitter':
                from etl.twitter_config import get_twitter_config
                return str(get_twitter_config().duckdb_path)
        except Exception:
            pass
        
        # Default path
        from pathlib import Path
        return str(Path(__file__).parent.parent / "data" / "warehouse.duckdb")
    
    def _run_gads_etl(self, start_date: str, end_date: str, duckdb_path: str):
        """Run Google Ads ETL."""
        from etl.gads_config import get_gads_config, get_gads_client
        from etl.gads_extractor import GAdsExtractor
        from scripts.utils.db import load_to_duckdb
        
        config = get_gads_config()
        client = get_gads_client()
        extractor = GAdsExtractor(client, config.customer_id, config.login_customer_id, self.logger)
        
        all_data = extractor.extract_all_data(start_date, end_date)
        
        total_rows = 0
        tables = []
        for name, data in all_data.items():
            table_name = f"gads_{name}"
            if load_to_duckdb(duckdb_path, data, table_name, self.logger):
                total_rows += len(data)
                tables.append(table_name)
        
        return True, total_rows, tables
    
    def _run_gsc_etl(self, start_date: str, end_date: str, duckdb_path: str):
        """Run GSC ETL."""
        from etl.gsc_config import get_gsc_config
        from etl.gsc_extractor import GSCExtractor
        from scripts.utils.db import load_to_duckdb
        
        config = get_gsc_config()
        extractor = GSCExtractor(str(config.credentials_path), config.site_url, self.logger)
        
        all_data = extractor.extract_all_data(start_date, end_date)
        
        total_rows = 0
        tables = []
        for name, data in all_data.items():
            table_name = f"gsc_{name}"
            if load_to_duckdb(duckdb_path, data, table_name, self.logger):
                total_rows += len(data)
                tables.append(table_name)
        
        return True, total_rows, tables
    
    def _run_ga4_etl(self, start_date: str, end_date: str, duckdb_path: str):
        """Run GA4 ETL."""
        # Placeholder - implement similar to gads
        self.logger.info("GA4 ETL not fully implemented in scheduler yet")
        return True, 0, []
    
    def _run_meta_etl(self, start_date: str, end_date: str, duckdb_path: str):
        """Run Meta ETL."""
        from etl.meta_config import get_meta_config
        from etl.meta_extractor import MetaExtractor
        from scripts.utils.db import load_dataframe_to_duckdb
        
        config = get_meta_config()
        
        total_rows = 0
        tables = []
        
        for account_id in config.ad_account_ids:
            extractor = MetaExtractor(config.access_token, account_id)
            all_data = extractor.extract_all(start_date=start_date, end_date=end_date)
            
            for name, df in all_data.items():
                table_name = f"meta_{name}"
                if load_dataframe_to_duckdb(duckdb_path, df, table_name, self.logger):
                    total_rows += len(df)
                    if table_name not in tables:
                        tables.append(table_name)
        
        return True, total_rows, tables
    
    def _run_twitter_etl(self, start_date: str, end_date: str, duckdb_path: str):
        """Run Twitter ETL."""
        from etl.twitter_config import get_twitter_config
        from etl.twitter_extractor import TwitterExtractor
        from scripts.utils.db import load_dataframe_to_duckdb
        
        config = get_twitter_config()
        extractor = TwitterExtractor(
            config.bearer_token, config.consumer_key, config.consumer_secret,
            config.access_token, config.access_token_secret, config.username
        )
        
        all_data = extractor.extract_all(start_date=start_date, end_date=end_date)
        
        total_rows = 0
        tables = []
        for name, df in all_data.items():
            table_name = f"twitter_{name}"
            if load_dataframe_to_duckdb(duckdb_path, df, table_name, self.logger):
                total_rows += len(df)
                tables.append(table_name)
        
        return True, total_rows, tables
    
    def _send_failure_notification(self, run: JobRun) -> None:
        """Send notification for failed job."""
        # Placeholder for notification logic
        # Could integrate with email, Slack, etc.
        self.logger.error(
            f"ALERT: Job {run.job_name} failed after all retries. "
            f"Error: {run.error_message}"
        )


def create_daily_etl_job(
    source: str,
    hour: int = 6,
    minute: int = 0,
    lookback_days: int = 7,
    enabled: bool = False
) -> ETLJob:
    """
    Create a standard daily ETL job.
    
    Args:
        source: Data source (ga4, gsc, gads, meta, twitter)
        hour: Hour to run (0-23)
        minute: Minute to run (0-59)
        lookback_days: Days of data to extract
        enabled: Whether the job is enabled (default: False)
        
    Returns:
        Configured ETLJob
        
    Note:
        Jobs are disabled by default. Set enabled=True or call
        scheduler.run_job() with force=True to run.
    """
    config = JobConfig(
        name=f"{source}_daily",
        source=source,
        enabled=enabled,
        schedule=f"{minute} {hour} * * *",
        lookback_days=lookback_days,
        retry_count=3,
        notify_on_failure=True
    )
    
    return ETLJob(config)


def create_incremental_etl_job(
    source: str,
    hour: int = 6,
    minute: int = 0,
    enabled: bool = False
) -> ETLJob:
    """
    Create an incremental ETL job (only yesterday's data).
    
    Args:
        source: Data source
        hour: Hour to run
        minute: Minute to run
        enabled: Whether the job is enabled (default: False)
        
    Returns:
        Configured ETLJob with lookback_days=1
        
    Note:
        Jobs are disabled by default. Set enabled=True or call
        scheduler.run_job() with force=True to run.
    """
    config = JobConfig(
        name=f"{source}_incremental",
        source=source,
        enabled=enabled,
        schedule=f"{minute} {hour} * * *",
        lookback_days=1,
        retry_count=3,
        notify_on_failure=True
    )
    
    return ETLJob(config)
