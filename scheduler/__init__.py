"""
Scheduler Module for rs_analytics

This module provides scheduled job execution for ETL pipelines:
- Daily data extraction from all sources
- Configurable schedules per source
- Retry logic for failed jobs
- Job status tracking and logging

Usage:
    from scheduler import JobScheduler, create_daily_etl_job
    
    scheduler = JobScheduler()
    scheduler.add_job(create_daily_etl_job('gads'))
    scheduler.start()

For command-line usage:
    python -m scheduler.runner --start
    python -m scheduler.runner --run-now gads
"""

from scheduler.jobs import (
    ETLJob,
    JobConfig,
    create_daily_etl_job,
    create_incremental_etl_job,
)
from scheduler.runner import JobScheduler

__all__ = [
    'ETLJob',
    'JobConfig',
    'JobScheduler',
    'create_daily_etl_job',
    'create_incremental_etl_job',
]
