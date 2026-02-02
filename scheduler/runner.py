#!/usr/bin/env python3
"""
Job Scheduler Runner for rs_analytics

This module provides the main scheduler that manages and executes ETL jobs.

Features:
- Schedule jobs with cron expressions
- Run jobs immediately on demand
- Track job history and status
- Graceful shutdown handling

Usage:
    # As a module
    from scheduler.runner import JobScheduler
    
    scheduler = JobScheduler()
    scheduler.add_default_jobs()
    scheduler.start()
    
    # From command line
    python -m scheduler.runner --start
    python -m scheduler.runner --run-now gads
    python -m scheduler.runner --list-jobs

Note: This module requires the 'apscheduler' package for full scheduling functionality.
Install with: pip install apscheduler>=3.10.0
"""

import argparse
import logging
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scheduler.jobs import (
    ETLJob,
    JobConfig,
    JobRun,
    create_daily_etl_job,
    create_incremental_etl_job,
)


logger = logging.getLogger(__name__)


class JobScheduler:
    """
    Main scheduler for ETL jobs.
    
    Manages job registration, scheduling, and execution.
    """
    
    def __init__(self, use_apscheduler: bool = True):
        """
        Initialize the job scheduler.
        
        Args:
            use_apscheduler: Whether to use APScheduler for cron scheduling.
                           If False, jobs must be run manually.
        """
        self.jobs: Dict[str, ETLJob] = {}
        self.running = False
        self.use_apscheduler = use_apscheduler
        self._scheduler = None
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        # Try to initialize APScheduler if available
        if use_apscheduler:
            try:
                from apscheduler.schedulers.background import BackgroundScheduler
                from apscheduler.triggers.cron import CronTrigger
                self._scheduler = BackgroundScheduler()
                logger.info("APScheduler initialized successfully")
            except ImportError:
                logger.warning(
                    "APScheduler not installed. Jobs will only run manually. "
                    "Install with: pip install apscheduler>=3.10.0"
                )
                self.use_apscheduler = False
    
    def add_job(self, job: ETLJob) -> None:
        """
        Add a job to the scheduler.
        
        Args:
            job: ETLJob to add
        """
        self.jobs[job.config.name] = job
        logger.info(f"Added job: {job.config.name} (schedule: {job.config.schedule})")
        
        # Schedule with APScheduler if available
        if self.use_apscheduler and self._scheduler:
            from apscheduler.triggers.cron import CronTrigger
            
            # Parse cron expression
            cron_parts = job.config.schedule.split()
            if len(cron_parts) == 5:
                trigger = CronTrigger(
                    minute=cron_parts[0],
                    hour=cron_parts[1],
                    day=cron_parts[2],
                    month=cron_parts[3],
                    day_of_week=cron_parts[4]
                )
                
                self._scheduler.add_job(
                    func=job.execute,
                    trigger=trigger,
                    id=job.config.name,
                    name=job.config.name,
                    replace_existing=True
                )
    
    def add_default_jobs(self, enabled: bool = False) -> None:
        """
        Add default daily ETL jobs for all sources.
        
        Creates jobs that run at staggered times:
        - GA4: 6:00 AM
        - GSC: 6:15 AM
        - Google Ads: 6:30 AM
        - Meta: 6:45 AM
        - Twitter: 7:00 AM
        
        Args:
            enabled: Whether jobs should be enabled (default: False)
                    Jobs are disabled by default and can be run manually
                    with --run-now or --run-all --force flags.
        """
        jobs = [
            create_daily_etl_job('ga4', hour=6, minute=0, enabled=enabled),
            create_daily_etl_job('gsc', hour=6, minute=15, enabled=enabled),
            create_daily_etl_job('gads', hour=6, minute=30, enabled=enabled),
            create_daily_etl_job('meta', hour=6, minute=45, enabled=enabled),
            create_daily_etl_job('twitter', hour=7, minute=0, enabled=enabled),
        ]
        
        for job in jobs:
            self.add_job(job)
        
        status = "enabled" if enabled else "disabled (use --force to run)"
        logger.info(f"Added {len(jobs)} default daily jobs ({status})")
    
    def remove_job(self, job_name: str) -> bool:
        """
        Remove a job from the scheduler.
        
        Args:
            job_name: Name of the job to remove
            
        Returns:
            True if removed, False if not found
        """
        if job_name in self.jobs:
            del self.jobs[job_name]
            
            if self._scheduler:
                try:
                    self._scheduler.remove_job(job_name)
                except Exception:
                    pass
            
            logger.info(f"Removed job: {job_name}")
            return True
        
        return False
    
    def run_job(self, job_name: str, force: bool = False) -> Optional[JobRun]:
        """
        Run a specific job immediately.
        
        Args:
            job_name: Name of the job to run
            force: Run even if job is disabled
            
        Returns:
            JobRun record, or None if job not found
        """
        if job_name not in self.jobs:
            logger.error(f"Job not found: {job_name}")
            return None
        
        job = self.jobs[job_name]
        logger.info(f"Running job manually: {job_name}")
        
        return job.execute(force=force)
    
    def run_all_jobs(self, force: bool = False) -> List[JobRun]:
        """
        Run all jobs immediately.
        
        Args:
            force: Run even if jobs are disabled
            
        Returns:
            List of JobRun records
        """
        results = []
        
        for job_name, job in self.jobs.items():
            logger.info(f"Running job: {job_name}")
            result = job.execute(force=force)
            results.append(result)
        
        return results
    
    def start(self) -> None:
        """Start the scheduler (for cron-based scheduling)."""
        if not self.use_apscheduler or not self._scheduler:
            logger.error(
                "APScheduler not available. Cannot start cron scheduler. "
                "Use run_job() or run_all_jobs() instead."
            )
            return
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        self._scheduler.start()
        self.running = True
        
        logger.info("Scheduler started. Press Ctrl+C to stop.")
        
        # List scheduled jobs
        self.list_jobs()
        
        # Keep running until shutdown
        try:
            while self.running:
                import time
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            self.stop()
    
    def stop(self) -> None:
        """Stop the scheduler gracefully."""
        logger.info("Stopping scheduler...")
        self.running = False
        
        if self._scheduler:
            self._scheduler.shutdown(wait=True)
        
        logger.info("Scheduler stopped.")
    
    def _handle_shutdown(self, signum, frame) -> None:
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
    
    def list_jobs(self) -> None:
        """Print list of registered jobs."""
        print("\nRegistered Jobs:")
        print("-" * 60)
        
        for name, job in self.jobs.items():
            status = "enabled" if job.config.enabled else "disabled"
            print(f"  {name}")
            print(f"    Source: {job.config.source}")
            print(f"    Schedule: {job.config.schedule}")
            print(f"    Lookback: {job.config.lookback_days} days")
            print(f"    Status: {status}")
            
            if job.last_run:
                result = "SUCCESS" if job.last_run.success else "FAILED"
                print(f"    Last Run: {job.last_run.started_at} ({result})")
            
            print()
    
    def get_job_status(self) -> Dict[str, dict]:
        """
        Get status of all jobs.
        
        Returns:
            Dictionary mapping job names to status info
        """
        status = {}
        
        for name, job in self.jobs.items():
            status[name] = {
                'source': job.config.source,
                'schedule': job.config.schedule,
                'enabled': job.config.enabled,
                'last_run': job.last_run.to_dict() if job.last_run else None,
                'run_count': len(job.run_history),
            }
        
        return status


def main():
    """Command-line interface for the scheduler."""
    parser = argparse.ArgumentParser(
        description="rs_analytics ETL Job Scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m scheduler.runner --list-jobs           # List configured jobs
  python -m scheduler.runner --run-now gads        # Run Google Ads ETL now
  python -m scheduler.runner --run-all --force     # Run all ETLs now (forced)
  python -m scheduler.runner --start --enable      # Start scheduler with jobs enabled

Note:
  Jobs are DISABLED by default. Use --force to run disabled jobs manually,
  or --enable with --start to enable jobs for automatic scheduling.
        """
    )
    
    parser.add_argument(
        "--start",
        action="store_true",
        help="Start the scheduler daemon"
    )
    
    parser.add_argument(
        "--run-now",
        type=str,
        metavar="SOURCE",
        help="Run ETL for a specific source immediately"
    )
    
    parser.add_argument(
        "--run-all",
        action="store_true",
        help="Run all ETLs immediately"
    )
    
    parser.add_argument(
        "--list-jobs",
        action="store_true",
        help="List all configured jobs"
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force run even if job is disabled"
    )
    
    parser.add_argument(
        "--enable",
        action="store_true",
        help="Enable jobs when starting scheduler (used with --start)"
    )
    
    args = parser.parse_args()
    
    # Create scheduler and add default jobs
    scheduler = JobScheduler()
    scheduler.add_default_jobs(enabled=args.enable)
    
    if args.list_jobs:
        scheduler.list_jobs()
        return 0
    
    if args.run_now:
        # Map source to job name
        job_name = f"{args.run_now}_daily"
        result = scheduler.run_job(job_name, force=args.force)
        
        if result:
            if result.success:
                print(f"\nJob completed successfully!")
                print(f"  Rows extracted: {result.rows_extracted:,}")
                print(f"  Tables created: {len(result.tables_created)}")
                return 0
            else:
                print(f"\nJob failed: {result.error_message}")
                return 1
        else:
            print(f"\nJob not found: {job_name}")
            return 1
    
    if args.run_all:
        results = scheduler.run_all_jobs(force=args.force)
        
        success_count = sum(1 for r in results if r.success)
        fail_count = len(results) - success_count
        
        print(f"\nCompleted: {success_count} succeeded, {fail_count} failed")
        
        for result in results:
            status = "OK" if result.success else "FAILED"
            print(f"  [{status}] {result.job_name}: {result.rows_extracted:,} rows")
        
        return 0 if fail_count == 0 else 1
    
    if args.start:
        print("Starting scheduler daemon...")
        scheduler.start()
        return 0
    
    # No action specified
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
