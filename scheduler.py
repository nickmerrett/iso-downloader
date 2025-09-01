import schedule
import time
import threading
import logging
import asyncio
from datetime import datetime
from typing import Callable, Optional
from config_manager import ConfigManager
from queue_manager import QueueManager

logger = logging.getLogger(__name__)


class DownloadScheduler:
    def __init__(self, config_manager: ConfigManager, queue_manager: QueueManager):
        self.config_manager = config_manager
        self.queue_manager = queue_manager
        self.scheduler_thread: Optional[threading.Thread] = None
        self.running = False
    
    def _schedule_job(self) -> None:
        logger.info("Scheduled download job triggered")
        try:
            self.config_manager.reload_config()
            # Run async method in sync context
            asyncio.run(self.queue_manager.publish_all_enabled_jobs(self.config_manager))
            logger.info(f"Scheduled download jobs published at {datetime.now()}")
        except Exception as e:
            logger.error(f"Error in scheduled job: {e}")
    
    def setup_schedule(self) -> None:
        schedule.clear()
        
        config = self.config_manager.config.scheduler
        frequency = config.frequency
        time_str = config.time
        
        if frequency == "daily":
            schedule.every().day.at(time_str).do(self._schedule_job)
            logger.info(f"Scheduled daily downloads at {time_str}")
            
        elif frequency == "weekly":
            schedule.every().monday.at(time_str).do(self._schedule_job)
            logger.info(f"Scheduled weekly downloads on Monday at {time_str}")
            
        elif frequency == "monthly":
            # Run on first day of every month - approximation using weekly check
            def monthly_job():
                now = datetime.now()
                if now.day == 1:
                    self._schedule_job()
            
            schedule.every().monday.at(time_str).do(monthly_job)
            logger.info(f"Scheduled monthly downloads on 1st at {time_str}")
        
        else:
            raise ValueError(f"Unsupported frequency: {frequency}")
    
    def _run_scheduler(self) -> None:
        logger.info("Scheduler thread started")
        while self.running:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
        logger.info("Scheduler thread stopped")
    
    def start(self) -> None:
        if self.running:
            logger.warning("Scheduler is already running")
            return
        
        self.setup_schedule()
        self.running = True
        self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.scheduler_thread.start()
        logger.info("Download scheduler started")
    
    def stop(self) -> None:
        if not self.running:
            logger.warning("Scheduler is not running")
            return
        
        self.running = False
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=5)
        
        schedule.clear()
        logger.info("Download scheduler stopped")
    
    def trigger_immediate_download(self) -> None:
        logger.info("Triggering immediate download")
        self._schedule_job()
    
    def get_next_run_time(self) -> Optional[datetime]:
        jobs = schedule.get_jobs()
        if not jobs:
            return None
        
        # Get the earliest next run time
        next_runs = [job.next_run for job in jobs if job.next_run]
        if not next_runs:
            return None
        
        return min(next_runs)
    
    def get_schedule_info(self) -> dict:
        config = self.config_manager.config.scheduler
        next_run = self.get_next_run_time()
        
        return {
            "frequency": config.frequency,
            "time": config.time,
            "running": self.running,
            "next_run": next_run.isoformat() if next_run else None,
            "jobs_count": len(schedule.get_jobs())
        }