import asyncio
import logging
import signal
import sys
from typing import Dict, Any
from config_manager import ConfigManager
from queue_manager import QueueManager
from downloader import DownloadManager

logger = logging.getLogger(__name__)


class DownloadWorker:
    def __init__(self, config_path: str = "config.yaml"):
        self.config_manager = ConfigManager(config_path)
        self.queue_manager = QueueManager(self.config_manager)
        self.download_manager = DownloadManager(self.config_manager.config.download)
        self.running = False
        self.setup_logging()
    
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('iso_downloader.log')
            ]
        )
    
    def signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
        self.queue_manager.stop_consumer()
    
    async def process_download_job(self, job: Dict[str, Any]) -> None:
        logger.info(f"Processing download job: {job['name']}")
        
        try:
            result = await self.download_manager.download_iso(job)
            
            if result["success"]:
                logger.info(f"Download completed successfully: {result['job_name']} "
                           f"({result['size_bytes']} bytes, {result['speed_mbps']:.2f} MB/s)")
            else:
                logger.error(f"Download failed: {result['job_name']} - {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"Unexpected error processing job {job['name']}: {e}")
    
    def start_worker(self):
        logger.info("Starting ISO download worker...")
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        self.running = True
        
        def sync_callback(message: Dict[str, Any]):
            asyncio.run(self.process_download_job(message))
        
        try:
            self.queue_manager.start_consumer(sync_callback)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error(f"Worker error: {e}")
        finally:
            self.queue_manager.disconnect()
            logger.info("Worker stopped")


if __name__ == "__main__":
    worker = DownloadWorker()
    worker.start_worker()