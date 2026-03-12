import asyncio
import logging
import signal
import threading
import time
from typing import Dict, Any
from config_manager import ConfigManager
from queue_manager import QueueManager
from downloader import DownloadManager

logger = logging.getLogger(__name__)


class DownloadWorker:
    def __init__(self, config_path: str = "config.yaml"):
        self.config_manager = ConfigManager(config_path)
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

    def _run_consumer(self, thread_id: int) -> None:
        """Run a single consumer loop in its own thread with its own RabbitMQ connection."""
        queue_manager = QueueManager(self.config_manager)

        def sync_callback(message: Dict[str, Any]):
            asyncio.run(self.process_download_job(message))

        while self.running:
            try:
                queue_manager.connect()
                logger.info(f"Worker thread {thread_id} connected, waiting for jobs...")
                queue_manager.start_consumer(sync_callback)
            except Exception as e:
                if self.running:
                    logger.error(f"Worker thread {thread_id} connection lost: {e} — reconnecting in 5s")
                    time.sleep(5)
            finally:
                try:
                    queue_manager.disconnect()
                except Exception:
                    pass

        logger.info(f"Worker thread {thread_id} stopped")

    def start_worker(self):
        concurrency = self.config_manager.config.worker.concurrency
        logger.info(f"Starting ISO download worker with {concurrency} concurrent thread(s)...")

        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.running = True

        threads = [
            threading.Thread(target=self._run_consumer, args=(i,), daemon=True)
            for i in range(concurrency)
        ]

        for t in threads:
            t.start()

        try:
            for t in threads:
                t.join()
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
            self.running = False
            for t in threads:
                t.join(timeout=10)

        logger.info("Worker stopped")


if __name__ == "__main__":
    worker = DownloadWorker()
    worker.start_worker()
