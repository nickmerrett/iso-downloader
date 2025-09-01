import asyncio
import json
import logging
from typing import Dict, Any, Optional, Callable, List
from config_manager import ConfigManager, ISOConfig

logger = logging.getLogger(__name__)


class SimpleQueueManager:
    """In-memory queue manager for testing without RabbitMQ"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager.config.rabbitmq
        self.queue: asyncio.Queue = asyncio.Queue()
        self._running = False
    
    def connect(self) -> None:
        logger.info("Using in-memory queue (RabbitMQ not available)")
    
    def disconnect(self) -> None:
        logger.info("Disconnected from in-memory queue")
    
    def publish_download_job(self, iso_config: ISOConfig) -> None:
        message = {
            "name": iso_config.name,
            "url": iso_config.url,
            "type": iso_config.type,
            "destination_dir": iso_config.destination_dir,
            "timestamp": None
        }
        
        try:
            # Put in queue in a thread-safe way
            asyncio.create_task(self.queue.put(message))
            logger.info(f"Queued download job for {iso_config.name}")
        except Exception as e:
            logger.error(f"Failed to queue job: {e}")
    
    async def publish_all_enabled_jobs(self, config_manager: ConfigManager) -> None:
        # Resolve all ISOs including glob patterns
        all_isos = await config_manager.resolve_all_isos()
        
        for iso in all_isos:
            message = {
                "name": iso.name,
                "url": iso.url,
                "type": iso.type,
                "destination_dir": iso.destination_dir,
                "timestamp": None
            }
            await self.queue.put(message)
        
        logger.info(f"Queued {len(all_isos)} download jobs (including discovered ISOs)")
    
    async def start_consumer(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        self._running = True
        logger.info("Started consuming messages from in-memory queue")
        
        while self._running:
            try:
                # Wait for a message with timeout
                message = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                
                try:
                    await callback(message)
                    logger.debug(f"Processed message: {message['name']}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    # Put message back in queue for retry
                    await self.queue.put(message)
                
            except asyncio.TimeoutError:
                # No message available, continue loop
                continue
            except Exception as e:
                logger.error(f"Queue consumer error: {e}")
                await asyncio.sleep(1)
    
    def stop_consumer(self) -> None:
        self._running = False
        logger.info("Stopped consuming messages")
    
    def get_queue_info(self) -> Dict[str, int]:
        return {
            "message_count": self.queue.qsize(),
            "consumer_count": 1 if self._running else 0
        }