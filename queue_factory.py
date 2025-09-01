import logging
from config_manager import ConfigManager

logger = logging.getLogger(__name__)


def create_queue_manager(config_manager: ConfigManager):
    """Factory function to create appropriate queue manager"""
    try:
        # Try to import and use RabbitMQ
        from queue_manager import QueueManager
        queue_manager = QueueManager(config_manager)
        
        # Test connection
        queue_manager.connect()
        queue_manager.disconnect()
        
        logger.info("Using RabbitMQ queue manager")
        return queue_manager
        
    except Exception as e:
        logger.warning(f"RabbitMQ not available ({e}), falling back to simple queue")
        
        # Fall back to simple queue
        from simple_queue import SimpleQueueManager
        return SimpleQueueManager(config_manager)