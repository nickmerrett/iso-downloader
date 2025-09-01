import pika
import json
import logging
from typing import Dict, Any, Optional, Callable
from config_manager import ConfigManager, ISOConfig

logger = logging.getLogger(__name__)


class QueueManager:
    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager.config.rabbitmq
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
    
    def connect(self) -> None:
        credentials = pika.PlainCredentials(
            self.config.username, 
            self.config.password
        )
        
        parameters = pika.ConnectionParameters(
            host=self.config.host,
            port=self.config.port,
            credentials=credentials
        )
        
        try:
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.config.queue_name, durable=True)
            logger.info(f"Connected to RabbitMQ at {self.config.host}:{self.config.port}")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise
    
    def disconnect(self) -> None:
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("Disconnected from RabbitMQ")
    
    def publish_download_job(self, iso_config: ISOConfig) -> None:
        if not self.channel:
            self.connect()
        
        message = {
            "name": iso_config.name,
            "url": iso_config.url,
            "type": iso_config.type,
            "destination_dir": iso_config.destination_dir,
            "timestamp": None
        }
        
        self.channel.basic_publish(
            exchange='',
            routing_key=self.config.queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
            )
        )
        
        logger.info(f"Published download job for {iso_config.name}")
    
    async def publish_all_enabled_jobs(self, config_manager: ConfigManager) -> None:
        # Resolve all ISOs including glob patterns
        all_isos = await config_manager.resolve_all_isos()
        
        for iso in all_isos:
            self.publish_download_job(iso)
        
        logger.info(f"Published {len(all_isos)} download jobs to queue (including discovered ISOs)")
    
    def start_consumer(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        if not self.channel:
            self.connect()
        
        def wrapper(ch, method, properties, body):
            try:
                message = json.loads(body)
                callback(message)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                logger.debug(f"Processed message: {message['name']}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=self.config.queue_name,
            on_message_callback=wrapper
        )
        
        logger.info("Started consuming messages from queue")
        self.channel.start_consuming()
    
    def stop_consumer(self) -> None:
        if self.channel:
            self.channel.stop_consuming()
            logger.info("Stopped consuming messages")
    
    def get_queue_info(self) -> Dict[str, int]:
        if not self.channel:
            self.connect()
        
        method = self.channel.queue_declare(queue=self.config.queue_name, passive=True)
        return {
            "message_count": method.method.message_count,
            "consumer_count": method.method.consumer_count
        }