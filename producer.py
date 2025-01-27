from aiokafka import AIOKafkaProducer
import asyncio
import logging
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

logger = logging.getLogger(__name__)

class MetricsProducer:
    def __init__(self):
        self.producer = None
        self.topic = KAFKA_TOPIC
        self.producer_config = {
            'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
            'acks': 1,  # Changed from 'all' to 1 for better throughput
            'compression_type': 'gzip',
            'max_batch_size': 32768,  # Replace batch_size
            'linger_ms': 50,      # Increased linger time
            'max_request_size': 4194304,
            'request_timeout_ms': 30000
        }

    async def start(self):
        if self.producer is None:
            self.producer = AIOKafkaProducer(**self.producer_config)
            await self.producer.start()
            logger.info("Producer started successfully")

    async def close(self):
        if self.producer is not None:
            await self.producer.stop()
            self.producer = None
            logger.info("Producer closed successfully")

    async def send_metric(self, metric: str):
        if self.producer is None:
            raise RuntimeError("Producer not started. Call start() first")
        try:
            await self.producer.send_and_wait(
                self.topic,
                metric.encode('utf-8')
            )
        except Exception as e:
            logger.error(f"Error sending metric: {e}")
            raise
