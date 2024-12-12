import asyncio
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaTimeoutError
import logging
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

class MetricsProducer:
    def __init__(self):
        self.producer = None
        self.bootstrap_servers = ','.join(KAFKA_BOOTSTRAP_SERVERS)
        self.max_retries = 3
        self.retry_backoff = 1

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            compression_type="gzip",  # Enable compression
            batch_size=16384,  # Increase batch size
            linger_ms=100,  # Wait up to 100ms to batch messages
            max_batch_size=3_000_000,  # Max batch size in bytes
            max_request_size=3_000_000,  # Max request size in bytes
            request_timeout_ms=10000,  # Increase timeout to 10s
            retry_backoff_ms=100,  # Backoff between retries
            max_in_flight_requests_per_connection=5,  # Allow multiple in-flight requests
            enable_idempotence=True  # Ensure exactly-once delivery
        )
        await self.producer.start()

    async def send_metric(self, metric_data):
        retries = 0
        while True:
            try:
                return await self.producer.send_and_wait(
                    KAFKA_TOPIC, 
                    metric_data.encode('utf-8')
                )
            except KafkaTimeoutError as e:
                retries += 1
                if retries >= self.max_retries:
                    raise
                await asyncio.sleep(self.retry_backoff * retries)
                logging.warning(f"Kafka timeout, retrying ({retries}/{self.max_retries})")

    async def close(self):
        if self.producer:
            await self.producer.stop()
