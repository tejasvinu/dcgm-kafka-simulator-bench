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
            compression_type="snappy",  # Changed to snappy for better performance
            client_id="dcgm-metrics-producer",
            linger_ms=100,  # Wait up to 100ms to batch messages
            max_batch_size=1048576,  # 1MB batch size
            request_timeout_ms=30000,  # 30 seconds timeout
            retry_backoff_ms=100,
            enable_idempotence=True,  # Ensure exactly-once delivery
            max_request_size=1048576,  # 1MB max request size
            partitioner=lambda key, all_partitions, available: hash(key) % len(all_partitions)  # Custom partitioner
        )
        await self.producer.start()

    async def send_metric(self, metric_data, key=None):
        retries = 0
        while True:
            try:
                # Use server_id as key for consistent partitioning
                if key is None:
                    key = metric_data.split('\n')[0].encode('utf-8')  # Use first line as key
                return await self.producer.send_and_wait(
                    KAFKA_TOPIC, 
                    metric_data.encode('utf-8'),
                    key=key,
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
