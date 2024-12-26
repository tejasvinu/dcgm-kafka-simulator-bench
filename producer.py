import asyncio
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaTimeoutError
import logging
from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, 
    PRODUCER_COMPRESSION, PRODUCER_BATCH_SIZE, 
    PRODUCER_LINGER_MS, MAX_REQUEST_SIZE,
    REQUEST_TIMEOUT_MS, METADATA_MAX_AGE_MS,
    RETRY_BACKOFF_MS, CONNECTIONS_MAX_IDLE_MS
)
import os
import sys
from datetime import datetime

def setup_logging():
    log_dir = "producer_logs"
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"{log_dir}/producer_{os.getpid()}_{timestamp}.log"
    
    # Create handlers
    file_handler = logging.FileHandler(filename=log_file)
    console_handler = logging.StreamHandler(sys.stdout)
    
    # Create formatters and add it to handlers
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    file_handler.setFormatter(logging.Formatter(log_format))
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # Get root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return log_file

class MetricsProducer:
    def __init__(self):
        self.log_file = setup_logging()  # Set up logging
        self.producer = None
        self.bootstrap_servers = ','.join(KAFKA_BOOTSTRAP_SERVERS)
        self.max_retries = 3
        self.retry_backoff = 1

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            compression_type=PRODUCER_COMPRESSION,  # Using config value
            client_id="dcgm-metrics-producer",
            request_timeout_ms=REQUEST_TIMEOUT_MS,
            metadata_max_age_ms=METADATA_MAX_AGE_MS,
            retry_backoff_ms=RETRY_BACKOFF_MS,
            connections_max_idle_ms=CONNECTIONS_MAX_IDLE_MS,
            enable_idempotence=True,
            max_request_size=MAX_REQUEST_SIZE,
            acks='all',  # Ensure durability with the new replication settings
            max_batch_size=16384,
            linger_ms=100,
            reconnect_backoff_max_ms=10000,
            reconnect_backoff_ms=100,
            partitioner=lambda key, all_partitions, available: hash(key) % len(all_partitions)
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
