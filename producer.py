import logging
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaTimeoutError, KafkaConnectionError
import asyncio
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
import time

class MetricsProducer:
    def __init__(self):
        self.producer = None
        self.max_retries = 3
        self.retry_delay = 1  # seconds
        self.connected = False
        
    async def start(self):
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                self.producer = AIOKafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    retry_backoff_ms=100,
                    request_timeout_ms=10000,
                    max_batch_size=32768,
                    compression_type='gzip',
                    acks=1,  # Changed from 'all' to 1
                    api_version='auto'
                )
                await self.producer.start()
                self.connected = True
                logging.info("Producer successfully connected to Kafka")
                break
            except Exception as e:
                retry_count += 1
                logging.error(f"Failed to start producer (attempt {retry_count}): {e}")
                if retry_count < self.max_retries:
                    await asyncio.sleep(self.retry_delay * retry_count)
                else:
                    raise
        
    async def close(self):
        if self.producer:
            self.connected = False
            await self.producer.stop()
            
    async def send_metric(self, metric_data):
        if not self.connected:
            raise RuntimeError("Producer not connected")
            
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                current_time = int(time.time() * 1000000)
                timestamp_ms = current_time // 1000
                
                headers = [
                    ('send_time_us', str(current_time).encode('utf-8'))
                ]
                
                await self.producer.send_and_wait(
                    topic=KAFKA_TOPIC,
                    value=metric_data.encode('utf-8'),
                    headers=headers,
                    timestamp_ms=timestamp_ms
                )
                return
                
            except (KafkaTimeoutError, KafkaConnectionError) as e:
                retry_count += 1
                if retry_count < self.max_retries:
                    await asyncio.sleep(self.retry_delay * retry_count)
                    logging.warning(f"Retrying send after error: {e}")
                else:
                    logging.error(f"Failed to send message after {self.max_retries} attempts")
                    raise
            except Exception as e:
                logging.error(f"Unexpected error sending message: {e}")
                raise