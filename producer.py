import logging
from aiokafka import AIOKafkaProducer
import asyncio
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
import time

class MetricsProducer:
    def __init__(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            retry_backoff_ms=500,
            request_timeout_ms=10000
        )
        
    async def start(self):
        await self.producer.start()
        
    async def close(self):
        await self.producer.stop()
        
    async def send_metric(self, metric_data):
        try:
            # Get precise timestamp in microseconds
            current_time = int(time.time() * 1000000)  # microsecond precision
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
        except Exception as e:
            logging.error(f"Error sending message to Kafka: {e}")
            raise