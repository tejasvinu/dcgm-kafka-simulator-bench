import asyncio
import logging
import time
from aiokafka import AIOKafkaConsumer
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Metrics:
    def __init__(self):
        self.message_count = 0
        self.total_latency = 0

    def add_message(self, latency):
        self.message_count += 1
        self.total_latency += latency

async def process_message(message, metrics):
    """Process each message received from Kafka"""
    try:
        receive_time = time.time() * 1000  # Convert to milliseconds
        value = message.value.decode('utf-8')
        
        # Extract send_time from message if available, otherwise use receive_time
        try:
            send_time = float(message.headers.get('send_time', receive_time))
        except:
            send_time = receive_time
            
        latency = receive_time - send_time
        metrics.add_message(latency)
        
        if metrics.message_count % 1000 == 0:
            logger.info(f"Processed {metrics.message_count} messages")
            
    except Exception as e:
        logger.error(f"Error processing message: {e}")

async def consume_metrics(metrics=None):
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='dcgm-metrics-consumer-group',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        max_poll_records=500
    )
    
    try:
        await consumer.start()
        logger.info(f"Started consuming from {KAFKA_TOPIC}")
        
        async for message in consumer:
            await process_message(message, metrics)
                
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    metrics = Metrics()
    try:
        asyncio.run(consume_metrics(metrics))
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
