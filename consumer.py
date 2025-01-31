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
        # Get current time in microseconds
        receive_time = int(time.time() * 1000000)
        
        # Extract send_time from headers
        send_time = None
        if message.headers:
            for key, value in message.headers:
                if key == 'send_time_us':
                    try:
                        send_time = int(value.decode('utf-8'))
                        break
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Invalid timestamp in header: {e}")
        
        if send_time is None:
            logger.warning("No valid send_time found in message headers")
            return
            
        # Calculate latency in milliseconds
        latency = (receive_time - send_time) / 1000.0  # Convert to milliseconds
        metrics.add_message(latency)
        
        if metrics.message_count % 1000 == 0:
            # Calculate average latency based on available attributes
            if hasattr(metrics, 'total_latency'):
                avg_latency = metrics.total_latency / metrics.message_count
            else:
                avg_latency = sum(metrics.latencies) / len(metrics.latencies)
                
            logger.info(f"Processed {metrics.message_count} messages. "
                       f"Avg latency: {avg_latency:.2f}ms, "
                       f"Current latency: {latency:.2f}ms")
            
    except Exception as e:
        logger.error(f"Error processing message: {e}", exc_info=True)

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
