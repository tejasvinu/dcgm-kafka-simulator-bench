import asyncio
import json
import time
import logging
from aiokafka import AIOKafkaConsumer
from datetime import datetime, timedelta
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kafka_consumer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Kafka Configuration
bootstrap_servers = ['10.180.8.24:9092', '10.180.8.24:9093', '10.180.8.24:9094']
topic_name = 'dcgm-metrics-test'
group_id = 'my-group-1'

# Add consumer configurations
consumer_config = {
    'fetch_max_bytes': 52428800,  # 50MB
    'max_partition_fetch_bytes': 1048576,  # 1MB
    'fetch_max_wait_ms': 500,
    'enable_auto_commit': False,
    'max_poll_records': 500
}

# Metrics Tracking
metrics = {
    'messages_received': 0,
    'bytes_received': 0,
    'processing_time': timedelta(0),
    'errors': 0,
    'message_sizes': [],
    'processing_times': [],
    'error_types': {},
    'batch_sizes': [],
    'latencies': [],
    'messages_per_node': {},
    'messages_per_gpu': {},
    'last_metrics_time': datetime.now(),
    'metrics_interval': 30  # Log metrics every 60 seconds
}

def log_metrics_summary():
    """Log detailed metrics summary"""
    current_time = datetime.now()
    elapsed_time = (current_time - metrics['last_metrics_time']).total_seconds()
    
    # Calculate statistics
    avg_msg_size = sum(metrics['message_sizes']) / len(metrics['message_sizes']) if metrics['message_sizes'] else 0
    avg_process_time = sum(metrics['processing_times']) / len(metrics['processing_times']) if metrics['processing_times'] else 0
    avg_latency = sum(metrics['latencies']) / len(metrics['latencies']) if metrics['latencies'] else 0
    throughput = metrics['messages_received'] / elapsed_time if elapsed_time > 0 else 0
    
    logger.info("\n=== Metrics Summary ===")
    logger.info(f"Messages Received: {metrics['messages_received']}")
    logger.info(f"Bytes Received: {metrics['bytes_received']:,} bytes")
    logger.info(f"Average Message Size: {avg_msg_size:.2f} bytes")
    logger.info(f"Average Processing Time: {avg_process_time:.4f} seconds")
    logger.info(f"Average Latency: {avg_latency:.4f} seconds")
    logger.info(f"Throughput: {throughput:.2f} messages/second")
    logger.info(f"Total Errors: {metrics['errors']}")
    
    # Log per-node statistics
    logger.info("\nMessages per Node:")
    for node, count in sorted(metrics['messages_per_node'].items()):
        logger.info(f"  Node {node}: {count:,} messages")
    
    # Log per-GPU statistics
    logger.info("\nMessages per GPU:")
    for gpu, count in sorted(metrics['messages_per_gpu'].items()):
        logger.info(f"  GPU {gpu}: {count:,} messages")
    
    # Log error types if any
    if metrics['error_types']:
        logger.info("\nError Types:")
        for error_type, count in metrics['error_types'].items():
            logger.info(f"  {error_type}: {count}")
    
    logger.info("=" * 50 + "\n")
    metrics['last_metrics_time'] = current_time

async def process_message(msg):
    """Process a single message and update metrics"""
    try:
        start_time = time.monotonic()
        value = json.loads(msg.value.decode('utf-8'))
        
        # Extract node and GPU information
        hostname = value.get('Hostname', 'unknown')
        gpu = value.get('gpu', 'unknown')
        
        # Update metrics
        metrics['messages_received'] += 1
        metrics['bytes_received'] += len(msg.value)
        metrics['message_sizes'].append(len(msg.value))
        
        # Update node and GPU counters
        metrics['messages_per_node'][hostname] = metrics['messages_per_node'].get(hostname, 0) + 1
        metrics['messages_per_gpu'][gpu] = metrics['messages_per_gpu'].get(gpu, 0) + 1
        
        # Calculate and store processing time
        process_time = time.monotonic() - start_time
        metrics['processing_times'].append(process_time)
        metrics['processing_time'] += timedelta(seconds=process_time)
        
        # Calculate message latency if timestamp available
        if 'timestamp' in value:
            try:
                msg_time = datetime.fromisoformat(value['timestamp'])
                latency = (datetime.now() - msg_time).total_seconds()
                metrics['latencies'].append(latency)
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid timestamp format: {e}")
        
        # Log detailed message info every 1000 messages
        if metrics['messages_received'] % 1000 == 0:
            logger.debug(f"Processed message: {value['metric_name']} from GPU {gpu} on {hostname}")
        
        # Log metrics summary periodically
        if (datetime.now() - metrics['last_metrics_time']).seconds >= metrics['metrics_interval']:
            log_metrics_summary()
            
    except json.JSONDecodeError as e:
        metrics['errors'] += 1
        metrics['error_types']['JSONDecodeError'] = metrics['error_types'].get('JSONDecodeError', 0) + 1
        logger.error(f"JSON decode error: {e}")
    except Exception as e:
        metrics['errors'] += 1
        error_type = type(e).__name__
        metrics['error_types'][error_type] = metrics['error_types'].get(error_type, 0) + 1
        logger.error(f"Error processing message: {e}")

async def consume():
    """Main consumer function"""
    logger.info("Starting Kafka consumer...")
    consumer = AIOKafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset='earliest',
        **consumer_config
    )
    
    try:
        await consumer.start()
        logger.info("Consumer started successfully")
        
        async for msg in consumer:
            await process_message(msg)
            
    except Exception as e:
        logger.error(f"Consumer error: {e}")
        raise
    finally:
        logger.info("Stopping consumer...")
        await consumer.stop()
        log_metrics_summary()  # Final metrics summary
        logger.info("Consumer stopped")

async def main():
    try:
        await consume()
    except asyncio.CancelledError:
        logger.info("Consumer cancelled by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
    asyncio.run(main())

