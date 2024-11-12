import asyncio
import json
import logging
import aiohttp
import uuid
from aiokafka import AIOKafkaProducer
from datetime import datetime
import os
import argparse
import time

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['10.180.8.24:9092', '10.180.8.24:9093', '10.180.8.24:9094']
DCGM_KAFKA_TOPIC = 'dcgm-metrics-test'
FETCH_INTERVAL = 1  # Seconds
RETRIES = 3
RETRY_DELAY = 5  # Seconds
TIMER_DURATION = 10 * 60  # 10 minutes in seconds
WARMUP_PERIOD = 60  # Warmup period in seconds

# Update producer configurations to use only supported parameters
producer_config = {
    'compression_type': 'gzip',  # Changed from 'lz4' to 'gzip'
    'acks': 1,
    'max_request_size': 1048576,
    'request_timeout_ms': 30000,
    'retry_backoff_ms': 100
}

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def create_producer():
    """Create a Kafka producer."""
    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k is not None else None,
            **producer_config
        )
        await producer.start()
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise

async def shutdown_producer(producer):
    """Flush and close the Kafka producer."""
    logger.info("Shutting down producer...")
    await producer.stop()
    logger.info("Producer shutdown complete.")

def parse_dcgm_metrics(metrics):
    """Parse the raw DCGM metrics text into the desired format."""
    lines = metrics.strip().split('\n')
    parsed_metrics = []
    for line in lines:
        # Skip empty lines and comments
        if not line.strip() or line.startswith("#"):
            continue
            
        try:
            # Split into metric name and rest of the line
            parts = line.split('{', 1)
            if len(parts) != 2:
                logger.debug(f"Skipping malformed line (no labels): {line}")
                continue
                
            metric_name, rest = parts
            # Split labels and value
            label_value_parts = rest.rsplit('}', 1)
            if len(label_value_parts) != 2:
                logger.debug(f"Skipping malformed line (no closing brace): {line}")
                continue
                
            labels, value = label_value_parts
            value = value.strip()
            metric_name = metric_name.strip()

            # Parse labels
            label_dict = {}
            for label in labels.split(','):
                if '=' in label:
                    key, val = label.split('=', 1)
                    label_dict[key.strip()] = val.strip('"')

            # Construct the key
            key = f"{metric_name}_gpu{label_dict.get('gpu', 'unknown')}"

            # Validate value can be converted to float
            try:
                float_value = float(value)
            except ValueError:
                logger.debug(f"Skipping line with invalid value: {line}")
                continue

            # Construct the message
            message = {
                "metric_name": metric_name,
                "value": float_value,
                "timestamp": datetime.now().isoformat(),
                "UUID": str(uuid.uuid4()),
                "gpu": label_dict.get('gpu', 'unknown'),
                "pci_bus_id": label_dict.get('pci_bus_id', 'unknown'),
                "device": label_dict.get('device', 'unknown'),
                "modelName": label_dict.get('modelName', 'unknown'),
                "Hostname": label_dict.get('Hostname', 'unknown'),
                "DCGM_FI_DRIVER_VERSION": label_dict.get('DCGM_FI_DRIVER_VERSION', 'unknown'),
                "err_code": "N/A",
                "err_msg": "N/A"
            }

            parsed_metrics.append((key, message))
        except Exception as e:
            logger.debug(f"Failed to parse line: {line}. Error: {str(e)}")
            continue
            
    return parsed_metrics

async def fetch_metrics(session, url):
    """Fetches metrics from a given URL using aiohttp."""
    async with session.get(url) as response:
        response.raise_for_status()
        return await response.text()

async def fetch_and_send_metrics(producer, session, node_id, num_nodes, num_processes):
    """Fetches metrics from a single node and sends them to Kafka."""
    port = 50000 + node_id  # Each node has its own port
    logger.info(f"Process handling node {node_id} on port {port}")

    while True:
        try:
            url = f"http://localhost:{port}/metrics"
            metrics = await fetch_metrics(session, url)
            parsed_metrics = parse_dcgm_metrics(metrics)
            
            await batch_send_messages(producer, parsed_metrics)
            logger.info(f"Sent metrics for node {node_id} (4 GPUs)")
            
        except Exception as e:
            logger.error(f"Error fetching/sending metrics for node {node_id}: {e}")
        
        await asyncio.sleep(FETCH_INTERVAL)

# Add batching support
async def batch_send_messages(producer, messages, batch_size=100):
    for i in range(0, len(messages), batch_size):
        batch = messages[i:i + batch_size]
        tasks = [
            producer.send_and_wait(DCGM_KAFKA_TOPIC, key=msg[0], value=msg[1])
            for msg in batch
        ]
        await asyncio.gather(*tasks)

async def stop_after_timer(tasks, producer):
    """Stops all metric-fetching tasks and shuts down the producer after the timer duration."""
    await asyncio.sleep(TIMER_DURATION)
    logger.info(f"Stopping all tasks after {TIMER_DURATION / 60} minutes.")
    
    # Cancel all running tasks
    for task in tasks:
        task.cancel()
    
    # Shutdown the producer
    await shutdown_producer(producer)

class RateLimiter:
    def __init__(self, rate_limit):
        self.rate_limit = rate_limit  # messages per second
        self.tokens = rate_limit
        self.last_update = time.monotonic()

    async def acquire(self):
        while self.tokens <= 0:
            now = time.monotonic()
            time_passed = now - self.last_update
            self.tokens = min(self.rate_limit, self.tokens + time_passed * self.rate_limit)
            self.last_update = now
            if self.tokens <= 0:
                await asyncio.sleep(0.1)
        self.tokens -= 1
        return True

async def run_producers(num_nodes, num_processes): 
    producer = await create_producer()
    rate_limiter = RateLimiter(rate_limit=1000)  # Adjust rate limit as needed
    
    metrics = {
        'start_time': time.time(),
        'warmup_complete': False,
        'messages_sent': 0,
        'bytes_sent': 0,
        'errors': 0
    }

    async def monitor_metrics():
        while True:
            await asyncio.sleep(30)
            elapsed = time.time() - metrics['start_time']
            if elapsed > WARMUP_PERIOD and not metrics['warmup_complete']:
                metrics['warmup_complete'] = True
                metrics['messages_sent'] = 0  # Reset after warmup
                logger.info("Warmup complete, starting main test")

            rate = metrics['messages_sent'] / 30 if metrics['warmup_complete'] else 0
            logger.info(f"Current sending rate: {rate:.2f} msgs/sec")

    async with aiohttp.ClientSession() as session:
        try:
            # Create one task per node
            tasks = [fetch_and_send_metrics(producer, session, i, num_nodes, num_processes) 
                    for i in range(num_nodes)]
            timer_task = asyncio.create_task(stop_after_timer(tasks, producer))
            
            await asyncio.gather(*tasks, timer_task)
        except asyncio.CancelledError:
            logger.info("Producer tasks cancelled.")
        finally:
            await shutdown_producer(producer)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Kafka producer.")
    parser.add_argument("--num_nodes", type=int, required=True, 
                      help="Number of nodes to simulate")
    parser.add_argument("--num_processes", type=int, required=True,
                      help="Number of processes to distribute the workload")
    args = parser.parse_args()

    # Validate arguments
    if args.num_nodes <= 0:
        parser.error("Number of nodes must be positive")
    if args.num_processes <= 0:
        parser.error("Number of processes must be positive")
    if args.num_processes > args.num_nodes:
        parser.error("Number of processes cannot exceed number of nodes")

    logger.info(f"Starting producer with {args.num_nodes} nodes and {args.num_processes} processes")
    asyncio.run(run_producers(args.num_nodes, args.num_processes))