import asyncio
import json
import time
import logging
from aiokafka import AIOKafkaConsumer
from datetime import datetime, timedelta
import os
import numpy as np
from collections import defaultdict

# Create global logger
logger = None

# Configure logging with timestamp in filename
def setup_logging(node_size=None):
    global logger
    log_dir = "benchmark_logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Get most recent benchmark directory
    benchmark_dirs = [d for d in os.listdir(log_dir) if os.path.isdir(os.path.join(log_dir, d))]
    if not benchmark_dirs:
        raise RuntimeError("No benchmark directory found")
    
    latest_dir = max(benchmark_dirs, key=lambda x: os.path.getctime(os.path.join(log_dir, x)))
    log_path = os.path.join(log_dir, latest_dir)
    
    # Set up file handler with appropriate name
    if node_size:
        log_file = os.path.join(log_path, f'consumer_{node_size}_nodes.log')
    else:
        log_file = os.path.join(log_path, 'consumer.log')
    
    # Remove existing handlers
    logging.getLogger().handlers = []
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    return logger

# Constants
STATS_INTERVAL = 30  # Log stats every 30 seconds
WARMUP_PERIOD = 300  # 5 minutes warmup

class MetricsCollector:
    def __init__(self, logger):
        self.logger = logger
        self.start_time = time.time()
        self.message_count = 0
        self.bytes_received = 0
        self.message_sizes = []
        self.latencies = []
        self.nodes_seen = set()
        self.last_report_time = time.time()
        self.warmup_complete = False
        self.metrics_by_node = defaultdict(int)
        self.last_message_time = None
        
    async def record_message(self, msg):
        try:
            current_time = time.time()
            msg_size = len(msg.value)
            value = json.loads(msg.value.decode('utf-8'))
            
            # Extract node information
            hostname = value.get('Hostname', 'unknown')
            if hostname != 'unknown':
                self.nodes_seen.add(hostname)
                self.metrics_by_node[hostname] += 1
            
            # Record basic metrics
            self.message_count += 1
            self.bytes_received += msg_size
            self.message_sizes.append(msg_size)
            
            # Calculate latency if timestamp available
            if 'timestamp' in value:
                try:
                    msg_time = datetime.fromisoformat(value['timestamp'])
                    latency = (datetime.now() - msg_time).total_seconds()
                    self.latencies.append(latency)
                except ValueError:
                    pass
            
            # Check if it's time to log stats
            if current_time - self.last_report_time >= STATS_INTERVAL:
                await self.log_stats()
                self.last_report_time = current_time
                
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error: {e}")
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")

    async def log_stats(self):
        elapsed = time.time() - self.start_time
        
        # Log even if no messages received
        if self.message_count == 0:
            self.logger.info("No messages received yet")
            return
        
        # Check if warmup period is complete
        if not self.warmup_complete and elapsed > WARMUP_PERIOD:
            self.logger.info("Warmup period complete, resetting metrics")
            self.reset_metrics()
            self.warmup_complete = True
            return
        
        if not self.warmup_complete:
            self.logger.info("Still in warmup period, stats will reset after warmup")
            return
            
        # Calculate statistics
        interval = time.time() - self.last_report_time
        msgs_per_sec = self.message_count / interval if interval > 0 else 0
        mb_per_sec = (self.bytes_received / 1024 / 1024) / interval if interval > 0 else 0
        avg_latency = np.mean(self.latencies) if self.latencies else 0
        p95_latency = np.percentile(self.latencies, 95) if self.latencies else 0
        
        # Log detailed statistics
        self.logger.info(
            f"Stats Summary:\n"
            f"Throughput: {msgs_per_sec:.2f} msgs/sec\n"
            f"Bandwidth: {mb_per_sec:.2f} MB/sec\n"
            f"Average Latency: {avg_latency:.3f} sec\n"
            f"P95 Latency: {p95_latency:.3f} sec\n"
            f"Total Messages: {self.message_count}\n"
            f"Unique Nodes: {len(self.nodes_seen)}\n"
            f"Messages per Node: {dict(self.metrics_by_node)}"
        )

async def consume(node_size=None):
    """
    Main consumer function
    Args:
        node_size: Optional node size for logging purposes
    """
    global logger
    logger = setup_logging(node_size)
    collector = MetricsCollector(logger)
    
    # Add consumer configuration
    consumer_config = {
        'group_id': 'my-group-1',
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': True,
        'auto_commit_interval_ms': 1000,
        'max_poll_interval_ms': 300000,
        'session_timeout_ms': 10000,
        'heartbeat_interval_ms': 3000
    }
    
    consumer = AIOKafkaConsumer(
        'dcgm-metrics-test',
        bootstrap_servers=['10.180.8.24:9092', '10.180.8.24:9093', '10.180.8.24:9094'],
        **consumer_config
    )
    
    try:
        await consumer.start()
        logger.info("Consumer started successfully")
        
        # Get list of topics and partitions
        topics = await consumer.topics()
        logger.info(f"Available topics: {topics}")
        
        partitions = await consumer.partitions_for_topic('dcgm-metrics-test')
        logger.info(f"Partitions for dcgm-metrics-test: {partitions}")
        
        # Get beginning and end offsets
        topic_partitions = [TopicPartition('dcgm-metrics-test', p) for p in partitions] if partitions else []
        if topic_partitions:
            beginning_offsets = await consumer.beginning_offsets(topic_partitions)
            end_offsets = await consumer.end_offsets(topic_partitions)
            logger.info(f"Beginning offsets: {beginning_offsets}")
            logger.info(f"End offsets: {end_offsets}")
        
        # Add periodic stats logging even without messages
        stats_task = asyncio.create_task(periodic_stats(collector))
        
        async for msg in consumer:
            logger.debug(f"Received message from partition {msg.partition} at offset {msg.offset}")
            await collector.record_message(msg)
                
    except asyncio.CancelledError:
        logger.info("Consumer cancelled by user")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        # Cancel stats task
        stats_task.cancel()
        try:
            await stats_task
        except asyncio.CancelledError:
            pass
        # Log final statistics
        await collector.log_stats()
        await consumer.stop()
        logger.info("Consumer stopped")

async def periodic_stats(collector):
    """Periodically log stats even if no messages are received"""
    while True:
        await asyncio.sleep(STATS_INTERVAL)
        await collector.log_stats()

if __name__ == "__main__":
    try:
        # Add argument parsing for node size
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("--node_size", type=int, help="Number of nodes being tested")
        args = parser.parse_args()
        
        asyncio.run(consume(args.node_size))
    except KeyboardInterrupt:
        if logger:
            logger.info("Shutting down...")

