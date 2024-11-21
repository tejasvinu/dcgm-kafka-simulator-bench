import asyncio
import json
import time
import logging
from aiokafka import AIOKafkaConsumer
from datetime import datetime, timedelta
import os
import numpy as np
from collections import defaultdict

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

# Constants
STATS_INTERVAL = 30  # Log stats every 30 seconds
WARMUP_PERIOD = 300  # 5 minutes warmup

class MetricsCollector:
    def __init__(self):
        self.start_time = time.time()
        self.message_count = 0
        self.bytes_received = 0
        self.message_sizes = []
        self.latencies = []
        self.nodes_seen = set()
        self.last_report_time = time.time()
        self.warmup_complete = False
        self.metrics_by_node = defaultdict(int)
        
    def record_message(self, msg, value):
        current_time = time.time()
        msg_size = len(msg.value)
        
        # Extract node information from the message
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
            self.log_stats()
            self.last_report_time = current_time
    
    def log_stats(self):
        elapsed = time.time() - self.start_time
        
        # Check if warmup period is complete
        if not self.warmup_complete and elapsed > WARMUP_PERIOD:
            logger.info("Warmup period complete, resetting metrics")
            self.reset_metrics()
            self.warmup_complete = True
            return
        
        if not self.warmup_complete:
            logger.info("Still in warmup period, stats will reset after warmup")
            return
            
        # Calculate statistics
        interval = time.time() - self.last_report_time
        msgs_per_sec = self.message_count / interval if interval > 0 else 0
        mb_per_sec = (self.bytes_received / 1024 / 1024) / interval if interval > 0 else 0
        avg_latency = np.mean(self.latencies) if self.latencies else 0
        p95_latency = np.percentile(self.latencies, 95) if self.latencies else 0
        
        # Log detailed statistics
        logger.info(
            f"Stats Summary:\n"
            f"Throughput: {msgs_per_sec:.2f} msgs/sec\n"
            f"Bandwidth: {mb_per_sec:.2f} MB/sec\n"
            f"Average Latency: {avg_latency:.3f} sec\n"
            f"P95 Latency: {p95_latency:.3f} sec\n"
            f"Total Messages: {self.message_count}\n"
            f"Unique Nodes: {len(self.nodes_seen)}\n"
            f"Messages per Node: {dict(self.metrics_by_node)}"
        )
    
    def reset_metrics(self):
        self.message_count = 0
        self.bytes_received = 0
        self.message_sizes = []
        self.latencies = []
        self.metrics_by_node.clear()
        self.last_report_time = time.time()

async def consume():
    collector = MetricsCollector()
    consumer = AIOKafkaConsumer(
        'dcgm-metrics-test',
        bootstrap_servers=['10.180.8.24:9092', '10.180.8.24:9093', '10.180.8.24:9094'],
        group_id='my-group-1',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    try:
        await consumer.start()
        logger.info("Consumer started successfully")
        
        async for msg in consumer:
            try:
                collector.record_message(msg, msg.value)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    except asyncio.CancelledError:
        logger.info("Consumer cancelled by user")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        # Log final statistics
        collector.log_stats()
        await consumer.stop()
        logger.info("Consumer stopped")

if __name__ == "__main__":
    try:
        asyncio.run(consume())
    except KeyboardInterrupt:
        logger.info("Shutting down...")

