import asyncio
from aiokafka import AIOKafkaConsumer
from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
    CONSUMER_GROUP, BATCH_SIZE, STATS_INTERVAL
)
import time
import sys
import logging
import psutil
import os

logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - PID:%(process)d - %(message)s')

class MetricsConsumer:
    def __init__(self):
        self.bootstrap_servers = ','.join(KAFKA_BOOTSTRAP_SERVERS)
        self.consumer = None
        self.message_count = 0
        self.start_time = time.time()
        self.batch_size = BATCH_SIZE
        self.max_init_retries = 3
        self.init_retry_delay = 5
        self.process = psutil.Process(os.getpid())
        self.consumer_id = f"{os.getpid()}_{int(time.time())}"
        self.logger = logging.getLogger(f'Consumer-{self.consumer_id}')
        self.last_stats_time = time.time()
        self.stats_interval = STATS_INTERVAL  # Log stats every 5 seconds
        self.partitions_processed = set()
        self.logger.info(f"Initializing consumer with bootstrap servers: {self.bootstrap_servers}")

    async def start(self):
        retries = 0
        while retries < self.max_init_retries:
            try:
                self.logger.info(f"Creating consumer instance (attempt {retries + 1}/{self.max_init_retries})...")
                self.consumer = AIOKafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=CONSUMER_GROUP,
                    client_id=f'dcgm-metrics-consumer-{self.consumer_id}',
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    auto_commit_interval_ms=1000,  # Commit more frequently
                    max_poll_records=self.batch_size,
                    max_partition_fetch_bytes=BATCH_SIZE,  # 1MB
                    fetch_max_wait_ms=500,  # Wait up to 500ms for data
                    fetch_max_bytes=52428800,  # 50MB max fetch
                    check_crcs=False,  # Disable CRC checks for better performance
                    session_timeout_ms=30000,  # Reduced from 60000
                    heartbeat_interval_ms=10000,  # Reduced from 20000
                    request_timeout_ms=70000,  # Increased from 40000
                    max_poll_interval_ms=300000,  # 5 minutes max poll interval
                    group_instance_id=None,  # Remove static group membership
                    api_version="2.4.0"  # Explicitly set API version
                )
                
                self.logger.info("Starting consumer...")
                await self.consumer.start()
                self.logger.info("Consumer started, waiting for partition assignment...")
                
                # Wait for partition assignment with timeout
                partition_timeout = 30
                partition_start = time.time()
                while time.time() - partition_start < partition_timeout:
                    partitions = self.consumer.assignment()
                    if partitions:
                        self.logger.info(f"Consumer assigned partitions: {partitions}")
                        self.logger.info("Consumer initialized successfully")
                        print("Consumer initialized successfully")
                        sys.stdout.flush()
                        return True
                    await asyncio.sleep(1)
                
                raise RuntimeError("No partitions assigned after timeout")
                
            except Exception as e:
                self.logger.error(f"Error during consumer initialization: {str(e)}", exc_info=True)
                if self.consumer:
                    await self.consumer.stop()
                retries += 1
                if retries < self.max_init_retries:
                    delay = self.init_retry_delay * (2 ** retries)  # Exponential backoff
                    self.logger.info(f"Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
                else:
                    error_msg = f"Failed to initialize consumer after {self.max_init_retries} attempts"
                    self.logger.error(error_msg)
                    print(error_msg, file=sys.stderr)
                    sys.exit(1)

    def _on_join_failed(self, error):
        """Callback for group join failures"""
        self.logger.error(f"Group join failed: {error}")

    async def _on_partitions_assigned(self, assigned):
        """Callback when partitions are assigned"""
        self.partitions_processed.update(assigned)
        self.logger.info(f"Assigned partitions: {assigned}")

    async def _collect_stats(self):
        """Periodically collect and log performance statistics"""
        while True:
            try:
                current_time = time.time()
                if current_time - self.last_stats_time >= self.stats_interval:
                    elapsed = current_time - self.start_time
                    rate = self.message_count / elapsed
                    cpu_percent = self.process.cpu_percent()
                    memory_info = self.process.memory_info()
                    
                    self.logger.info(
                        f"Stats: Rate={rate:.2f} msg/s, CPU={cpu_percent:.1f}%, "
                        f"Memory={memory_info.rss/1024/1024:.1f}MB, "
                        f"Partitions={len(self.partitions_processed)}, "
                        f"Total Messages={self.message_count}"
                    )
                    self.last_stats_time = current_time
                
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.error(f"Error collecting stats: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def consume(self):
        try:
            batch = []
            async for message in self.consumer:
                try:
                    batch.append(message.value.decode('utf-8'))
                    
                    if len(batch) >= self.batch_size:
                        await self.process_batch(batch)
                        batch = []
                        
                except Exception as e:
                    print(f"Error processing message: {e}", file=sys.stderr)
                    continue

        except asyncio.CancelledError:
            if batch:  # Process remaining messages
                await self.process_batch(batch)
            await self.stop()
        except Exception as e:
            print(f"Error in consumer loop: {e}", file=sys.stderr)
            await self.stop()
            sys.exit(1)

    async def process_batch(self, batch):
        """Process a batch of messages efficiently"""
        batch_start = time.time()
        processed_count = 0
        error_count = 0
        
        try:
            for message in batch:
                try:
                    metrics = message.split('\n')
                    for metric in metrics:
                        if not metric:
                            continue
                            
                        fields = metric.split('|')
                        if len(fields) == 6:
                            processed_count += 1
                        else:
                            error_count += 1
                            
                except Exception as e:
                    error_count += 1
                    self.logger.warning(f"Error processing message: {e}")
            
            self.message_count += processed_count
            batch_time = time.time() - batch_start
            batch_rate = len(batch) / batch_time
            
            if error_count > 0:
                self.logger.warning(
                    f"Batch processing stats: Size={len(batch)}, "
                    f"Time={batch_time:.3f}s, Rate={batch_rate:.2f} msg/s, "
                    f"Errors={error_count}"
                )
            else:
                self.logger.debug(
                    f"Batch processing stats: Size={len(batch)}, "
                    f"Time={batch_time:.3f}s, Rate={batch_rate:.2f} msg/s"
                )
                
        except Exception as e:
            self.logger.error(f"Batch processing failed: {e}", exc_info=True)
            raise

    async def stop(self):
        """Clean shutdown of consumer"""
        try:
            if hasattr(self, 'stats_task'):
                self.stats_task.cancel()
                try:
                    await self.stats_task
                except asyncio.CancelledError:
                    pass
                    
            if self.consumer:
                self.logger.info("Closing consumer...")
                await self.consumer.stop()
                self.logger.info("Consumer closed")
                
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}", exc_info=True)

async def main():
    consumer = MetricsConsumer()
    await consumer.start()
    await consumer.consume()

if __name__ == '__main__':
    asyncio.run(main())
