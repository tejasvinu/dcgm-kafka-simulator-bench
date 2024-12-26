import asyncio
from aiokafka import AIOKafkaConsumer
from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
    CONSUMER_GROUP, STATS_INTERVAL
)
import time
import sys
import logging
import psutil
import os
from datetime import datetime
from kafka.errors import KafkaError, UnknownError
import backoff

def setup_logging():
    log_dir = "consumer_logs"
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"{log_dir}/consumer_{os.getpid()}_{timestamp}.log"
    
    # Create handlers
    file_handler = logging.FileHandler(filename=log_file)
    console_handler = logging.StreamHandler(sys.stdout)
    
    # Create formatters and add it to handlers
    log_format = '%(asctime)s - %(name)s - %(levelname)s - PID:%(process)d - %(message)s'
    file_handler.setFormatter(logging.Formatter(log_format))
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # Get root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return log_file

class MetricsConsumer:
    def __init__(self):
        self.log_file = setup_logging()  # Set up logging
        self.bootstrap_servers = ','.join(KAFKA_BOOTSTRAP_SERVERS)
        self.consumer = None
        self.message_count = 0
        self.start_time = time.time()
        self.max_init_retries = 3
        self.init_retry_delay = 5
        self.process = psutil.Process(os.getpid())
        self.consumer_id = f"{os.getpid()}_{int(time.time())}"
        self.logger = logging.getLogger(f'Consumer-{self.consumer_id}')
        self.last_stats_time = time.time()
        self.stats_interval = STATS_INTERVAL  # Log stats every 5 seconds
        self.partitions_processed = set()
        self.logger.info(f"Initializing consumer with bootstrap servers: {self.bootstrap_servers}")
        self.max_fetch_retries = 5
        self.fetch_retry_delay = 2
        self.health_check_interval = 30  # seconds
        self.last_successful_fetch = time.time()
        self.consecutive_errors = 0
        self.max_consecutive_errors = 10

    @backoff.on_exception(backoff.expo,
                         (KafkaError, UnknownError),
                         max_tries=5,
                         max_time=30)
    async def _fetch_with_retry(self):
        try:
            message = await self.consumer.__anext__()
            self.last_successful_fetch = time.time()
            self.consecutive_errors = 0
            return message
        except Exception as e:
            self.consecutive_errors += 1
            self.logger.warning(f"Fetch error (attempt {self.consecutive_errors}): {str(e)}")
            if self.consecutive_errors >= self.max_consecutive_errors:
                self.logger.error("Too many consecutive errors, forcing consumer restart")
                await self.restart_consumer()
            raise

    async def restart_consumer(self):
        """Restart the consumer connection"""
        self.logger.info("Restarting consumer connection...")
        if self.consumer:
            await self.consumer.stop()
        
        # Wait before reconnecting
        await asyncio.sleep(5)
        
        # Reinitialize consumer
        self.consumer = None
        await self.start()
        self.consecutive_errors = 0
        self.logger.info("Consumer connection restarted")

    async def check_connection_health(self):
        """Check if the consumer connection is healthy"""
        if time.time() - self.last_successful_fetch > self.health_check_interval:
            self.logger.warning("No successful fetches in health check interval")
            return False
        return True

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
                    auto_commit_interval_ms=1000,
                    fetch_max_wait_ms=500,
                    fetch_max_bytes=52428800,  # 50MB max fetch
                    check_crcs=False,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000,
                    request_timeout_ms=70000,
                    max_poll_interval_ms=300000,
                    group_instance_id=None,
                    api_version="2.4.0"
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
        """Modified consume method with better error handling"""
        self.stats_task = asyncio.create_task(self._collect_stats())
        self.health_check_task = asyncio.create_task(self._health_check_loop())
        
        try:
            while True:
                try:
                    message = await self._fetch_with_retry()
                    await self.process_message(message.value.decode('utf-8'))
                except asyncio.CancelledError:
                    self.logger.info("Consumer cancelled")
                    break
                except Exception as e:
                    self.logger.error(f"Error in consumer loop: {e}", exc_info=True)
                    # Check if we should continue or exit
                    if self.consecutive_errors >= self.max_consecutive_errors:
                        self.logger.error("Maximum error threshold reached, exiting")
                        break
                    await asyncio.sleep(self.fetch_retry_delay)
        finally:
            await self.stop()

    async def _health_check_loop(self):
        """Periodic health check loop"""
        while True:
            try:
                if not await self.check_connection_health():
                    self.logger.warning("Health check failed, attempting restart")
                    await self.restart_consumer()
                await asyncio.sleep(self.health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in health check loop: {e}")
                await asyncio.sleep(5)

    async def process_message(self, message):
        """Process a single message"""
        try:
            metrics = message.split('\n')
            for metric in metrics:
                if not metric:
                    continue
                    
                fields = metric.split('|')
                if len(fields) == 6:
                    self.message_count += 1
                    
        except Exception as e:
            self.logger.warning(f"Error processing message: {e}")
            raise

    async def stop(self):
        """Enhanced stop method"""
        try:
            self.logger.info("Stopping consumer...")
            
            # Cancel background tasks
            if hasattr(self, 'stats_task'):
                self.stats_task.cancel()
                try:
                    await self.stats_task
                except asyncio.CancelledError:
                    pass
                    
            if hasattr(self, 'health_check_task'):
                self.health_check_task.cancel()
                try:
                    await self.health_check_task
                except asyncio.CancelledError:
                    pass
                    
            if self.consumer:
                self.logger.info("Closing consumer connection...")
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
