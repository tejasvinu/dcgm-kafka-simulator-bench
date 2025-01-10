import asyncio
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError, KafkaConnectionError, ConsumerStoppedError, UnknownTopicOrPartitionError
from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
    CONSUMER_GROUP, STATS_INTERVAL,
    KAFKA_CONNECTION_CONFIG
)
import time
import sys
import logging
import psutil
import os
from datetime import datetime
import backoff
import signal

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
        self.reconnect_backoff = 5
        self.max_reconnect_attempts = 3
        self.running = True
        self.shutdown_event = asyncio.Event()

    async def handle_shutdown(self, sig):
        """Handle shutdown signals"""
        self.logger.info(f"Received shutdown signal {sig.name}")
        self.running = False
        self.shutdown_event.set()
        await self.stop()

    @backoff.on_exception(backoff.expo,
                         (KafkaError, KafkaConnectionError),
                         max_tries=5,
                         max_time=30)
    async def _fetch_with_retry(self):
        try:
            # Use __anext__ directly instead of anext()
            message = await self.consumer.__anext__()
            self.last_successful_fetch = time.time()
            self.consecutive_errors = 0
            return message
        except StopAsyncIteration:
            # This is normal when consumer is stopping
            self.logger.info("Consumer iteration stopped")
            raise
        except ConsumerStoppedError:
            self.logger.error("Consumer stopped, attempting restart")
            await self.restart_consumer()
            raise
        except KafkaConnectionError as e:
            self.consecutive_errors += 1
            self.logger.warning(f"Connection error (attempt {self.consecutive_errors}): {str(e)}")
            if self.consecutive_errors >= self.max_consecutive_errors:
                self.logger.error("Too many consecutive errors, forcing consumer restart")
                await self.restart_consumer()
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error in fetch: {str(e)}")
            raise

    async def restart_consumer(self):
        """Restart the consumer connection with retry logic"""
        for attempt in range(self.max_reconnect_attempts):
            try:
                self.logger.info(f"Restarting consumer (attempt {attempt + 1}/{self.max_reconnect_attempts})")
                if self.consumer:
                    await self.consumer.stop()
                
                # Wait before reconnecting with exponential backoff
                await asyncio.sleep(self.reconnect_backoff * (2 ** attempt))
                
                # Reinitialize consumer
                self.consumer = None
                success = await self.start()
                if success:
                    self.consecutive_errors = 0
                    self.logger.info("Consumer successfully restarted")
                    return True
            except Exception as e:
                self.logger.error(f"Error during consumer restart: {str(e)}")
                if attempt == self.max_reconnect_attempts - 1:
                    self.logger.error("Max restart attempts reached, giving up")
                    raise

        return False

    async def check_connection_health(self):
        """Check if the consumer connection is healthy using aiokafka methods"""
        try:
            if not self.consumer or not self.consumer.assignment():
                self.logger.warning("Consumer has no partition assignments")
                return False
            
            if time.time() - self.last_successful_fetch > self.health_check_interval:
                self.logger.warning("No successful fetches in health check interval")
                return False

            # Check if consumer's coordinator is connected
            coordinator_id = self.consumer._coordinator.coordinator_id
            if coordinator_id is None:
                self.logger.warning("No coordinator connection")
                return False

            # Check if consumer is connected to at least one broker
            connected = False
            for node_id in self.consumer._client.cluster.brokers():
                if await self.consumer._client.ready(node_id):
                    connected = True
                    break

            if not connected:
                self.logger.warning("Consumer not connected to any broker")
                return False

            return True
        except Exception as e:
            self.logger.error(f"Error checking connection health: {e}")
            return False

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
                    request_timeout_ms=30000,
                    api_version='auto'
                )

                self.logger.info("Starting consumer...")
                await self.consumer.start()
                
                # Wait for partition assignment
                assignment_start = time.time()
                assignment_timeout = 30
                
                while time.time() - assignment_start < assignment_timeout:
                    partitions = self.consumer.assignment()
                    if partitions:
                        self.logger.info(f"Consumer assigned partitions: {partitions}")
                        print("Consumer initialized successfully")
                        return True
                    await asyncio.sleep(1)
                
                raise RuntimeError("No partitions assigned within timeout period")
                
            except Exception as e:
                self.logger.error(f"Error during consumer initialization: {str(e)}", exc_info=True)
                if self.consumer:
                    await self.consumer.stop()
                retries += 1
                if retries < self.max_init_retries:
                    delay = self.init_retry_delay * (2 ** retries)
                    self.logger.info(f"Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
                else:
                    self.logger.error(f"Failed to initialize consumer after {self.max_init_retries} attempts")
                    raise

        return False

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
            while self.running:
                try:
                    message = await self._fetch_with_retry()
                    if message:  # Add null message check
                        await self.process_message(message.value.decode('utf-8'))
                except StopAsyncIteration:
                    self.logger.info("Consumer iteration completed")
                    if await self.restart_consumer():
                        continue
                    else:
                        break
                except asyncio.CancelledError:
                    self.logger.info("Consumer cancelled")
                    break
                except ConsumerStoppedError:
                    if self.running:  # Only retry if not shutting down
                        if await self.restart_consumer():
                            continue
                    break
                except UnknownTopicOrPartitionError:
                    self.logger.error("Topic or partition not found")
                    await asyncio.sleep(5)  # Wait before retry
                    continue
                except Exception as e:
                    self.logger.error(f"Error in consumer loop: {e}", exc_info=True)
                    if self.consecutive_errors >= self.max_consecutive_errors:
                        if not await self.restart_consumer():
                            self.logger.error("Failed to recover, exiting")
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
        """Enhanced stop method with proper cleanup"""
        if not self.running:
            return  # Already stopping
            
        self.running = False
        self.logger.info("Stopping consumer...")
        
        try:
            # Cancel background tasks
            tasks = []
            if hasattr(self, 'stats_task'):
                self.stats_task.cancel()
                tasks.append(self.stats_task)
            if hasattr(self, 'health_check_task'):
                self.health_check_task.cancel()
                tasks.append(self.health_check_task)
                
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            
            # Ensure proper consumer shutdown
            if self.consumer:
                self.logger.info("Closing consumer connection...")
                try:
                    await asyncio.wait_for(self.consumer.stop(), timeout=10.0)
                except asyncio.TimeoutError:
                    self.logger.warning("Consumer stop timed out after 10 seconds")
                self.consumer = None
                self.logger.info("Consumer closed")
                
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}", exc_info=True)
        finally:
            # Clean up signal handlers
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                try:
                    loop.remove_signal_handler(sig)
                except NotImplementedError:
                    pass  # Signal handlers not supported on this platform

async def main():
    consumer = None
    try:
        consumer = MetricsConsumer()
        await consumer.start()
        await consumer.consume()
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
    finally:
        if consumer:
            await consumer.stop()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, shutting down...")
