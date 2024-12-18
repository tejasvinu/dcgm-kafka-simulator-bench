import asyncio
from aiokafka import AIOKafkaConsumer
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
import time
import sys
import logging

logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')

class MetricsConsumer:
    def __init__(self):
        self.bootstrap_servers = ','.join(KAFKA_BOOTSTRAP_SERVERS)
        self.consumer = None
        self.message_count = 0
        self.start_time = time.time()
        self.batch_size = 1000  # Process messages in batches
        self.max_init_retries = 3
        self.init_retry_delay = 5
        logging.info(f"Initializing consumer with bootstrap servers: {self.bootstrap_servers}")

    async def start(self):
        retries = 0
        while retries < self.max_init_retries:
            try:
                logging.info(f"Creating consumer instance (attempt {retries + 1}/{self.max_init_retries})...")
                self.consumer = AIOKafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id='dcgm-metrics-group',
                    client_id='dcgm-metrics-consumer',
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    auto_commit_interval_ms=5000,  # Commit every 5 seconds
                    max_poll_records=self.batch_size,
                    max_partition_fetch_bytes=1048576,  # 1MB
                    fetch_max_wait_ms=500,  # Wait up to 500ms for data
                    fetch_max_bytes=52428800,  # 50MB max fetch
                    check_crcs=False,  # Disable CRC checks for better performance
                    session_timeout_ms=60000,  # Increased from 30000
                    heartbeat_interval_ms=20000,  # Increased from 10000
                    request_timeout_ms=70000,  # Increased from 40000
                    max_poll_interval_ms=300000,  # 5 minutes max poll interval
                    group_instance_id=f"consumer-{int(time.time())}"  # Add unique instance ID
                )
                logging.info("Starting consumer...")
                await self.consumer.start()
                logging.info("Consumer started, waiting for partition assignment...")
                
                # Wait for partition assignment
                await asyncio.sleep(5)
                
                partitions = self.consumer.assignment()
                if not partitions:
                    raise RuntimeError("No partitions assigned after startup")
                
                logging.info(f"Consumer assigned partitions: {partitions}")
                logging.info("Consumer initialized successfully")
                print("Consumer initialized successfully")
                sys.stdout.flush()
                return
                
            except Exception as e:
                logging.error(f"Error during consumer initialization: {e}")
                if self.consumer:
                    await self.consumer.stop()
                retries += 1
                if retries < self.max_init_retries:
                    logging.info(f"Retrying in {self.init_retry_delay} seconds...")
                    await asyncio.sleep(self.init_retry_delay)
                else:
                    error_msg = f"Failed to initialize consumer after {self.max_init_retries} attempts"
                    logging.error(error_msg)
                    print(error_msg, file=sys.stderr)
                    sys.exit(1)

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
        for message in batch:
            metrics = message.split('\n')
            for metric in metrics:
                if not metric:
                    continue
                    
                fields = metric.split('|')
                if len(fields) == 6:
                    timestamp, server_id, gpu_id, temp, util, mem = fields
                    print(f"Server: {server_id}, GPU: {gpu_id}, Temp: {temp}°C, Util: {util}%, Mem: {mem}GB")
                    sys.stdout.flush()
            
            self.message_count += 1
            if self.message_count % 1000 == 0:
                elapsed = time.time() - self.start_time
                rate = self.message_count / elapsed
                print(f"Processing rate: {rate:.2f} messages/second")
                sys.stdout.flush()

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()

async def main():
    consumer = MetricsConsumer()
    await consumer.start()
    await consumer.consume()

if __name__ == '__main__':
    asyncio.run(main())
