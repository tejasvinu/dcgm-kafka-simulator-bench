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
        logging.info(f"Initializing consumer with bootstrap servers: {self.bootstrap_servers}")

    async def start(self):
        try:
            logging.info("Creating consumer instance...")
            self.consumer = AIOKafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=self.bootstrap_servers,
                group_id='dcgm-metrics-group',
                auto_offset_reset='latest'
            )
            logging.info("Starting consumer...")
            await self.consumer.start()
            logging.info("Consumer initialized successfully")
            print("Consumer initialized successfully")  # Keep this for benchmark runner
            sys.stdout.flush()
        except Exception as e:
            error_msg = f"Error initializing consumer: {str(e)}"
            logging.error(error_msg)
            print(error_msg, file=sys.stderr)
            sys.exit(1)

    async def consume(self):
        try:
            async for message in self.consumer:
                try:
                    # Parse message value
                    value = message.value.decode('utf-8')
                    # Split batched messages
                    metrics = value.split('\n')
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

                except Exception as e:
                    print(f"Error processing message: {e}", file=sys.stderr)
                    continue

        except asyncio.CancelledError:
            await self.stop()
        except Exception as e:
            print(f"Error in consumer loop: {e}", file=sys.stderr)
            await self.stop()
            sys.exit(1)

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()

async def main():
    consumer = MetricsConsumer()
    await consumer.start()
    await consumer.consume()

if __name__ == '__main__':
    asyncio.run(main())
