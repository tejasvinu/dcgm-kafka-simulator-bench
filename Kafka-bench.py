import asyncio
import time
import logging
import statistics
from typing import List, Dict
from aiokafka import AIOKafkaConsumer
from server_emulator import DCGMServerEmulator
from producer import MetricsProducer
from config import (NUM_SERVERS, GPUS_PER_SERVER, METRICS_INTERVAL,
                   KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaBenchmark:
    def __init__(self, duration: int = 300):
        """
        Initialize benchmark parameters
        duration: benchmark duration in seconds
        """
        self.duration = duration
        self.producer_stats: Dict[str, List[float]] = {
            'latency': [],
            'throughput': []
        }
        self.consumer_stats: Dict[str, List[float]] = {
            'latency': [],
            'throughput': []
        }
        self.producer = MetricsProducer()
        self.messages_sent = 0
        self.messages_received = 0
        self.start_time = 0
        self.should_stop = False

    async def producer_task(self, server_id: int):
        """Simulate server sending metrics"""
        emulator = DCGMServerEmulator(server_id)
        end_time = self.start_time + self.duration
        
        while time.time() < end_time and not self.should_stop:
            for gpu_id in range(emulator.num_gpus):
                if time.time() >= end_time or self.should_stop:
                    break
                    
                start_time = time.time()
                metric = emulator.generate_metric(gpu_id)
                
                try:
                    await self.producer.send_metric(metric)
                    end_time_metric = time.time()
                    self.producer_stats['latency'].append(end_time_metric - start_time)
                    self.messages_sent += 1
                except Exception as e:
                    logger.error(f"Producer error: {e}")
                
            await asyncio.sleep(METRICS_INTERVAL)

    async def consumer_task(self):
        """Consume metrics and measure performance"""
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id='benchmark_group',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        
        try:
            await consumer.start()
            end_time = self.start_time + self.duration
            
            while time.time() < end_time and not self.should_stop:
                try:
                    # Set timeout to ensure we don't block indefinitely
                    start_time = time.time()
                    msg = await asyncio.wait_for(
                        consumer.getone(),
                        timeout=1.0
                    )
                    end_time_msg = time.time()
                    self.consumer_stats['latency'].append(end_time_msg - start_time)
                    self.messages_received += 1
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"Consumer error: {e}")
                    
        finally:
            await consumer.stop()

    async def shutdown(self):
        """Gracefully shutdown the benchmark"""
        self.should_stop = True
        logger.info("Initiating benchmark shutdown...")
        await self.producer.close()

    def calculate_stats(self):
        """Calculate and return benchmark statistics"""
        duration = time.time() - self.start_time
        producer_throughput = self.messages_sent / duration
        consumer_throughput = self.messages_received / duration
        
        stats = {
            'producer': {
                'messages_sent': self.messages_sent,
                'throughput_msgs_per_sec': producer_throughput,
                'avg_latency_ms': statistics.mean(self.producer_stats['latency']) * 1000 if self.producer_stats['latency'] else 0,
                'p95_latency_ms': statistics.quantiles(self.producer_stats['latency'], n=20)[-1] * 1000 if self.producer_stats['latency'] else 0,
            },
            'consumer': {
                'messages_received': self.messages_received,
                'throughput_msgs_per_sec': consumer_throughput,
                'avg_latency_ms': statistics.mean(self.consumer_stats['latency']) * 1000 if self.consumer_stats['latency'] else 0,
                'p95_latency_ms': statistics.quantiles(self.consumer_stats['latency'], n=20)[-1] * 1000 if self.consumer_stats['latency'] else 0,
            }
        }
        return stats

    async def run_benchmark(self):
        """Run the benchmark"""
        logger.info(f"Starting Kafka benchmark for {self.duration} seconds...")
        await self.producer.start()
        
        try:
            self.start_time = time.time()
            
            # Create tasks for producers and consumer
            producer_tasks = [
                asyncio.create_task(self.producer_task(server_id), name=f"producer-{server_id}")
                for server_id in range(NUM_SERVERS)
            ]
            consumer_task = asyncio.create_task(self.consumer_task(), name="consumer")
            timeout_task = asyncio.create_task(asyncio.sleep(self.duration), name="timeout")
            
            all_tasks = producer_tasks + [consumer_task, timeout_task]
            
            try:
                # Run all tasks concurrently until timeout
                done, pending = await asyncio.wait(
                    all_tasks,
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # Cancel remaining tasks
                self.should_stop = True
                for task in pending:
                    if not task.done():
                        task.cancel()
                
                # Wait for tasks to finish
                if pending:
                    await asyncio.wait(pending)
                
            except asyncio.CancelledError:
                self.should_stop = True
                # Cancel all tasks
                for task in all_tasks:
                    if not task.done():
                        task.cancel()
                # Wait for tasks to finish
                await asyncio.wait(all_tasks)
                raise
            
            # Calculate and display results
            stats = self.calculate_stats()
            logger.info("Benchmark Results:")
            logger.info(f"Producer Statistics:")
            logger.info(f"  Messages Sent: {stats['producer']['messages_sent']}")
            logger.info(f"  Throughput: {stats['producer']['throughput_msgs_per_sec']:.2f} msgs/sec")
            logger.info(f"  Average Latency: {stats['producer']['avg_latency_ms']:.2f} ms")
            logger.info(f"  P95 Latency: {stats['producer']['p95_latency_ms']:.2f} ms")
            
            logger.info(f"Consumer Statistics:")
            logger.info(f"  Messages Received: {stats['consumer']['messages_received']}")
            logger.info(f"  Throughput: {stats['consumer']['throughput_msgs_per_sec']:.2f} msgs/sec")
            logger.info(f"  Average Latency: {stats['consumer']['avg_latency_ms']:.2f} ms")
            logger.info(f"  P95 Latency: {stats['consumer']['p95_latency_ms']:.2f} ms")
            
        except asyncio.CancelledError:
            logger.info("Benchmark cancelled.")
        except Exception as e:
            logger.error(f"Benchmark error: {e}")
        finally:
            await self.shutdown()

def main():
    benchmark = KafkaBenchmark(duration=300)  # 5 minutes benchmark
    try:
        asyncio.run(benchmark.run_benchmark())
    except KeyboardInterrupt:
        logger.info("Benchmark interrupted by user.")

if __name__ == "__main__":
    main()