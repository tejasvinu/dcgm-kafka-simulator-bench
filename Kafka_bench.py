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
        self.consumer_stats: Dict[str, List[float]] = {
            'latency': [],
            'throughput': []
        }
        self.messages_received = 0
        self.start_time = 0
        self.should_stop = False
        self.batch_size = 100  # Batch size for statistics collection
        self.stats_flush_interval = 10  # Flush stats every 10 seconds

    def _flush_stats(self):
        """Flush statistics to prevent memory growth"""
        if len(self.consumer_stats['latency']) > 10000:
            self.consumer_stats['latency'] = self.consumer_stats['latency'][-10000:]

    async def consumer_task(self):
        consumer = None
        try:
            consumer = AIOKafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id='benchmark_group',
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000
            )
            
            await consumer.start()
            end_time = self.start_time + self.duration
            
            while time.time() < end_time and not self.should_stop:
                try:
                    msg = await asyncio.wait_for(
                        consumer.getone(),
                        timeout=1.0
                    )
                    self.messages_received += 1
                except asyncio.TimeoutError:
                    if time.time() >= end_time:
                        break
                    continue
                except Exception as e:
                    logger.error(f"Consumer error: {e}")
                    if self.should_stop:
                        break
        finally:
            if consumer:
                await consumer.stop()

    async def shutdown(self):
        """Gracefully shutdown the benchmark"""
        self.should_stop = True
        logger.info("Initiating benchmark shutdown...")

    def calculate_stats(self):
        """Calculate and return benchmark statistics"""
        duration = time.time() - self.start_time
        consumer_throughput = self.messages_received / duration
        
        stats = {
            'consumer': {
                'messages_received': self.messages_received,
                'throughput_msgs_per_sec': consumer_throughput,
                'avg_latency_ms': statistics.mean(self.consumer_stats['latency']) * 1000 if self.consumer_stats['latency'] else 0,
                'p95_latency_ms': statistics.quantiles(self.consumer_stats['latency'], n=20)[-1] * 1000 if self.consumer_stats['latency'] else 0,
            }
        }
        return stats

    async def run_benchmark(self):
        """Run the benchmark with only a consumer"""
        logger.info(f"Starting Kafka consumer for {self.duration} seconds...")
        self.start_time = time.time()
        
        try:
            consumer_task = asyncio.create_task(self.consumer_task(), name="consumer")
            timeout_task = asyncio.create_task(asyncio.sleep(self.duration), name="timeout")
            all_tasks = [consumer_task, timeout_task]
            
            done, pending = await asyncio.wait(all_tasks, return_when=asyncio.ALL_COMPLETED)
            self.should_stop = True
            
            # Calculate and display results
            stats = self.calculate_stats()
            logger.info("Benchmark Results:")
            
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