import asyncio
import logging
import time
import statistics
import argparse
from consumer import consume_metrics
from server_emulator import main as server_main

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BenchmarkMetrics:
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.message_count = 0
        self.latencies = []
        self.total_latency = 0  # Add this field

    def add_message(self, latency_ms):
        self.message_count += 1
        self.latencies.append(latency_ms)
        self.total_latency += latency_ms  # Update total latency

    def get_results(self):
        duration = self.end_time - self.start_time
        avg_latency = self.total_latency / self.message_count if self.message_count > 0 else 0
        throughput = self.message_count / duration if duration > 0 else 0
        
        return {
            "duration_seconds": duration,
            "total_messages": self.message_count,
            "messages_per_second": throughput,
            "avg_latency_ms": avg_latency,
            "p95_latency_ms": statistics.quantiles(self.latencies, n=20)[18] if self.latencies else 0,
            "p99_latency_ms": statistics.quantiles(self.latencies, n=100)[98] if self.latencies else 0
        }

async def run_benchmark(duration_seconds=30, num_servers=None):
    """Run the consumer and server emulator concurrently for a given duration."""
    metrics = BenchmarkMetrics()
    metrics.start_time = time.time()

    consumer_task = asyncio.create_task(consume_metrics(metrics))
    # Pass num_servers to server_main
    server_task = asyncio.create_task(server_main(num_servers))

    logger.info(f"Benchmark started. Running for {duration_seconds} seconds...")
    await asyncio.sleep(duration_seconds)
    
    metrics.end_time = time.time()
    consumer_task.cancel()
    server_task.cancel()
    
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass
    
    try:
        await server_task
    except asyncio.CancelledError:
        pass

    results = metrics.get_results()
    logger.info("Benchmark Results:")
    for key, value in results.items():
        logger.info(f"{key}: {value}")
        
    return results  # Return results for the automation script

def main():
    parser = argparse.ArgumentParser(description='Kafka Benchmark Tool')
    parser.add_argument('--duration', type=int, default=30,
                      help='Duration of the benchmark in seconds')
    args = parser.parse_args()
    
    try:
        asyncio.run(run_benchmark(args.duration))
    except KeyboardInterrupt:
        logger.info("Benchmark stopped by user")
    except Exception as e:
        logger.error(f"Benchmark failed: {e}")

if __name__ == "__main__":
    main()
