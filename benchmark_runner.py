import asyncio
import logging
import json
from datetime import datetime
from kafka_bench import run_benchmark
import config  # Direct import of config module
from config import (
    KAFKA_PORTS, KAFKA_TOPIC, 
    SERVER_SCALE_CONFIGS,
    calculate_num_consumers
)
from benchmark_report import generate_html_report
import os
import re
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DURATION_SECONDS = 300  # 5 minutes
RUNS_PER_CONFIG = 3  # Number of runs per configuration for averaging

def setup_logging(timestamp):
    """Configure logging with file and console output"""
    log_dir = "benchmark_logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = f"{log_dir}/benchmark_{timestamp}.log"
    
    # Create handlers
    file_handler = logging.FileHandler(filename=log_file)
    console_handler = logging.StreamHandler(sys.stdout)
    
    # Create formatters and add it to handlers
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    file_handler.setFormatter(logging.Formatter(log_format))
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # Get root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return log_file

class BenchmarkRunner:
    def __init__(self):
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.results_dir = "benchmark_results"
        self.log_file = setup_logging(self.timestamp)
        os.makedirs(f"{self.results_dir}/{self.timestamp}", exist_ok=True)
        
        self.server_counts = SERVER_SCALE_CONFIGS
        self.results = []
        
        # Test phase durations
        self.warmup_duration = 60     # 1 min warmup
        self.test_duration = 300      # 5 min test
        self.cooldown_duration = 60   # 1 min cooldown
        
        logging.info(f"Initialized benchmark runner with server scales: {self.server_counts}")

    async def run_benchmark(self, num_servers):
        """Run benchmark with specified number of servers"""
        try:
            # Update configuration
            num_consumers = calculate_num_consumers(num_servers)
            logging.info(f"Starting benchmark with {num_servers} servers and {num_consumers} consumers")
            
            # Update config.py with new values
            with open("config.py", "r") as f:
                config_content = f.read()
            
            updated_config = re.sub(
                r'NUM_SERVERS = \d+',
                f'NUM_SERVERS = {num_servers}',
                config_content
            )
            updated_config = re.sub(
                r'NUM_CONSUMERS = \d+',
                f'NUM_CONSUMERS = {num_consumers}',
                updated_config
            )
            
            with open("config.py", "w") as f:
                f.write(updated_config)
            
            # Run the actual benchmark
            result = await run_benchmark(DURATION_SECONDS, num_servers)
            return result
            
        except Exception as e:
            logging.error(f"Error running benchmark with {num_servers} servers: {e}")
            raise

async def run_configuration_benchmark(num_servers):
    """Run multiple benchmark iterations for a server configuration"""
    results = []
    max_retries = 3
    
    for run in range(RUNS_PER_CONFIG):
        retry_count = 0
        while retry_count < max_retries:
            try:
                logger.info(f"Starting benchmark run {run + 1}/{RUNS_PER_CONFIG} "
                          f"with {num_servers} servers (attempt {retry_count + 1})")
                
                result = await run_benchmark(DURATION_SECONDS, num_servers)
                
                if result["avg_latency_ms"] == 0:
                    raise ValueError("Invalid latency measurements detected")
                    
                results.append(result)
                break  # Success, exit retry loop
                
            except Exception as e:
                retry_count += 1
                logger.error(f"Benchmark attempt failed: {e}")
                if retry_count < max_retries:
                    logger.info(f"Retrying in 10 seconds...")
                    await asyncio.sleep(10)
                else:
                    logger.error(f"All retry attempts failed for {num_servers} servers")
                    raise
        
        await asyncio.sleep(10)  # Wait between runs
    
    # Calculate averages
    return {
        "num_servers": num_servers,
        "total_messages": sum(r["total_messages"] for r in results) / len(results),
        "messages_per_second": sum(r["messages_per_second"] for r in results) / len(results),
        "avg_latency_ms": sum(r["avg_latency_ms"] for r in results) / len(results),
        "p95_latency_ms": sum(r["p95_latency_ms"] for r in results) / len(results),
        "p99_latency_ms": sum(r["p99_latency_ms"] for r in results) / len(results)
    }

async def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"benchmark_results_{timestamp}.json"
    all_results = []

    try:
        # Changed SERVER_CONFIGS to SERVER_SCALE_CONFIGS
        for num_servers in SERVER_SCALE_CONFIGS:
            result = await run_configuration_benchmark(num_servers)
            all_results.append(result)
            
            # Save intermediate results
            with open(results_file, 'w') as f:
                json.dump(all_results, f, indent=2)
            
            logger.info(f"Results for {num_servers} servers:")
            logger.info(f"Messages/sec: {result['messages_per_second']:.2f}")
            logger.info(f"Avg Latency: {result['avg_latency_ms']:.2f} ms")
            logger.info(f"P95 Latency: {result['p95_latency_ms']:.2f} ms")
            logger.info(f"P99 Latency: {result['p99_latency_ms']:.2f} ms")
            logger.info("-" * 50)
            
            # Cool down period between configurations
            await asyncio.sleep(30)
            
    except Exception as e:
        logger.error(f"Benchmark failed: {e}")
        raise  # Re-raise the exception for debugging
    finally:
        # Generate reports
        config_data = {
            "Kafka Brokers": len(KAFKA_PORTS),  # Using imported KAFKA_PORTS
            "Topic": KAFKA_TOPIC,               # Using imported KAFKA_TOPIC
            "Duration per test": f"{DURATION_SECONDS} seconds",
            "Runs per configuration": RUNS_PER_CONFIG,
            "Test Date": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Generate HTML report
        html_content = generate_html_report(
            all_results,
            config_data,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
        
        html_file = f"benchmark_report_{timestamp}.html"
        with open(html_file, 'w') as f:
            f.write(html_content)
        
        logger.info(f"HTML report generated: {html_file}")
        
        # Also generate the markdown report for compatibility
        generate_report(all_results, timestamp)

def generate_report(results, timestamp):
    report_file = f"benchmark_report_{timestamp}.md"
    
    with open(report_file, 'w') as f:
        f.write("# Kafka Benchmark Results\n\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Configuration table
        f.write("## Test Configuration\n\n")
        f.write("| Parameter | Value |\n")
        f.write("|-----------|-------|\n")
        f.write(f"| Kafka Brokers | {len(config.KAFKA_PORTS)} |\n")
        f.write(f"| Topic | {config.KAFKA_TOPIC} |\n")
        f.write(f"| Duration per test | {DURATION_SECONDS} seconds |\n")
        f.write(f"| Runs per configuration | {RUNS_PER_CONFIG} |\n\n")
        
        # Results table
        f.write("## Results\n\n")
        f.write("| Servers | Messages/sec | Avg Latency (ms) | P95 Latency (ms) | P99 Latency (ms) |\n")
        f.write("|---------|--------------|------------------|------------------|------------------|\n")
        
        for r in results:
            f.write(f"| {r['num_servers']:>7} | {r['messages_per_second']:>12.2f} | "
                   f"{r['avg_latency_ms']:>16.2f} | {r['p95_latency_ms']:>16.2f} | "
                   f"{r['p99_latency_ms']:>16.2f} |\n")

if __name__ == "__main__":
    asyncio.run(main())
