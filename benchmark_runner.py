import asyncio
import logging
import json
from datetime import datetime
from Kafka_bench import run_benchmark
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
    # Replace f-string with format() for compatibility with older Python versions
    log_file = "{}/benchmark_{}.log".format(log_dir, timestamp)
    
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
        os.makedirs("{}/{}".format(self.results_dir, self.timestamp), exist_ok=True)
        
        self.server_counts = SERVER_SCALE_CONFIGS
        self.results = []
        
        # Test phase durations
        self.warmup_duration = 60     # 1 min warmup
        self.test_duration = 300      # 5 min test
        self.cooldown_duration = 60   # 1 min cooldown
        
        logging.info("Initialized benchmark runner with server scales: {}".format(self.server_counts))

    async def run_benchmark(self, num_servers):
        """Run benchmark with specified number of servers"""
        try:
            # Update configuration
            num_consumers = calculate_num_consumers(num_servers)
            logging.info("Starting benchmark with {} servers and {} consumers".format(num_servers, num_consumers))
            
            # Update config.py with new values
            with open("config.py", "r") as f:
                config_content = f.read()
            
            updated_config = re.sub(
                r'NUM_SERVERS = \d+',
                'NUM_SERVERS = {}'.format(num_servers),
                config_content
            )
            updated_config = re.sub(
                r'NUM_CONSUMERS = \d+',
                'NUM_CONSUMERS = {}'.format(num_consumers),
                updated_config
            )
            
            with open("config.py", "w") as f:
                f.write(updated_config)
            
            # Run the actual benchmark
            result = await run_benchmark(DURATION_SECONDS, num_servers)
            return result
            
        except Exception as e:
            logging.error("Error running benchmark with {} servers: {}".format(num_servers, e))
            raise

async def run_configuration_benchmark(num_servers):
    """Run multiple benchmark iterations for a server configuration"""
    results = []
    max_retries = 3
    
    for run in range(RUNS_PER_CONFIG):
        retry_count = 0
        while retry_count < max_retries:
            try:
                logger.info("Starting benchmark run {}/{} with {} servers (attempt {})".format(
                    run + 1, RUNS_PER_CONFIG, num_servers, retry_count + 1))
                
                # Fixed: Use consistent parameter names
                result = await run_benchmark(
                    duration_seconds=DURATION_SECONDS,
                    num_servers=num_servers
                )
                
                if result["avg_latency_ms"] == 0:
                    raise ValueError("Invalid latency measurements detected")
                    
                results.append(result)
                break  # Success, exit retry loop
                
            except Exception as e:
                retry_count += 1
                logger.error("Benchmark attempt failed: {}".format(e))
                if retry_count < max_retries:
                    logger.info("Retrying in 10 seconds...")
                    await asyncio.sleep(10)
                else:
                    logger.error("All retry attempts failed for {} servers".format(num_servers))
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
    results_file = "benchmark_results_{}.json".format(timestamp)
    all_results = []

    try:
        # Changed SERVER_CONFIGS to SERVER_SCALE_CONFIGS
        for num_servers in SERVER_SCALE_CONFIGS:
            result = await run_configuration_benchmark(num_servers)
            all_results.append(result)
            
            # Save intermediate results
            with open(results_file, 'w') as f:
                json.dump(all_results, f, indent=2)
            
            logger.info("Results for {} servers:".format(num_servers))
            logger.info("Messages/sec: {:.2f}".format(result['messages_per_second']))
            logger.info("Avg Latency: {:.2f} ms".format(result['avg_latency_ms']))
            logger.info("P95 Latency: {:.2f} ms".format(result['p95_latency_ms']))
            logger.info("P99 Latency: {:.2f} ms".format(result['p99_latency_ms']))
            logger.info("-" * 50)
            
            # Cool down period between configurations
            await asyncio.sleep(30)
            
    except Exception as e:
        logger.error("Benchmark failed: {}".format(e))
        raise  # Re-raise the exception for debugging
    finally:
        # Generate reports
        config_data = {
            "Kafka Brokers": len(KAFKA_PORTS),  # Using imported KAFKA_PORTS
            "Topic": KAFKA_TOPIC,               # Using imported KAFKA_TOPIC
            "Duration per test": "{} seconds".format(DURATION_SECONDS),
            "Runs per configuration": RUNS_PER_CONFIG,
            "Test Date": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Generate HTML report
        html_content = generate_html_report(
            all_results,
            config_data,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
        
        html_file = "benchmark_report_{}.html".format(timestamp)
        with open(html_file, 'w') as f:
            f.write(html_content)
        
        logger.info("HTML report generated: {}".format(html_file))
        
        # Also generate the markdown report for compatibility
        generate_report(all_results, timestamp)

def generate_report(results, timestamp):
    report_file = "benchmark_report_{}.md".format(timestamp)
    
    with open(report_file, 'w') as f:
        f.write("# Kafka Benchmark Results\n\n")
        f.write("Date: {}\n\n".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        
        # Configuration table
        f.write("## Test Configuration\n\n")
        f.write("| Parameter | Value |\n")
        f.write("|-----------|-------|\n")
        f.write("| Kafka Brokers | {} |\n".format(len(config.KAFKA_PORTS)))
        f.write("| Topic | {} |\n".format(config.KAFKA_TOPIC))
        f.write("| Duration per test | {} seconds |\n".format(DURATION_SECONDS))
        f.write("| Runs per configuration | {} |\n\n".format(RUNS_PER_CONFIG))
        
        # Results table
        f.write("## Results\n\n")
        f.write("| Servers | Messages/sec | Avg Latency (ms) | P95 Latency (ms) | P99 Latency (ms) |\n")
        f.write("|---------|--------------|------------------|------------------|------------------|\n")
        
        for r in results:
            f.write("| {:>7} | {:>12.2f} | {:>16.2f} | {:>16.2f} | {:>16.2f} |\n".format(
                r['num_servers'], r['messages_per_second'], r['avg_latency_ms'],
                r['p95_latency_ms'], r['p99_latency_ms']))

if __name__ == "__main__":
    asyncio.run(main())
