import asyncio
import logging
import json
import time
from datetime import datetime
from kafka_bench import run_benchmark
import config
from benchmark_report import generate_html_report

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVER_CONFIGS = [4, 8, 16, 32, 64, 128, 256, 512, 1024]
DURATION_SECONDS = 300  # 5 minutes
RUNS_PER_CONFIG = 3  # Number of runs per configuration for averaging

async def run_configuration_benchmark(num_servers):
    results = []
    max_retries = 3
    
    # Use a constant interval: remove or comment out dynamic assignment
    # config.METRICS_INTERVAL = config.get_metrics_interval(num_servers)
    config.METRICS_INTERVAL = 1.0
    
    for run in range(RUNS_PER_CONFIG):
        retry_count = 0
        while retry_count < max_retries:
            try:
                logger.info(f"Starting benchmark run {run + 1}/{RUNS_PER_CONFIG} with {num_servers} servers (attempt {retry_count + 1})")
                
                # Update configuration
                config.NUM_SERVERS = num_servers
                
                # Run benchmark
                result = await run_benchmark(DURATION_SECONDS)
                
                # Verify results are valid
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
                    logger.error(f"All retry attempts failed for {num_servers} servers, run {run + 1}")
                    raise
        
        # Wait between successful runs
        await asyncio.sleep(10)
    
    # Calculate averages
    avg_results = {
        "num_servers": num_servers,
        "total_messages": sum(r["total_messages"] for r in results) / len(results),
        "messages_per_second": sum(r["messages_per_second"] for r in results) / len(results),
        "avg_latency_ms": sum(r["avg_latency_ms"] for r in results) / len(results),
        "p95_latency_ms": sum(r["p95_latency_ms"] for r in results) / len(results),
        "p99_latency_ms": sum(r["p99_latency_ms"] for r in results) / len(results)
    }
    
    return avg_results

async def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"benchmark_results_{timestamp}.json"
    all_results = []

    try:
        for num_servers in SERVER_CONFIGS:
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
    finally:
        # Generate reports
        config_data = {
            "Kafka Brokers": len(config.KAFKA_PORTS),
            "Topic": config.KAFKA_TOPIC,
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
