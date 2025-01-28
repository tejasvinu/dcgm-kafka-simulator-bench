import asyncio
import logging
import json
import os
import time
from datetime import datetime
from Kafka_bench import KafkaBenchmark
from producer import MetricsProducer
from config import (
    update_num_servers, 
    GPUS_PER_SERVER, 
    METRICS_INTERVAL
)
from report_generator import generate_report
from server_emulator import run_server

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SERVER_SCALES = [4, 8, 32, 64, 128, 256, 512, 1024]
BENCHMARK_DURATION = 300  # 5 minutes per scale
RESULTS_FILE = "benchmark_results.json"
RESULTS_DIR = "benchmark_results"
REPORTS_DIR = "benchmark_reports"

async def monitor_progress(scale, benchmark, expected_msgs):
    """Monitor benchmark progress"""
    start_time = time.time()
    last_count = 0
    last_report_time = start_time
    
    while time.time() - start_time < BENCHMARK_DURATION and not benchmark.should_stop:
        await asyncio.sleep(10)
        current_time = time.time()
        elapsed = current_time - start_time
        
        current_count = benchmark.messages_received
        current_rate = (current_count - last_count) / 10.0
        
        logger.info(f"Scale {scale} Progress: "
                   f"GPUs: {scale * GPUS_PER_SERVER}, "
                   f"Rate: {current_rate:.1f} msgs/sec, "
                   f"Total Messages: {current_count}, "
                   f"Time: {elapsed:.0f}/{BENCHMARK_DURATION}s")
        
        last_count = current_count
        last_report_time = current_time

async def run_scale_benchmark():
    results = {}
    final_report_created = False
    
    # Create results directory
    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(REPORTS_DIR, exist_ok=True)
    
    start_time = datetime.now()
    logger.info(f"Starting complete benchmark suite at {start_time}")
    
    try:
        for scale in SERVER_SCALES:
            logger.info(f"\n{'='*50}")
            logger.info(f"Starting benchmark with {scale} servers ({scale * GPUS_PER_SERVER} GPUs)")
            logger.info(f"{'='*50}")
            
            update_num_servers(scale)
            producers = []
            server_tasks = []
            
            try:
                # Start producers and servers
                for server_id in range(scale):
                    producer = MetricsProducer()
                    await producer.start()
                    producers.append(producer)
                    server_tasks.append(asyncio.create_task(
                        run_server(server_id, producer),
                        name=f"server-{server_id}"
                    ))
                
                # Start benchmark and monitoring
                benchmark = KafkaBenchmark(duration=BENCHMARK_DURATION)
                expected_msgs = scale * GPUS_PER_SERVER * (BENCHMARK_DURATION / METRICS_INTERVAL)
                
                # Run benchmark for fixed duration
                monitor_task = asyncio.create_task(monitor_progress(scale, benchmark, expected_msgs))
                benchmark_task = asyncio.create_task(benchmark.run_benchmark())
                
                await asyncio.sleep(BENCHMARK_DURATION)
                
                # Stop benchmark and get stats
                benchmark.should_stop = True
                monitor_task.cancel()
                await asyncio.wait_for(benchmark_task, timeout=10)
                
                stats = benchmark.calculate_stats()
                results[str(scale)] = stats
                save_intermediate_results(results)
                
                logger.info(f"Scale {scale} completed with {benchmark.messages_received} messages processed")
                
            finally:
                # Clean up tasks and resources
                logger.info("Cleaning up resources...")
                
                # Cancel server tasks
                for task in server_tasks:
                    if not task.done():
                        task.cancel()
                
                # Wait for tasks to finish with timeout
                if server_tasks:
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*server_tasks, return_exceptions=True),
                            timeout=5
                        )
                    except asyncio.TimeoutError:
                        logger.warning("Timeout waiting for server tasks to cancel")
                
                # Stop benchmark if running
                if benchmark:
                    await benchmark.shutdown()
                
                # Close producers
                for producer in producers:
                    try:
                        await asyncio.wait_for(producer.close(), timeout=5)
                    except asyncio.TimeoutError:
                        logger.warning("Timeout closing producer")
                    except Exception as e:
                        logger.error(f"Error closing producer: {e}")
                
                logger.info("Cooling down for 30 seconds...")
                await asyncio.sleep(30)
    
        # Generate final report
        end_time = datetime.now()
        duration = end_time - start_time
        report_path = generate_final_report(results, start_time, end_time, duration)
        logger.info(f"Complete benchmark report generated: {report_path}")
            
    except Exception as e:
        logger.error(f"Benchmark suite error: {e}")
    finally:
        if not final_report_created and results:
            # Generate report even if incomplete
            end_time = datetime.now()
            duration = end_time - start_time
            report_path = generate_final_report(results, start_time, end_time, duration)
            logger.info(f"Partial benchmark report generated: {report_path}")

def save_intermediate_results(results):
    """Save intermediate results without generating report"""
    json_filename = os.path.join(RESULTS_DIR, "benchmark_results_latest.json")
    with open(json_filename, 'w') as f:
        json.dump(results, f, indent=2)
    logger.info(f"Intermediate results saved to {json_filename}")

def generate_final_report(results, start_time, end_time, duration):
    """Generate final comprehensive report"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_name = f"complete_benchmark_report_{timestamp}"
    
    # Save final results
    json_filename = os.path.join(RESULTS_DIR, f"{report_name}.json")
    with open(json_filename, 'w') as f:
        json.dump({
            'metadata': {
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration': str(duration),
                'scales_tested': list(results.keys())
            },
            'results': results
        }, f, indent=2)
    
    # Generate HTML report
    report_path = generate_report(results, REPORTS_DIR)
    return report_path

def main():
    try:
        asyncio.run(run_scale_benchmark())
    except KeyboardInterrupt:
        logger.info("Benchmark suite interrupted by user.")

if __name__ == "__main__":
    main()
