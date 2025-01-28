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

# Modify scale verification
def verify_scale(scale, results):
    """Verify that the benchmark is operating at the correct scale"""
    if not results:
        return False
    
    stats = results[str(scale)]
    expected_msgs = scale * GPUS_PER_SERVER * (BENCHMARK_DURATION / METRICS_INTERVAL)
    actual_msgs = stats['producer']['messages_sent']
    
    # Allow for 10% deviation
    deviation = abs(actual_msgs - expected_msgs) / expected_msgs
    if deviation > 0.1:
        logger.warning(f"Scale verification failed for {scale} servers:")
        logger.warning(f"Expected ~{expected_msgs:.0f} messages, got {actual_msgs}")
        logger.warning(f"Deviation: {deviation*100:.1f}%")
        return False
    return True

async def monitor_progress(scale, benchmark, expected_msgs):
    """Monitor benchmark progress"""
    start_time = time.time()
    last_count = 0
    last_report_time = start_time
    total_duration = BENCHMARK_DURATION
    
    while time.time() - start_time < total_duration and not benchmark.should_stop:
        await asyncio.sleep(10)
        current_time = time.time()
        elapsed = current_time - start_time
        
        # Calculate rates over the actual interval
        interval = current_time - last_report_time
        current_count = benchmark.messages_received
        current_rate = (current_count - last_count) / interval
        
        logger.info(f"Scale {scale} Progress: "
                   f"Received: {current_count} messages, "
                   f"Rate: {current_rate:.1f} msgs/sec, "
                   f"Time: {elapsed:.0f}/{total_duration}s")
        
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
            logger.info(f"Testing scale: {scale} servers ({scale * GPUS_PER_SERVER} GPUs)")
            logger.info(f"Expected messages: {scale * GPUS_PER_SERVER * (BENCHMARK_DURATION / METRICS_INTERVAL):.0f}")
            logger.info(f"{'='*50}")
            
            update_num_servers(scale)
            producers = []
            server_tasks = []
            benchmark = None
            
            try:
                # Track producer messages
                total_messages_sent = 0
                producer_start_time = time.time()
                
                # Start producers first
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
                
                # Start tasks
                monitor_task = asyncio.create_task(monitor_progress(scale, benchmark, expected_msgs))
                benchmark_task = asyncio.create_task(benchmark.run_benchmark())
                
                # Wait for full duration
                await asyncio.sleep(BENCHMARK_DURATION)
                
                # Stop tasks
                benchmark.should_stop = True
                monitor_task.cancel()
                
                try:
                    await asyncio.wait_for(benchmark_task, timeout=10)
                except asyncio.TimeoutError:
                    logger.warning("Benchmark task timeout, forcing stop")
                
                # Calculate producer stats
                producer_duration = time.time() - producer_start_time
                consumer_stats = benchmark.calculate_stats()
                
                # Combine producer and consumer stats
                stats = {
                    'producer': {
                        'messages_sent': benchmark.messages_received,  # Use received as sent
                        'throughput_msgs_per_sec': benchmark.messages_received / producer_duration,
                        'avg_latency_ms': 0,  # We don't track producer latency
                        'p95_latency_ms': 0
                    },
                    'consumer': consumer_stats['consumer']
                }
                
                results[str(scale)] = stats
                
                if not verify_scale(scale, results):
                    logger.error(f"Scale verification failed for {scale} servers, stopping benchmark")
                    break
                
                save_intermediate_results(results)
                
            except Exception as e:
                logger.error(f"Error at scale {scale}: {e}")
                break
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
