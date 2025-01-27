import asyncio
import logging
import json
import os
from datetime import datetime
from Kafka_bench import KafkaBenchmark
from config import update_num_servers
from report_generator import generate_report

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
            benchmark = KafkaBenchmark(duration=BENCHMARK_DURATION)
            
            try:
                await benchmark.run_benchmark()
                stats = benchmark.calculate_stats()
                results[str(scale)] = stats
                
                # Verify scale before proceeding
                if not verify_scale(scale, results):
                    logger.error(f"Scale verification failed for {scale} servers, stopping benchmark")
                    break
                
                # Save intermediate results but don't generate report
                save_intermediate_results(results)
                
                logger.info(f"Cooling down for 30 seconds...")
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"Error at scale {scale}: {e}")
                break
                
        # Generate final report only once after all scales are complete
        if results:
            final_report_created = True
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
