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

async def run_scale_benchmark():
    results = {}
    
    # Create results directory
    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(REPORTS_DIR, exist_ok=True)
    
    for scale in SERVER_SCALES:
        logger.info(f"\n{'='*50}")
        logger.info(f"Starting benchmark with {scale} servers")
        logger.info(f"{'='*50}")
        
        # Update the configuration
        update_num_servers(scale)
        
        # Run benchmark for this scale
        benchmark = KafkaBenchmark(duration=BENCHMARK_DURATION)
        try:
            await benchmark.run_benchmark()
            stats = benchmark.calculate_stats()
            results[str(scale)] = stats
            
            # Save results after each scale
            save_results(results)
            
            # Cool down period between scales
            logger.info(f"Cooling down for 30 seconds before next scale...")
            await asyncio.sleep(30)
            
        except Exception as e:
            logger.error(f"Error during benchmark at scale {scale}: {e}")
            break

    # After all scales are complete or on break
    try:
        # Generate HTML report
        report_path = generate_report(results, REPORTS_DIR)
        logger.info(f"Generated HTML report: {report_path}")
    except Exception as e:
        logger.error(f"Error generating report: {e}")

def save_results(results):
    """Save results to JSON and generate HTML report"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save JSON results
    json_filename = os.path.join(RESULTS_DIR, f"benchmark_results_{timestamp}.json")
    with open(json_filename, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Generate HTML report
    try:
        report_path = generate_report(results, REPORTS_DIR)
        logger.info(f"Results saved to {json_filename}")
        logger.info(f"Report generated at {report_path}")
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        logger.info(f"Results saved to {json_filename}")

def main():
    try:
        asyncio.run(run_scale_benchmark())
    except KeyboardInterrupt:
        logger.info("Benchmark suite interrupted by user.")

if __name__ == "__main__":
    main()
