import asyncio
import subprocess
import time
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import os
import logging
import sys
import signal
import threading
import queue
import statistics
import psutil

logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')

class ProcessOutputReader(threading.Thread):
    def __init__(self, process, output_queue, is_stderr=False):
        threading.Thread.__init__(self)
        self.process = process
        self.output_queue = output_queue
        self.is_stderr = is_stderr
        self.daemon = True

    def run(self):
        stream = self.process.stderr if self.is_stderr else self.process.stdout
        if stream:
            try:
                for line in iter(stream.readline, ''):
                    self.output_queue.put(line.strip())
            except Exception as e:
                logging.error("Error reading process output: %s" % e)
            finally:
                stream.close()

class BenchmarkRunner:
    def __init__(self):
        self.results_dir = "benchmark_results"
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(f"{self.results_dir}/{self.timestamp}", exist_ok=True)
        # Reduced server counts for more thorough testing
        self.server_counts = [8, 16, 32, 128, 256, 512, 1024, 2048, 4096, 8192]
        self.results = []
        self.current_processes = []
        
        # Test phase durations
        self.warmup_duration = 300  # 5 min warmup
        self.test_duration = 1800   # 30 min test
        self.cooldown_duration = 120  # 2 min cooldown
        self.metric_interval = 10   # Sample every 10s

    def cleanup_processes(self):
        for process in self.current_processes:
            try:
                if process and process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
            except Exception as e:
                logging.error(f"Error cleaning up process: {e}")
        self.current_processes.clear()

    async def collect_metrics(self, consumer_output_queue):
        rate = 0
        try:
            while True:
                try:
                    line = consumer_output_queue.get_nowait()
                    if "Processing rate:" in line:
                        rate = float(line.split(":")[1].split()[0])
                except queue.Empty:
                    break
        except Exception as e:
            logging.error(f"Error collecting metrics: {e}")
        return rate

    async def run_benchmark(self, num_servers):
        consumer_proc = None
        server_proc = None
        try:
            # Update config
            with open("config.py", "w") as f:
                f.write(f"""KAFKA_BOOTSTRAP_SERVERS = ['10.180.8.24:9092', '10.180.8.24:9093', '10.180.8.24:9094']
KAFKA_TOPIC = 'dcgm-metrics-test'
NUM_SERVERS = {num_servers}
GPUS_PER_SERVER = 4
METRICS_INTERVAL = 1
""")

            # Start consumer with metrics collection
            consumer_output_queue = queue.Queue()
            consumer_error_queue = queue.Queue()
            
            logging.info("Starting consumer process...")
            consumer_proc = subprocess.Popen(
                [sys.executable, "consumer.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=1,
                universal_newlines=True,
                text=True
            )
            self.current_processes.append(consumer_proc)

            # Start output and error readers
            consumer_out_reader = ProcessOutputReader(consumer_proc, consumer_output_queue, is_stderr=False)
            consumer_err_reader = ProcessOutputReader(consumer_proc, consumer_error_queue, is_stderr=True)
            consumer_out_reader.start()
            consumer_err_reader.start()

            # Wait for consumer initialization
            logging.info("Waiting for consumer initialization...")
            initialization_timeout = 30  # Increased timeout
            start_wait = time.time()
            initialized = False

            while time.time() - start_wait < initialization_timeout:
                if consumer_proc.poll() is not None:
                    # Process died during initialization
                    error_msgs = []
                    while True:
                        try:
                            error_msgs.append(consumer_error_queue.get_nowait())
                        except queue.Empty:
                            break
                    error_text = "\n".join(error_msgs)
                    raise RuntimeError(f"Consumer process died during initialization:\n{error_text}")

                # Check for successful initialization
                try:
                    line = consumer_output_queue.get_nowait()
                    logging.debug(f"Consumer output: {line}")
                    if "Consumer initialized successfully" in line:
                        initialized = True
                        break
                except queue.Empty:
                    await asyncio.sleep(0.1)
                    continue

            if not initialized:
                raise RuntimeError("Consumer failed to initialize within timeout period")

            # Start server emulator
            logging.info("Starting server emulator process...")
            server_proc = subprocess.Popen(
                [sys.executable, "server_emulator.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=1,
                universal_newlines=True
            )
            self.current_processes.append(server_proc)

            # Monitor both processes
            server_output_queue = queue.Queue()
            server_error_queue = queue.Queue()
            server_out_reader = ProcessOutputReader(server_proc, server_output_queue, is_stderr=False)
            server_err_reader = ProcessOutputReader(server_proc, server_error_queue, is_stderr=True)
            server_out_reader.start()
            server_err_reader.start()

            # Warmup phase
            logging.info("Starting warmup phase...")
            start_time = time.time()
            while time.time() - start_time < self.warmup_duration:
                if not self.check_process_health(consumer_proc, server_proc, 
                                              consumer_error_queue, server_error_queue):
                    raise RuntimeError("Process died during warmup")
                await asyncio.sleep(self.metric_interval)

            # Main test phase
            logging.info("Starting main test phase...")
            detailed_metrics = []
            start_time = time.time()
            
            while time.time() - start_time < self.test_duration:
                if not self.check_process_health(consumer_proc, server_proc,
                                              consumer_error_queue, server_error_queue):
                    raise RuntimeError("Process died during test")
                
                rate = await self.collect_metrics(consumer_output_queue)
                detailed_metrics.append({
                    'timestamp': time.time(),
                    'rate': rate,
                    'cpu_usage': psutil.cpu_percent(interval=None),
                    'mem_usage': psutil.virtual_memory().percent,
                    'num_servers': num_servers
                })
                await asyncio.sleep(self.metric_interval)

            # Cooldown phase
            logging.info("Starting cooldown phase...")
            await asyncio.sleep(self.cooldown_duration)

            # Calculate statistics
            rates = [m['rate'] for m in detailed_metrics]
            self.results.append({
                'num_servers': num_servers,
                'avg_rate': sum(rates)/len(rates),
                'min_rate': min(rates),
                'max_rate': max(rates),
                'stddev': statistics.stdev(rates),
                'samples': len(rates),
                'avg_cpu': sum(m['cpu_usage'] for m in detailed_metrics)/len(detailed_metrics),
                'avg_mem': sum(m['mem_usage'] for m in detailed_metrics)/len(detailed_metrics)
            })

            # Save detailed metrics
            df_detailed = pd.DataFrame(detailed_metrics)
            df_detailed.to_csv(f"{self.results_dir}/{self.timestamp}/detailed_metrics_{num_servers}.csv", 
                             index=False)

        except Exception as e:
            logging.error(f"Error during benchmark with {num_servers} servers: {e}")
            # Collect any remaining error output
            if consumer_proc and consumer_error_queue:
                error_msgs = []
                while True:
                    try:
                        error_msgs.append(consumer_error_queue.get_nowait())
                    except queue.Empty:
                        break
                if error_msgs:
                    logging.error(f"Consumer errors:\n{' '.join(error_msgs)}")
            
            if server_proc and server_error_queue:
                error_msgs = []
                while True:
                    try:
                        error_msgs.append(server_error_queue.get_nowait())
                    except queue.Empty:
                        break
                if error_msgs:
                    logging.error(f"Server emulator errors:\n{' '.join(error_msgs)}")

            self.results.append({
                'num_servers': num_servers,
                'avg_processing_rate': 0,
                'total_gpus': num_servers * 4,
                'error': str(e)
            })

        finally:
            self.cleanup_processes()

    def check_process_health(self, consumer_proc, server_proc, consumer_error_queue, server_error_queue):
        """Check if processes are still running and collect error messages if not"""
        if consumer_proc.poll() is not None or server_proc.poll() is not None:
            self.collect_error_messages(consumer_error_queue, "Consumer")
            self.collect_error_messages(server_error_queue, "Server")
            return False
        return True

    def collect_error_messages(self, error_queue, process_name):
        """Collect and log error messages from process queue"""
        error_msgs = []
        while True:
            try:
                error_msgs.append(error_queue.get_nowait())
            except queue.Empty:
                break
        if error_msgs:
            logging.error(f"{process_name} errors:\n{' '.join(error_msgs)}")

    def generate_report(self):
        # Create DataFrame
        df = pd.DataFrame(self.results)
        
        # Save raw data
        df.to_csv(f"{self.results_dir}/{self.timestamp}/benchmark_results.csv", index=False)

        # Generate plots
        plt.figure(figsize=(12, 6))
        plt.plot(df['num_servers'], df['avg_rate'], marker='o')  # Changed from avg_processing_rate to avg_rate
        plt.xlabel('Number of Servers')
        plt.ylabel('Messages/Second')
        plt.title('Kafka Processing Rate vs Number of Servers')
        plt.grid(True)
        plt.savefig(f"{self.results_dir}/{self.timestamp}/processing_rate.png")
        plt.close()

        # Generate additional plots
        plt.figure(figsize=(12, 6))
        plt.plot(df['num_servers'], df['avg_cpu'], marker='o', label='CPU Usage (%)')
        plt.plot(df['num_servers'], df['avg_mem'], marker='s', label='Memory Usage (%)')
        plt.xlabel('Number of Servers')
        plt.ylabel('Resource Usage (%)')
        plt.title('Resource Usage vs Number of Servers')
        plt.legend()
        plt.grid(True)
        plt.savefig(f"{self.results_dir}/{self.timestamp}/resource_usage.png")
        plt.close()

        # Generate HTML report with more detailed information
        html_report = f"""
        <html>
        <head>
            <title>Kafka Benchmark Report - {self.timestamp}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .graph {{ margin: 20px 0; }}
                .metrics {{ margin-bottom: 30px; }}
            </style>
        </head>
        <body>
            <h1>Kafka Benchmark Report</h1>
            <p>Generated: {self.timestamp}</p>
            
            <div class="metrics">
                <h2>Results Summary</h2>
                <table>
                    <tr>
                        <th>Servers</th>
                        <th>Avg Rate (msg/s)</th>
                        <th>Min Rate</th>
                        <th>Max Rate</th>
                        <th>StdDev</th>
                        <th>Avg CPU (%)</th>
                        <th>Avg Mem (%)</th>
                    </tr>
                    {''.join(f"<tr><td>{row['num_servers']}</td><td>{row['avg_rate']:.2f}</td><td>{row['min_rate']:.2f}</td><td>{row['max_rate']:.2f}</td><td>{row['stddev']:.2f}</td><td>{row['avg_cpu']:.1f}</td><td>{row['avg_mem']:.1f}</td></tr>" for _, row in df.iterrows())}
                </table>
            </div>
            
            <div class="graph">
                <h2>Processing Rate Graph</h2>
                <img src="processing_rate.png" alt="Processing Rate Graph" style="width: 100%; max-width: 800px;">
            </div>
            
            <div class="graph">
                <h2>Resource Usage Graph</h2>
                <img src="resource_usage.png" alt="Resource Usage Graph" style="width: 100%; max-width: 800px;">
            </div>
        </body>
        </html>
        """
        
        with open(f"{self.results_dir}/{self.timestamp}/report.html", "w") as f:
            f.write(html_report)

async def main():
    runner = BenchmarkRunner()
    try:
        for num_servers in runner.server_counts:
            logging.info(f"Running benchmark with {num_servers} servers...")
            await runner.run_benchmark(num_servers)
        runner.generate_report()
        logging.info(f"Benchmark complete. Results available in: {runner.results_dir}/{runner.timestamp}/")
    except KeyboardInterrupt:
        logging.info("Benchmark interrupted by user")
        runner.cleanup_processes()
    except Exception as e:
        logging.error(f"Benchmark failed: {e}")
        runner.cleanup_processes()

if __name__ == "__main__":
    # Handle Ctrl+C gracefully
    signal.signal(signal.SIGINT, lambda sig, frame: sys.exit(0))
    asyncio.run(main())