import asyncio
import subprocess
import time
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import os
import logging
import sys
import signal
import threading
import queue
import statistics
import psutil
import numpy as np
import json
from typing import Dict, List

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
        self.warmup_duration = 60    # 1 min warmup
        self.test_duration = 600     # 10 min test
        self.cooldown_duration = 60  # 1 min cooldown
        self.metric_interval = 10    # Sample every 10s

        # Additional metrics
        self.collect_interval = 1  # Collect metrics every second
        self.detailed_metrics = {
            'latency': [],
            'throughput': [],
            'cpu_per_core': [],
            'memory_details': [],
            'network_io': [],
            'disk_io': [],
            'gc_stats': [],
            'partition_distribution': []
        }
        
        # Expanded server counts for more granular data
        self.server_counts = [
            8, 16, 32, 64, 128, 256, 384, 512, 768, 
            1024, 1536, 2048, 3072, 4096, 6144, 8192
        ]

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
        rates = []
        try:
            while True:
                try:
                    line = consumer_output_queue.get_nowait()
                    if "Processing rate:" in line:
                        rate = float(line.split(":")[1].split()[0])
                        rates.append(rate)
                        logging.info(f"Current processing rate: {rate:.2f} messages/second")
                except queue.Empty:
                    break
        except Exception as e:
            logging.error(f"Error collecting metrics: {e}")
        
        # Return average rate if rates were collected, otherwise 0
        return statistics.mean(rates) if rates else 0

    async def collect_system_metrics(self):
        """Collect detailed system metrics"""
        cpu_per_core = psutil.cpu_percent(percpu=True)
        memory = psutil.virtual_memory()
        disk = psutil.disk_io_counters()
        network = psutil.net_io_counters()
        
        return {
            'timestamp': time.time(),
            'cpu_per_core': cpu_per_core,
            'memory': {
                'total': memory.total,
                'available': memory.available,
                'used': memory.used,
                'free': memory.free,
                'cached': memory.cached,
                'buffers': getattr(memory, 'buffers', 0),
                'percent': memory.percent
            },
            'disk': {
                'read_bytes': disk.read_bytes,
                'write_bytes': disk.write_bytes,
                'read_count': disk.read_count,
                'write_count': disk.write_count
            },
            'network': {
                'bytes_sent': network.bytes_sent,
                'bytes_recv': network.bytes_recv,
                'packets_sent': network.packets_sent,
                'packets_recv': network.packets_recv
            }
        }

    def calculate_statistics(self, data: List[float]) -> Dict:
        """Calculate detailed statistics for a metric"""
        if not data:
            return {}
        
        return {
            'mean': np.mean(data),
            'median': np.median(data),
            'std': np.std(data),
            'min': np.min(data),
            'max': np.max(data),
            'p95': np.percentile(data, 95),
            'p99': np.percentile(data, 99),
            'p99_9': np.percentile(data, 99.9)
        }

    async def run_benchmark(self, num_servers):
        consumer_proc = None
        server_proc = None
        try:
            # Update config
            with open("config.py", "w") as f:
                f.write(f"""KAFKA_BOOTSTRAP_SERVERS = ['10.180.8.24:9092', '10.180.8.24:9093', '10.180.8.24:9094','10.180.8.24:9095','10.180.8.24:9096']
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
            initialization_timeout = 120  # Increased from 30 to 120 seconds
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
                    logging.info(f"Consumer output: {line}")  # Changed from debug to info
                    if "Consumer initialized successfully" in line:
                        initialized = True
                        break
                except queue.Empty:
                    await asyncio.sleep(1)  # Increased from 0.1 to 1 second
                    continue

            if not initialized:
                # Collect any error messages before raising the timeout error
                error_msgs = []
                while True:
                    try:
                        error_msgs.append(consumer_error_queue.get_nowait())
                    except queue.Empty:
                        break
                error_text = "\n".join(error_msgs) if error_msgs else "No error messages available"
                raise RuntimeError(f"Consumer failed to initialize within {initialization_timeout} seconds.\nLast known state:\n{error_text}")

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

            # Warmup phase - don't collect metrics
            logging.info("Starting warmup phase...")
            start_time = time.time()
            while time.time() - start_time < self.warmup_duration:
                if not self.check_process_health(consumer_proc, server_proc, 
                                              consumer_error_queue, server_error_queue):
                    raise RuntimeError("Process died during warmup")
                # Clear any metrics from warmup period
                while not consumer_output_queue.empty():
                    consumer_output_queue.get()
                await asyncio.sleep(self.metric_interval)

            # Main test phase - only collect metrics during this period
            logging.info("Starting main test phase...")
            detailed_metrics = []
            test_start_time = time.time()
            last_metric_time = test_start_time
            metrics_collected = 0
            expected_metrics = self.test_duration // self.collect_interval
            
            while time.time() - test_start_time < self.test_duration:
                if not self.check_process_health(consumer_proc, server_proc,
                                              consumer_error_queue, server_error_queue):
                    raise RuntimeError("Process died during test")
                
                # Collect detailed metrics
                current_time = time.time()
                system_metrics = await self.collect_system_metrics()
                rate = await self.collect_metrics(consumer_output_queue)
                
                if rate > 0:
                    metrics_collected += 1
                    elapsed_time = current_time - test_start_time
                    remaining_time = self.test_duration - elapsed_time
                    
                    detailed_metrics.append({
                        'timestamp': current_time,
                        'rate': rate,
                        'cpu_usage': psutil.cpu_percent(interval=None),
                        'mem_usage': psutil.virtual_memory().percent,
                        'num_servers': num_servers,
                        'system_metrics': system_metrics
                    })
                    
                    # Print progress information
                    logging.info(f"""
Benchmark Progress for {num_servers} servers:
    Elapsed Time: {elapsed_time:.1f}s / {self.test_duration}s
    Remaining Time: {remaining_time:.1f}s
    Current Rate: {rate:.2f} msg/sec
    CPU Usage: {detailed_metrics[-1]['cpu_usage']:.1f}%
    Memory Usage: {detailed_metrics[-1]['mem_usage']:.1f}%
    Metrics Collected: {metrics_collected} / ~{expected_metrics}
""")
                    
                await asyncio.sleep(self.collect_interval)

            # Verify metrics collection
            collection_ratio = metrics_collected / expected_metrics
            if collection_ratio < 0.9:  # Less than 90% of expected metrics collected
                logging.warning(f"""
Metrics collection may be incomplete:
    Expected metrics: ~{expected_metrics}
    Collected metrics: {metrics_collected}
    Collection ratio: {collection_ratio:.1%}
""")
            else:
                logging.info(f"""
Metrics collection completed successfully:
    Expected metrics: ~{expected_metrics}
    Collected metrics: {metrics_collected}
    Collection ratio: {collection_ratio:.1%}
""")

            # Calculate statistics only from main test phase
            if detailed_metrics:
                # Verify data quality
                rates = [m['rate'] for m in detailed_metrics]
                zero_rates = sum(1 for r in rates if r == 0)
                if zero_rates > 0:
                    logging.warning(f"{zero_rates} samples had zero rate out of {len(rates)} total samples")
                
                # Calculate and log statistics
                stats = {
                    'avg_rate': statistics.mean(rates),
                    'min_rate': min(rates),
                    'max_rate': max(rates),
                    'stddev': statistics.stdev(rates),
                    'p95': np.percentile(rates, 95),
                    'p99': np.percentile(rates, 99)
                }
                
                logging.info(f"""
Benchmark Statistics for {num_servers} servers:
    Average Rate: {stats['avg_rate']:.2f} msg/sec
    Min Rate: {stats['min_rate']:.2f} msg/sec
    Max Rate: {stats['max_rate']:.2f} msg/sec
    Standard Deviation: {stats['stddev']:.2f}
    95th Percentile: {stats['p95']:.2f} msg/sec
    99th Percentile: {stats['p99']:.2f} msg/sec
""")

                self.results.append({
                    'num_servers': num_servers,
                    'avg_rate': sum(rates)/len(rates),
                    'min_rate': min(rates),
                    'max_rate': max(rates),
                    'stddev': statistics.stdev(rates),
                    'samples': len(rates),
                    'avg_cpu': sum(m['cpu_usage'] for m in detailed_metrics)/len(detailed_metrics),
                    'avg_mem': sum(m['mem_usage'] for m in detailed_metrics)/len(detailed_metrics),
                    'test_duration': self.test_duration,
                    'actual_duration': detailed_metrics[-1]['timestamp'] - detailed_metrics[0]['timestamp']
                })

                # Save detailed metrics with clear phase markers
                df_detailed = pd.DataFrame(detailed_metrics)
                df_detailed['phase'] = 'main_test'  # All these metrics are from main test phase
                df_detailed.to_csv(f"{self.results_dir}/{self.timestamp}/detailed_metrics_{num_servers}.csv", 
                                 index=False)
            else:
                raise RuntimeError("No metrics collected during test phase")

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
        """Generate enhanced benchmark report"""
        df = pd.DataFrame(self.results)
        
        # Save raw data as JSON for more detail
        with open(f"{self.results_dir}/{self.timestamp}/raw_results.json", 'w') as f:
            json.dump(self.results, f, indent=2)

        # Generate standard plots
        self._generate_standard_plots(df)
        
        # Generate additional plots
        self._generate_performance_plots(df)
        self._generate_resource_plots(df)
        self._generate_latency_plots(df)
        
        # Generate enhanced HTML report
        self._generate_html_report(df)

    def _generate_standard_plots(self, df):
        """Generate standard performance plots"""
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

    def _generate_performance_plots(self, df):
        """Generate detailed performance plots"""
        # Throughput over time
        plt.figure(figsize=(12, 6))
        for servers in self.server_counts:
            server_data = df[df['num_servers'] == servers]
            plt.plot(server_data['timestamp'], server_data['rate'], 
                    label=f'{servers} servers')
        plt.xlabel('Time (s)')
        plt.ylabel('Messages/Second')
        plt.title('Throughput Over Time by Server Count')
        plt.legend()
        plt.grid(True)
        plt.savefig(f"{self.results_dir}/{self.timestamp}/throughput_over_time.png")
        plt.close()

    def _generate_resource_plots(self, df):
        """Generate resource usage plots"""
        # CPU usage per core
        plt.figure(figsize=(12, 6))
        df_cpu = pd.DataFrame(df['system_metrics'].apply(lambda x: x['cpu_per_core']).tolist())
        df_cpu.plot(kind='box')
        plt.xlabel('CPU Core')
        plt.ylabel('Usage (%)')
        plt.title('CPU Usage Distribution per Core')
        plt.savefig(f"{self.results_dir}/{self.timestamp}/cpu_per_core.png")
        plt.close()

        # Memory usage breakdown
        plt.figure(figsize=(12, 6))
        memory_data = df['system_metrics'].apply(lambda x: x['memory'])
        plt.stackplot(df['timestamp'],
                     memory_data.apply(lambda x: x['used']),
                     memory_data.apply(lambda x: x['cached']),
                     memory_data.apply(lambda x: x['buffers']),
                     labels=['Used', 'Cached', 'Buffers'])
        plt.xlabel('Time (s)')
        plt.ylabel('Memory (bytes)')
        plt.title('Memory Usage Breakdown Over Time')
        plt.legend()
        plt.savefig(f"{self.results_dir}/{self.timestamp}/memory_breakdown.png")
        plt.close()

    def _generate_latency_plots(self, df):
        """Generate latency-related plots"""
        # Latency percentiles
        latency_stats = [self.calculate_statistics(group['rate']) 
                        for name, group in df.groupby('num_servers')]
        
        plt.figure(figsize=(12, 6))
        x = self.server_counts
        plt.plot(x, [stats['p95'] for stats in latency_stats], label='95th percentile')
        plt.plot(x, [stats['p99'] for stats in latency_stats], label='99th percentile')
        plt.plot(x, [stats['p99_9'] for stats in latency_stats], label='99.9th percentile')
        plt.xlabel('Number of Servers')
        plt.ylabel('Latency (ms)')
        plt.title('Latency Percentiles vs Server Count')
        plt.legend()
        plt.grid(True)
        plt.savefig(f"{self.results_dir}/{self.timestamp}/latency_percentiles.png")
        plt.close()

    def _generate_html_report(self, df):
        """Generate enhanced HTML report"""
        template = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Enhanced Kafka Benchmark Report - {timestamp}</title>
            <style>
                body { 
                    font-family: Arial, sans-serif; 
                    margin: 40px; 
                    background-color: #fafafa;
                    color: #333;
                }
                table { 
                    border-collapse: collapse; 
                    width: 100%; 
                    margin-bottom: 20px;
                    background-color: white;
                }
                th, td { 
                    border: 1px solid #ddd; 
                    padding: 12px 8px; 
                    text-align: left; 
                }
                th { 
                    background-color: #f2f2f2; 
                    font-weight: bold;
                }
                tr:nth-child(even) {
                    background-color: #f9f9f9;
                }
                .metric-card {
                    border: 1px solid #ddd;
                    border-radius: 8px;
                    padding: 20px;
                    margin: 20px 0;
                    background-color: white;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }
                .chart-container {
                    margin: 20px 0;
                    text-align: center;
                    background-color: white;
                    padding: 15px;
                    border-radius: 4px;
                }
                .chart-container img {
                    max-width: 100%;
                    height: auto;
                    margin: 10px 0;
                }
                .stats-table {
                    width: 100%;
                    margin: 20px 0;
                }
                h1 {
                    color: #2c3e50;
                    margin-bottom: 30px;
                }
                h2 {
                    color: #34495e;
                    margin: 10px 0;
                }
                ul {
                    list-style-type: none;
                    padding: 0;
                }
                li {
                    padding: 8px 0;
                    border-bottom: 1px solid #eee;
                }
                li:last-child {
                    border-bottom: none;
                }
                .timestamp {
                    color: #666;
                    font-style: italic;
                }
            </style>
        </head>
        <body>
            <h1>Enhanced Kafka Benchmark Report</h1>
            <p class="timestamp">Generated: {timestamp}</p>
            
            <div class="metric-card">
                <h2>Test Configuration</h2>
                <ul>
                    <li>Test Duration: {test_duration} seconds</li>
                    <li>Warmup Duration: {warmup_duration} seconds</li>
                    <li>Cooldown Duration: {cooldown_duration} seconds</li>
                    <li>Number of Server Configurations: {num_configs}</li>
                    <li>Total Messages Processed: {total_messages}</li>
                </ul>
            </div>

            <div class="metric-card">
                <h2>Performance Summary</h2>
                <div class="chart-container">
                    <img src="processing_rate.png" alt="Processing Rate">
                    <img src="throughput_over_time.png" alt="Throughput Over Time">
                </div>
            </div>

            <div class="metric-card">
                <h2>Resource Usage</h2>
                <div class="chart-container">
                    <img src="cpu_per_core.png" alt="CPU Usage per Core">
                    <img src="memory_breakdown.png" alt="Memory Usage Breakdown">
                </div>
            </div>

            <div class="metric-card">
                <h2>Latency Analysis</h2>
                <div class="chart-container">
                    <img src="latency_percentiles.png" alt="Latency Percentiles">
                </div>
            </div>

            <div class="metric-card">
                <h2>Detailed Statistics</h2>
                {detailed_stats_table}
            </div>
        </body>
        </html>
        """
        
        # Generate detailed statistics table
        detailed_stats = self._generate_detailed_stats_table(df)
        
        # Format the template
        report_content = template.format(
            timestamp=self.timestamp,
            test_duration=self.test_duration,
            warmup_duration=self.warmup_duration,
            cooldown_duration=self.cooldown_duration,
            num_configs=len(self.server_counts),
            total_messages=df['message_count'].sum(),
            detailed_stats_table=detailed_stats
        )
        
        with open(f"{self.results_dir}/{self.timestamp}/enhanced_report.html", "w") as f:
            f.write(report_content)

    def _generate_detailed_stats_table(self, df):
        """Generate detailed statistics table HTML"""
        stats_html = "<table class='stats-table'>"
        stats_html += "<tr><th>Metric</th><th>Value</th></tr>"
        
        # Add various statistics
        total_duration = df['timestamp'].max() - df['timestamp'].min()
        avg_rate = df['rate'].mean()
        peak_rate = df['rate'].max()
        
        stats = [
            ("Total Test Duration", f"{total_duration:.2f} seconds"),
            ("Average Processing Rate", f"{avg_rate:.2f} msg/sec"),
            ("Peak Processing Rate", f"{peak_rate:.2f} msg/sec"),
            ("Average CPU Usage", f"{df['cpu_usage'].mean():.2f}%"),
            ("Peak CPU Usage", f"{df['cpu_usage'].max():.2f}%"),
            ("Average Memory Usage", f"{df['mem_usage'].mean():.2f}%"),
            ("Peak Memory Usage", f"{df['mem_usage'].max():.2f}%"),
        ]

        for metric, value in stats:
            stats_html += f"<tr><td>{metric}</td><td>{value}</td></tr>"
        
        stats_html += "</table>"
        return stats_html

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
