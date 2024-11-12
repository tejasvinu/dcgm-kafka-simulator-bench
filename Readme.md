# dcgm-hpc-simulator: Exascale GPU Benchmark Simulator

A comprehensive benchmarking suite for simulating and testing DCGM (Data Center GPU Manager) metrics collection at scale, designed for exascale computing environments.

## Overview

`dcgm-hpc-simulator` is a simulation framework that tests the scalability and performance of DCGM metrics collection systems. It simulates multiple GPU nodes (each with 4 GPUs) and generates realistic DCGM metrics, measuring throughput and performance characteristics.

## Features

* Simulates multiple GPU nodes (scalable from 8 to 8192 nodes).
* Each node simulates 4 GPUs, generating realistic DCGM metrics.
* Kafka-based metrics collection and processing.
* Comprehensive benchmarking with configurable warmup and test durations.
* Detailed performance analysis and visualization.
* Resource monitoring and logging.
* Configurable test scenarios.

## Components

1. **Metrics Server (`dcgm_sim_test.py`):** Simulates multiple GPU nodes, providing DCGM-format metrics via HTTP endpoints.  Uses a scalable architecture with one port per node.

2. **Kafka Producer (`kafka_prod.py`):** Collects metrics from simulated nodes, efficiently batching and rate-limiting data. Handles multiple nodes concurrently.

3. **Kafka Consumer (`kafka_consume.py`):** Processes incoming metrics, tracking throughput and performance metrics. Implements reliable message handling.

4. **Benchmark Controller (`simulator.sh`):** Orchestrates the benchmark process, managing configurations, execution, cleanup, and resource management.

5. **Analysis Tool (`analyze_throughput.py`):** Processes benchmark results, generating statistical analysis (including confidence intervals and regression analysis) and visualizations.


## Prerequisites

* Python 3.8+
* A running Kafka cluster (with configured bootstrap servers).
* Python packages:  Install using `pip install aiokafka aiohttp numpy pandas matplotlib seaborn scipy psutil lz4`

## Configuration

1. **Kafka Settings:** Update `KAFKA_BOOTSTRAP_SERVERS` in `kafka_prod.py` and configure the topic name in `DCGM_KAFKA_TOPIC`.

2. **Test Parameters:** Modify test configurations within `simulator.sh` (node counts, process distribution, warmup/test durations, etc.).


## Usage

1. **Start the Benchmark:** `./simulator.sh`

2. **Test Configurations:**  The script supports various node configurations (8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192 nodes), each with a corresponding process distribution optimized for performance.

3. **Results and Analysis:**
    * Logs: `benchmark_logs/<timestamp>`
    * Analysis Results: `benchmark_logs/<timestamp>/analysis_results.log`
    * Visualizations: `benchmark_logs/<timestamp>/throughput_analysis.png`


## Performance Considerations

* Each node simulates 4 GPUs with realistic DCGM metrics.
* Default warmup period: 5 minutes.
* Default test duration: 30 minutes.
* Resource monitoring interval: 30 seconds (configurable).
* Configurable rate limiting and batch sizes.


## Monitoring and Logging

* Real-time resource monitoring (CPU, memory, disk usage).
* Detailed logs for each component.
* Performance metrics and error tracking.
* Automated log rotation and archiving.


## Error Handling

* Graceful shutdown mechanisms.
* Automatic resource cleanup.
* Error logging and reporting.
* Process monitoring and recovery.


## Analysis Features

* Statistical analysis of throughput.
* Confidence intervals calculation.
* Regression analysis.
* Performance visualization.
* Steady-state analysis.


## Contributing

1. Fork the repository.
2. Create a feature branch.
3. Commit your changes.
4. Push to the branch.
5. Create a pull request.


## License

MIT License (see `LICENSE` file)


## Contact

Use the issue tracker for questions or concerns.
