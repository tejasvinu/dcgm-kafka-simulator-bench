# dcgm-hpc-simulator Simulator

## Overview

ExaODAF Simulator is a comprehensive toolkit designed to simulate GPU metrics, handle Kafka-based data production and consumption, and analyze system throughput. It leverages NVIDIA's DCGM for GPU management and Kafka for real-time data streaming, enabling efficient monitoring and analysis of GPU performance in a clustered environment.

## Components

- **simulator.sh**: Shell script to initialize and manage simulation servers.
- **kafka_prod.py**: Asynchronous Kafka producer that generates and sends DCGM metrics.
- **kafka_consume.py**: Asynchronous Kafka consumer that processes incoming DCGM metrics.
- **dcgm_sim_test.py**: Simulated DCGM metrics server to emulate GPU data for testing purposes.
- **analyze_throughput.py**: Data analysis tool for evaluating throughput based on Kafka consumer logs.

## Installation

1. **Clone the Repository**
    ```bash
    git clone https://github.com/yourusername/ExaODAF.git
    cd ExaODAF
    ```

2. **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

3. **Configure Kafka**
    Ensure Kafka is installed and running. Update the bootstrap servers in `kafka_prod.py` and `kafka_consume.py` as per your setup.

## Usage

1. **Start the DCGM Metrics Server**
    ```bash
    python dcgm_sim_test.py --num_ports <number_of_ports> --start_port <starting_port> --total_nodes <total_nodes>
    ```

2. **Run the Kafka Producer**
    ```bash
    python kafka_prod.py
    ```

3. **Run the Kafka Consumer**
    ```bash
    python kafka_consume.py
    ```

4. **Analyze Throughput**
    ```bash
    python analyze_throughput.py
    ```

## Configuration

- **Kafka Producer (`kafka_prod.py`)**
    - Update `KAFKA_BOOTSTRAP_SERVERS` with your Kafka servers.
    - Adjust `FETCH_INTERVAL`, `RETRIES`, and other parameters as needed.

- **Kafka Consumer (`kafka_consume.py`)**
    - Update `bootstrap_servers` with your Kafka servers.
    - Modify `group_id` and other consumer configurations as necessary.

- **DCGM Metrics Server (`dcgm_sim_test.py`)**
    - Set `METRICS_CACHE_TTL`, `WORKER_CONNECTIONS`, and other server parameters.
    - Configure the number of GPUs per node and other simulation settings.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the [MIT License](LICENSE).
