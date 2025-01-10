import socket
import logging
from typing import List

def verify_kafka_brokers(host: str, ports: List[int], timeout: int = 5) -> List[str]:
    """Verify Kafka broker connectivity and return list of available brokers"""
    available_brokers = []
    for port in ports:
        try:
            sock = socket.create_connection((host, port), timeout=timeout)
            sock.close()
            available_brokers.append(f"{host}:{port}")
            logging.info(f"Successfully connected to broker at {host}:{port}")
        except (socket.timeout, socket.error) as e:
            logging.warning(f"Could not connect to broker at {host}:{port}: {e}")
    return available_brokers

# Kafka broker configuration
KAFKA_HOST = '10.180.8.24'
KAFKA_PORTS = [9092, 9093, 9094, 9095, 9096]

# Verify and get available brokers
KAFKA_BOOTSTRAP_SERVERS = verify_kafka_brokers(KAFKA_HOST, KAFKA_PORTS)
if not KAFKA_BOOTSTRAP_SERVERS:
    raise RuntimeError("No Kafka brokers available")

# Topic configuration
KAFKA_TOPIC = 'dcgm-metrics-test-optimized'
NUM_SERVERS = 32
GPUS_PER_SERVER = 4
METRICS_INTERVAL = 1

# Consumer configuration
NUM_CONSUMERS = 4
CONSUMER_GROUP = 'dcgm-metrics-group'
STATS_INTERVAL = 5

# Producer configuration
PRODUCER_COMPRESSION = 'zstd'
MAX_REQUEST_SIZE = 1048576  # 1MB

# Kafka client configurations - only essential settings
KAFKA_CONNECTION_CONFIG = {
    'request_timeout_ms': 30000,
    'metadata_max_age_ms': 30000,
    'api_version': 'auto'
}
