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

logging.info(f"Using Kafka brokers: {KAFKA_BOOTSTRAP_SERVERS}")

# Topic configuration
KAFKA_TOPIC = 'dcgm-metrics-test'
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

# Kafka client configurations with optimized settings for multiple brokers
KAFKA_CONNECTION_CONFIG = {
    'metadata_max_age_ms': 10000,          # 10 seconds - more frequent metadata refresh
    'retry_backoff_ms': 100,               # 100ms between retries
    'request_timeout_ms': 30000,           # 30 second timeout
    'session_timeout_ms': 45000,           # 45 second session timeout
    'heartbeat_interval_ms': 15000,        # 15 second heartbeat
    'max_poll_interval_ms': 600000,        # 10 minutes
    'connections_max_idle_ms': 540000,     # 9 minutes
    'reconnect_backoff_ms': 50,            # 50ms initial backoff
    'reconnect_backoff_max_ms': 5000,      # 5 seconds max backoff
    'security_protocol': 'PLAINTEXT',
    'max_poll_records': 500,               # Max records per poll
    'fetch_max_wait_ms': 500,              # Max time to wait for data
    'fetch_max_bytes': 52428800,           # 50MB max fetch size
    'receive_buffer_bytes': 32768,         # 32KB receive buffer
    'send_buffer_bytes': 131072,           # 128KB send buffer
    'enable_auto_commit': True,
    'auto_commit_interval_ms': 5000,       # 5 second auto commit
    'check_crcs': False,                   # Disable CRC checks for performance
    'socket_connection_setup_timeout_ms': 10000,  # 10 second connection timeout
    'socket_connection_setup_timeout_max_ms': 30000  # 30 second max connection timeout
}

# Additional settings for stability
MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 5
MAX_REQUEST_RETRIES = 5
METADATA_MAX_RETRIES = 3
