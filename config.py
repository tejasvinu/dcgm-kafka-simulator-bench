import socket
import logging

def verify_kafka_connection(host, port, timeout=5):
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        return True
    except (socket.timeout, socket.error):
        return False

# Basic configuration
KAFKA_HOST = '10.180.8.24'
KAFKA_PORT = 9092

# Try simple connection first
if verify_kafka_connection(KAFKA_HOST, KAFKA_PORT):
    KAFKA_BOOTSTRAP_SERVERS = [f'{KAFKA_HOST}:{KAFKA_PORT}']
else:
    logging.error(f"Cannot connect to Kafka broker at {KAFKA_HOST}:{KAFKA_PORT}")
    raise RuntimeError(f"Cannot connect to Kafka broker at {KAFKA_HOST}:{KAFKA_PORT}")

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

# Kafka client configurations with more conservative timeouts
KAFKA_CONNECTION_CONFIG = {
    'metadata_max_age_ms': 30000,          # 30 seconds
    'retry_backoff_ms': 500,               # 500ms between retries
    'request_timeout_ms': 30000,           # 30 second timeout
    'session_timeout_ms': 30000,           # 30 second session timeout
    'heartbeat_interval_ms': 10000,        # 10 second heartbeat
    'max_poll_interval_ms': 300000,        # 5 minutes
    'connections_max_idle_ms': 540000,     # 9 minutes
    'reconnect_backoff_ms': 100,           # 100ms initial backoff
    'reconnect_backoff_max_ms': 10000,     # 10 seconds max backoff
    'security_protocol': 'PLAINTEXT'
}
