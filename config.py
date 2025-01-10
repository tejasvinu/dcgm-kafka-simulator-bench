import socket

def verify_kafka_connection(host, port, timeout=5):
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        return True
    except (socket.timeout, socket.error):
        return False

# Basic configuration with connection verification
KAFKA_HOST = '10.180.8.24'
KAFKA_PORT = 9092

if verify_kafka_connection(KAFKA_HOST, KAFKA_PORT):
    KAFKA_BOOTSTRAP_SERVERS = [f'{KAFKA_HOST}:{KAFKA_PORT}']
else:
    raise RuntimeError(f"Cannot connect to Kafka broker at {KAFKA_HOST}:{KAFKA_PORT}")

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

# Kafka client configurations
CLIENT_CONFIG = {
    'metadata_max_age_ms': 5000,
    'retry_backoff_ms': 1000,
    'request_timeout_ms': 30000,
    'session_timeout_ms': 10000,
    'heartbeat_interval_ms': 3000
}
