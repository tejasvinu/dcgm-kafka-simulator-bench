# Basic configuration
KAFKA_BOOTSTRAP_SERVERS = [
    '10.180.8.24:9092'  # Start with a single broker for initial testing
]

# Keep existing topic configuration
KAFKA_TOPIC = 'dcgm-metrics-test'
NUM_SERVERS = 32
GPUS_PER_SERVER = 4
METRICS_INTERVAL = 1

# Consumer configuration
NUM_CONSUMERS = 4
CONSUMER_GROUP = 'dcgm-metrics-group'
STATS_INTERVAL = 5

# Producer configuration with additional settings
PRODUCER_COMPRESSION = 'zstd'
MAX_REQUEST_SIZE = 1048576  # 1MB

# Add retry and timeout configurations
CONNECTION_TIMEOUT_MS = 10000
MAX_RETRIES = 3
RETRY_BACKOFF_MS = 1000
