# Simplified broker configuration - use host:port format without explicit broker IDs
KAFKA_BOOTSTRAP_SERVERS = ['10.180.8.24:9092', '10.180.8.24:9093', '10.180.8.24:9094','10.180.8.24:9095','10.180.8.24:9096']
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

# Connection settings
METADATA_MAX_AGE_MS = 30000  # 30 seconds
REQUEST_TIMEOUT_MS = 30000
MAX_POLL_INTERVAL_MS = 300000
CONNECTIONS_MAX_IDLE_MS = 60000
RETRY_BACKOFF_MS = 1000
