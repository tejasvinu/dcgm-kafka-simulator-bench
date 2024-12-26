KAFKA_BOOTSTRAP_SERVERS = ['10.180.8.24:9092', '10.180.8.24:9093', '10.180.8.24:9094','10.180.8.24:9095','10.180.8.24:9096']
KAFKA_TOPIC = 'dcgm-metrics-test-optimized'  # Updated topic name
NUM_SERVERS = 32
GPUS_PER_SERVER = 4
METRICS_INTERVAL = 1

# Consumer configuration
NUM_CONSUMERS = 4
CONSUMER_GROUP = 'dcgm-metrics-group'
BATCH_SIZE = 1048576  # Increased to 1MB to match producer batch size
STATS_INTERVAL = 5

# Producer configuration
PRODUCER_COMPRESSION = 'zstd'  # Changed to zstd compression
PRODUCER_BATCH_SIZE = 1048576  # 1MB
PRODUCER_LINGER_MS = 100
MAX_REQUEST_SIZE = 1048576  # 1MB

# Kafka topic configuration
TOPIC_CONFIG = {
    'num_partitions': 240,  # Optimized for 4 consumers and 5 brokers
    'replication_factor': 3,
    'min_insync_replicas': 1,
    'compression_type': 'zstd',
    'cleanup_policy': 'delete',
    'retention_ms': 3600000,  # 1 hour retention
    'retention_bytes': -1
}
