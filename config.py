import time
# Kafka Configuration
KAFKA_HOST = '10.180.8.24'
KAFKA_PORTS = [9092, 9093, 9094, 9095, 9096]
KAFKA_TOPIC = 'dcgm-metrics-test-optimized'
KAFKA_BOOTSTRAP_SERVERS = [f'{KAFKA_HOST}:{port}' for port in KAFKA_PORTS]

# Benchmark Configuration
BENCHMARK_DURATION = 300  # seconds
WARMUP_DURATION = 30     # seconds for system warmup
COOLDOWN_DURATION = 30   # seconds between scales
NUM_SERVERS = 4         # Default value
GPUS_PER_SERVER = 4
METRICS_INTERVAL = 1.0  # seconds

# Producer Configuration
PRODUCER_BATCH_SIZE = 16384
PRODUCER_LINGER_MS = 100
PRODUCER_COMPRESSION = 'zstd'

# Consumer Configuration
CONSUMER_GROUP = f'benchmark_group_{int(time.time())}'  # Unique group per run
CONSUMER_TIMEOUT = 1.0  # seconds

def update_num_servers(new_count):
    """Update the number of servers dynamically"""
    global NUM_SERVERS
    NUM_SERVERS = new_count
    return NUM_SERVERS
