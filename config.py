KAFKA_HOST = '10.180.8.24'
KAFKA_PORTS = [9092, 9093, 9094, 9095, 9096]
KAFKA_TOPIC = 'dcgm-metrics-test-optimized'
KAFKA_BOOTSTRAP_SERVERS = [f'{KAFKA_HOST}:{port}' for port in KAFKA_PORTS]

# Server emulator settings
NUM_SERVERS = 10
GPUS_PER_SERVER = 4

# Remove dynamic logic and set a constant interval
METRICS_INTERVAL = 1.0

# Server scaling configuration
SERVER_SCALE_CONFIGS = [
    8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192
]

# Dynamic consumer scaling
CONSUMERS_PER_SERVER = 0.01  # 1 consumer per 100 servers
MIN_CONSUMERS = 4
MAX_CONSUMERS = 64

def calculate_num_consumers(num_servers):
    """Calculate optimal number of consumers based on server count"""
    num_consumers = max(MIN_CONSUMERS, min(MAX_CONSUMERS, 
                       int(num_servers * CONSUMERS_PER_SERVER)))
    return num_consumers

# Default values if not specified
NUM_SERVERS = SERVER_SCALE_CONFIGS[0]
NUM_CONSUMERS = calculate_num_consumers(NUM_SERVERS)
