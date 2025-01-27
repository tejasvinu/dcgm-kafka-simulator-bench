# Kafka Configuration
KAFKA_HOST = '10.180.8.24'
KAFKA_PORTS = [9092, 9093, 9094, 9095, 9096]
KAFKA_TOPIC = 'dcgm-metrics-test-optimized'
KAFKA_BOOTSTRAP_SERVERS = [f'{KAFKA_HOST}:{port}' for port in KAFKA_PORTS]

# Server Emulator Configuration
NUM_SERVERS = 4  # Default value
GPUS_PER_SERVER = 4
METRICS_INTERVAL = 1.0  # seconds

def update_num_servers(new_count):
    """Update the number of servers dynamically"""
    global NUM_SERVERS
    NUM_SERVERS = new_count
    return NUM_SERVERS
