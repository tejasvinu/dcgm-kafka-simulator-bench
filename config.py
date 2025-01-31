KAFKA_HOST = '10.180.8.24'
KAFKA_PORTS = [9092, 9093, 9094, 9095, 9096]
KAFKA_TOPIC = 'dcgm-metrics-test-optimized'
KAFKA_BOOTSTRAP_SERVERS = [f'{KAFKA_HOST}:{port}' for port in KAFKA_PORTS]

# Server emulator settings
NUM_SERVERS = 10
GPUS_PER_SERVER = 4

# Adjust metrics interval based on server count
def get_metrics_interval(num_servers):
    if num_servers <= 8:
        return 1.0
    elif num_servers <= 32:
        return 2.0
    elif num_servers <= 128:
        return 4.0
    else:
        return 8.0

# Default interval
METRICS_INTERVAL = 1.0  # Will be adjusted dynamically
