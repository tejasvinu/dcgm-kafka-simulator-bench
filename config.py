KAFKA_HOST = '10.180.8.24'
KAFKA_PORTS = [9092, 9093, 9094, 9095, 9096]
KAFKA_TOPIC = 'dcgm-metrics-test-optimized'
KAFKA_BOOTSTRAP_SERVERS = [f'{KAFKA_HOST}:{port}' for port in KAFKA_PORTS]

# Server emulator settings
NUM_SERVERS = 10
GPUS_PER_SERVER = 4

# Remove dynamic logic and set a constant interval
METRICS_INTERVAL = 1.0
