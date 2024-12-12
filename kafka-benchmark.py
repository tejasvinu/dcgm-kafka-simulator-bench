import asyncio
from kafka import KafkaProducer
import json
import time
import random

NUM_SERVERS = 8192  # Adjust this value between 8 and 8192
GPUS_PER_SERVER = 4

# Placeholder data for GPU metrics
placeholder_data = {
    "timestamp": 0,
    "server_id": "",
    "gpu_id": 0,
    "metrics": {
        "temperature": 0.0,
        "utilization": 0.0,
        "memory_used": 0.0
    }
}

async def send_metrics(producer, server_id, gpu_id):
    while True:
        data = placeholder_data.copy()
        data["timestamp"] = int(time.time())
        data["server_id"] = server_id
        data["gpu_id"] = gpu_id
        data["metrics"]["temperature"] = random.uniform(30.0, 85.0)
        data["metrics"]["utilization"] = random.uniform(0.0, 100.0)
        data["metrics"]["memory_used"] = random.uniform(0.0, 16.0)  # Assuming 16GB GPUs

        message = json.dumps(data).encode('utf-8')
        producer.send('dcgm-metrics-topic', message)
        await asyncio.sleep(1)  # Adjust the interval as needed

async def main():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    tasks = []
    for server_num in range(NUM_SERVERS):
        server_id = f'server_{server_num}'
        for gpu_id in range(GPUS_PER_SERVER):
            task = asyncio.create_task(send_metrics(producer, server_id, gpu_id))
            tasks.append(task)
    await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
