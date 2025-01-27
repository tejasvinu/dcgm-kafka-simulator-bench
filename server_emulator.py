import asyncio
import random
import time
import logging
from producer import MetricsProducer
from config import NUM_SERVERS, GPUS_PER_SERVER, METRICS_INTERVAL

class DCGMServerEmulator:
    def __init__(self, server_id):
        self.server_id = server_id  # Remove f-string to keep numeric ID
        self.num_gpus = GPUS_PER_SERVER
        self.batch_size = 50  # Increased batch size
        self.metrics_cache = {}  # Cache for metric templates

    def generate_metric(self, gpu_id):
        # Cache the metric template for this GPU
        cache_key = f"{self.server_id}_{gpu_id}"
        if cache_key not in self.metrics_cache:
            server_hex = f"{self.server_id:04x}"
            gpu_hex = f"{gpu_id:02x}"
            uuid = f"GPU-{server_hex}{gpu_hex}{''.join([format(x, '02x') for x in range(14)])}"
            pci_id = f"{server_hex[:2]}:{gpu_hex}:{server_hex[2:]}:0"
            
            # Create template with placeholders
            self.metrics_cache[cache_key] = f'''DCGM_FI_DEV_SM_CLOCK{{node="{self.server_id}",gpu="{gpu_id}",UUID="{uuid}",pci_bus_id="{pci_id}",device="nvidia{gpu_id}",modelName="Tesla V100-SXM2-16GB",Hostname="node{self.server_id:04d}"}} {{sm_clock}}
DCGM_FI_DEV_GPU_TEMP{{node="{self.server_id}",gpu="{gpu_id}",UUID="{uuid}",pci_bus_id="{pci_id}",device="nvidia{gpu_id}",modelName="Tesla V100-SXM2-16GB",Hostname="node{self.server_id:04d}"}} {{gpu_temp}}
DCGM_FI_DEV_POWER_USAGE{{node="{self.server_id}",gpu="{gpu_id}",UUID="{uuid}",pci_bus_id="{pci_id}",device="nvidia{gpu_id}",modelName="Tesla V100-SXM2-16GB",Hostname="node{self.server_id:04d}"}} {{power}}
DCGM_FI_DEV_GPU_UTIL{{node="{self.server_id}",gpu="{gpu_id}",UUID="{uuid}",pci_bus_id="{pci_id}",device="nvidia{gpu_id}",modelName="Tesla V100-SXM2-16GB",Hostname="node{self.server_id:04d}"}} {{util}}'''

        # Generate random values
        sm_clock = 1200 + (self.server_id + gpu_id) % 300
        gpu_temp = 45 + (self.server_id + gpu_id) % 30
        power_usage = 300 + (self.server_id + gpu_id) % 100
        gpu_util = (self.server_id + gpu_id) % 100

        # Fill in the template
        return self.metrics_cache[cache_key].format(
            sm_clock=sm_clock,
            gpu_temp=gpu_temp,
            power=power_usage,
            util=gpu_util
        )

async def run_server(server_id, producer):
    emulator = DCGMServerEmulator(server_id)
    metrics_batch = []
    last_send_time = time.time()
    
    while True:
        current_time = time.time()
        if current_time - last_send_time >= METRICS_INTERVAL:
            for gpu_id in range(emulator.num_gpus):
                metrics_batch.append(emulator.generate_metric(gpu_id))
                
                if len(metrics_batch) >= emulator.batch_size:
                    try:
                        await producer.send_metric('\n'.join(metrics_batch))
                        metrics_batch = []
                    except Exception as e:
                        logging.error(f"Error sending metrics batch: {e}")
                        metrics_batch = []
            
            if metrics_batch:
                try:
                    await producer.send_metric('\n'.join(metrics_batch))
                    metrics_batch = []
                except Exception as e:
                    logging.error(f"Error sending remaining metrics: {e}")
                    metrics_batch = []
                    
            last_send_time = current_time
        else:
            await asyncio.sleep(0.1)  # Short sleep to prevent CPU spinning

async def main():
    producer = MetricsProducer()
    try:
        await producer.start()
        tasks = [
            run_server(server_id, producer)
            for server_id in range(NUM_SERVERS)
        ]
        await asyncio.gather(*tasks)
    finally:
        await producer.close()

if __name__ == '__main__':
    asyncio.run(main())
