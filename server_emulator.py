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

    def generate_metric(self, gpu_id):
        # Generate unique metrics for each server/GPU combination
        server_hex = f"{self.server_id:04x}"
        gpu_hex = f"{gpu_id:02x}"
        uuid = f"GPU-{server_hex}{gpu_hex}{''.join([format(x, '02x') for x in range(14)])}"
        pci_id = f"{server_hex[:2]}:{gpu_hex}:{server_hex[2:]}:0"
        
        # Generate random but realistic values
        sm_clock = 1200 + (self.server_id + gpu_id) % 300  # Range: 1200-1500 MHz
        mem_clock = 850 + (self.server_id + gpu_id) % 100  # Range: 850-950 MHz
        gpu_temp = 45 + (self.server_id + gpu_id) % 30     # Range: 45-75°C
        power_usage = 300 + (self.server_id + gpu_id) % 100  # Range: 300-400W
        gpu_util = (self.server_id + gpu_id) % 100         # Range: 0-100%
        
        return f'''# HELP DCGM_FI_DEV_SM_CLOCK SM clock frequency (in MHz)
# TYPE DCGM_FI_DEV_SM_CLOCK gauge
DCGM_FI_DEV_SM_CLOCK{{node="{self.server_id}",gpu="{gpu_id}",UUID="{uuid}",pci_bus_id="{pci_id}",device="nvidia{gpu_id}",modelName="Tesla V100-SXM2-16GB",Hostname="node{self.server_id:04d}",DCGM_FI_DRIVER_VERSION="450.51.06"}} {sm_clock}
# HELP DCGM_FI_DEV_GPU_TEMP GPU temperature (in C)
# TYPE DCGM_FI_DEV_GPU_TEMP gauge
DCGM_FI_DEV_GPU_TEMP{{node="{self.server_id}",gpu="{gpu_id}",UUID="{uuid}",pci_bus_id="{pci_id}",device="nvidia{gpu_id}",modelName="Tesla V100-SXM2-16GB",Hostname="node{self.server_id:04d}",DCGM_FI_DRIVER_VERSION="450.51.06"}} {gpu_temp}
# HELP DCGM_FI_DEV_POWER_USAGE Power draw (in W)
# TYPE DCGM_FI_DEV_POWER_USAGE gauge
DCGM_FI_DEV_POWER_USAGE{{node="{self.server_id}",gpu="{gpu_id}",UUID="{uuid}",pci_bus_id="{pci_id}",device="nvidia{gpu_id}",modelName="Tesla V100-SXM2-16GB",Hostname="node{self.server_id:04d}",DCGM_FI_DRIVER_VERSION="450.51.06"}} {power_usage}
# HELP DCGM_FI_DEV_GPU_UTIL GPU utilization (in %)
# TYPE DCGM_FI_DEV_GPU_UTIL gauge
DCGM_FI_DEV_GPU_UTIL{{node="{self.server_id}",gpu="{gpu_id}",UUID="{uuid}",pci_bus_id="{pci_id}",device="nvidia{gpu_id}",modelName="Tesla V100-SXM2-16GB",Hostname="node{self.server_id:04d}",DCGM_FI_DRIVER_VERSION="450.51.06"}} {gpu_util}'''

async def run_server(server_id, producer):
    emulator = DCGMServerEmulator(server_id)
    batch_size = 10  # Number of metrics to batch together
    metrics_batch = []
    
    while True:
        for gpu_id in range(emulator.num_gpus):
            metric = emulator.generate_metric(gpu_id)
            metrics_batch.append(metric)
            
            if len(metrics_batch) >= batch_size:
                try:
                    combined_metric = "\n".join(metrics_batch)
                    await producer.send_metric(combined_metric)
                    metrics_batch = []
                except Exception as e:
                    logging.error(f"Error sending metrics batch: {e}")
                    metrics_batch = []
                
        if metrics_batch:  # Send any remaining metrics
            try:
                combined_metric = "\n".join(metrics_batch)
                await producer.send_metric(combined_metric)
                metrics_batch = []
            except Exception as e:
                logging.error(f"Error sending remaining metrics: {e}")
                metrics_batch = []
                
        await asyncio.sleep(METRICS_INTERVAL)

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
