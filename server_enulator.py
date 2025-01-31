import asyncio
import random
import time
import logging
from producer import MetricsProducer
from config import NUM_SERVERS, GPUS_PER_SERVER, METRICS_INTERVAL

class DCGMServerEmulator:
    def __init__(self, server_id):
        self.server_id = f"server_{server_id}"
        self.num_gpus = GPUS_PER_SERVER
        self.hostname = f"node{server_id:02d}"

    def generate_metric(self, gpu_id):
        # Generate unique UUID for each GPU
        uuid = f"GPU-{hash(f'{self.server_id}-{gpu_id}'):016x}"
        pci_id = f"{hash(uuid) & 0xFF:02X}:{hash(uuid) >> 8 & 0xFF:02X}:{hash(uuid) >> 16 & 0xFF:02X}.0"
        
        # Generate realistic varying metrics
        sm_clock = 1200 + random.randint(-100, 100)
        mem_clock = 850 + random.randint(-50, 50)
        temp = 50 + random.randint(0, 30)
        power = 300 + random.randint(-50, 50)
        gpu_util = random.randint(30, 95)
        mem_util = random.randint(20, 85)
        mem_free = random.randint(4000, 12000)
        mem_used = 16384 - mem_free  # Assuming 16GB total memory

        return f"""# HELP DCGM_FI_DEV_SM_CLOCK SM clock frequency (in MHz)
# TYPE DCGM_FI_DEV_SM_CLOCK gauge
DCGM_FI_DEV_SM_CLOCK{{node="{self.server_id}",gpu="{gpu_id}",UUID="{uuid}",pci_bus_id="{pci_id}",device="nvidia{gpu_id}",modelName="Tesla V100-SXM2-16GB",Hostname="{self.hostname}",DCGM_FI_DRIVER_VERSION="450.51.06"}} {sm_clock}
# HELP DCGM_FI_DEV_MEM_CLOCK Memory clock frequency (in MHz)
# TYPE DCGM_FI_DEV_MEM_CLOCK gauge
DCGM_FI_DEV_MEM_CLOCK{{node="{self.server_id}",gpu="{gpu_id}",UUID="{uuid}",pci_bus_id="{pci_id}",device="nvidia{gpu_id}",modelName="Tesla V100-SXM2-16GB",Hostname="{self.hostname}",DCGM_FI_DRIVER_VERSION="450.51.06"}} {mem_clock}
# HELP DCGM_FI_DEV_GPU_TEMP GPU temperature (in C)
# TYPE DCGM_FI_DEV_GPU_TEMP gauge
DCGM_FI_DEV_GPU_TEMP{{node="{self.server_id}",gpu="{gpu_id}",UUID="{uuid}",pci_bus_id="{pci_id}",device="nvidia{gpu_id}",modelName="Tesla V100-SXM2-16GB",Hostname="{self.hostname}",DCGM_FI_DRIVER_VERSION="450.51.06"}} {temp}
# HELP DCGM_FI_DEV_POWER_USAGE Power draw (in W)
# TYPE DCGM_FI_DEV_POWER_USAGE gauge
DCGM_FI_DEV_POWER_USAGE{{node="{self.server_id}",gpu="{gpu_id}",UUID="{uuid}",pci_bus_id="{pci_id}",device="nvidia{gpu_id}",modelName="Tesla V100-SXM2-16GB",Hostname="{self.hostname}",DCGM_FI_DRIVER_VERSION="450.51.06"}} {power}
# HELP DCGM_FI_DEV_GPU_UTIL GPU utilization (in %)
# TYPE DCGM_FI_DEV_GPU_UTIL gauge
DCGM_FI_DEV_GPU_UTIL{{node="{self.server_id}",gpu="{gpu_id}",UUID="{uuid}",pci_bus_id="{pci_id}",device="nvidia{gpu_id}",modelName="Tesla V100-SXM2-16GB",Hostname="{self.hostname}",DCGM_FI_DRIVER_VERSION="450.51.06"}} {gpu_util}"""

async def run_server(server_id, producer):
    emulator = DCGMServerEmulator(server_id)
    failures = 0
    max_failures = 10  # Maximum consecutive failures before giving up
    
    while True:
        try:
            start_time = time.time()
            
            # Send metrics for each GPU sequentially to avoid overwhelming
            for gpu_id in range(emulator.num_gpus):
                metric = emulator.generate_metric(gpu_id)
                await producer.send_metric(metric)
                # Small delay between GPUs
                await asyncio.sleep(0.01)
            
            # Calculate and maintain proper interval
            elapsed = time.time() - start_time
            sleep_time = max(0.1, METRICS_INTERVAL - elapsed)  # Minimum 100ms interval
            await asyncio.sleep(sleep_time)
            failures = 0  # Reset failure counter on success
            
        except Exception as e:
            failures += 1
            logging.error(f"Server {server_id} encountered error: {e}")
            if failures >= max_failures:
                logging.error(f"Server {server_id} exceeded maximum failures, shutting down")
                break
            await asyncio.sleep(1)  # Brief pause before retry

async def main(num_servers):
    producer = MetricsProducer()
    tasks = []
    
    try:
        await producer.start()
        for server_id in range(num_servers):
            task = asyncio.create_task(run_server(server_id, producer))
            tasks.append(task)
            await asyncio.sleep(0.5)
        await asyncio.gather(*tasks)
    finally:
        await producer.close()

if __name__ == '__main__':
    asyncio.run(main(NUM_SERVERS))