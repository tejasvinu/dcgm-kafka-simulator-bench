import asyncio
import random
import time
import logging
from producer import MetricsProducer
from config import NUM_SERVERS, GPUS_PER_SERVER, METRICS_INTERVAL

class DCGMServerEmulator:
    def __init__(self, server_id, batch_size=10):
        self.server_id = f"server_{server_id}"
        self.num_gpus = GPUS_PER_SERVER
        self.batch_size = batch_size
        self.metrics_buffer = []
        self.last_send_time = time.time()

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

    async def send_metrics_batch(self, producer):
        if not self.metrics_buffer:
            return
            
        try:
            combined_metric = "\n".join(self.metrics_buffer)
            await producer.send_metric(combined_metric, key=self.server_id.encode())
            self.metrics_buffer = []
            self.last_send_time = time.time()
        except Exception as e:
            logging.error(f"Error sending metrics batch for {self.server_id}: {e}")
            self.metrics_buffer = []  # Clear buffer on error

async def run_servers(num_servers, producer):
    """Run multiple server emulators with dynamic batch sizing"""
    logging.info(f"Starting {num_servers} server emulators...")
    
    # Adjust batch size based on server count
    batch_size = min(50, max(10, num_servers // 100))  # Scale batch size with server count
    emulators = [DCGMServerEmulator(i, batch_size) for i in range(num_servers)]
    
    while True:
        tasks = []
        for emulator in emulators:
            for gpu_id in range(emulator.num_gpus):
                metric = emulator.generate_metric(gpu_id)
                emulator.metrics_buffer.append(metric)
                
                if len(emulator.metrics_buffer) >= emulator.batch_size:
                    tasks.append(emulator.send_metrics_batch(producer))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        await asyncio.sleep(METRICS_INTERVAL)

async def main(num_servers=None):
    """Main entry point with configurable server count"""
    if num_servers is None:
        num_servers = NUM_SERVERS
        
    producer = MetricsProducer()
    try:
        await producer.start()
        await run_servers(num_servers, producer)
    finally:
        await producer.close()

if __name__ == '__main__':
    asyncio.run(main(NUM_SERVERS))