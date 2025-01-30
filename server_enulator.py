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

    def generate_metric(self, gpu_id):
        return r""""# HELP DCGM_FI_DEV_SM_CLOCK SM clock frequency (in MHz)
# TYPE DCGM_FI_DEV_SM_CLOCK gauge
DCGM_FI_DEV_SM_CLOCK{node="0",gpu="0",UUID="GPU-65b748f6d35fc670c5ad13b8d9dba852",pci_bus_id="1F:CE:62.0",device="nvidia0",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 1350
# HELP DCGM_FI_DEV_MEM_CLOCK Memory clock frequency (in MHz)
# TYPE DCGM_FI_DEV_MEM_CLOCK gauge
DCGM_FI_DEV_MEM_CLOCK{node="0",gpu="0",UUID="GPU-65b748f6d35fc670c5ad13b8d9dba852",pci_bus_id="1F:CE:62.0",device="nvidia0",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 877
# HELP DCGM_FI_DEV_MEMORY_TEMP Memory temperature (in C)
# TYPE DCGM_FI_DEV_MEMORY_TEMP gauge
DCGM_FI_DEV_MEMORY_TEMP{node="0",gpu="0",UUID="GPU-65b748f6d35fc670c5ad13b8d9dba852",pci_bus_id="1F:CE:62.0",device="nvidia0",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 74
# HELP DCGM_FI_DEV_GPU_TEMP GPU temperature (in C)
# TYPE DCGM_FI_DEV_GPU_TEMP gauge
DCGM_FI_DEV_GPU_TEMP{node="0",gpu="0",UUID="GPU-65b748f6d35fc670c5ad13b8d9dba852",pci_bus_id="1F:CE:62.0",device="nvidia0",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 74
# HELP DCGM_FI_DEV_POWER_USAGE Power draw (in W)
# TYPE DCGM_FI_DEV_POWER_USAGE gauge
DCGM_FI_DEV_POWER_USAGE{node="0",gpu="0",UUID="GPU-65b748f6d35fc670c5ad13b8d9dba852",pci_bus_id="1F:CE:62.0",device="nvidia0",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 340.3
# HELP DCGM_FI_DEV_GPU_UTIL GPU utilization (in %)
# TYPE DCGM_FI_DEV_GPU_UTIL gauge
DCGM_FI_DEV_GPU_UTIL{node="0",gpu="0",UUID="GPU-65b748f6d35fc670c5ad13b8d9dba852",pci_bus_id="1F:CE:62.0",device="nvidia0",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 80
# HELP DCGM_FI_DEV_MEM_COPY_UTIL Memory utilization (in %)
# TYPE DCGM_FI_DEV_MEM_COPY_UTIL gauge
DCGM_FI_DEV_MEM_COPY_UTIL{node="0",gpu="0",UUID="GPU-65b748f6d35fc670c5ad13b8d9dba852",pci_bus_id="1F:CE:62.0",device="nvidia0",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 72
# HELP DCGM_FI_DEV_FB_FREE Frame buffer memory free (in MB)
# TYPE DCGM_FI_DEV_FB_FREE gauge
DCGM_FI_DEV_FB_FREE{node="0",gpu="0",UUID="GPU-65b748f6d35fc670c5ad13b8d9dba852",pci_bus_id="1F:CE:62.0",device="nvidia0",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 4587.52
# HELP DCGM_FI_DEV_FB_USED Frame buffer memory used (in MB)
# TYPE DCGM_FI_DEV_FB_USED gauge
DCGM_FI_DEV_FB_USED{node="0",gpu="0",UUID="GPU-65b748f6d35fc670c5ad13b8d9dba852",pci_bus_id="1F:CE:62.0",device="nvidia0",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 11796.48
# HELP DCGM_FI_DEV_SM_CLOCK SM clock frequency (in MHz)
# TYPE DCGM_FI_DEV_SM_CLOCK gauge
DCGM_FI_DEV_SM_CLOCK{node="0",gpu="1",UUID="GPU-f9a4ee7519dd133c5f270e783acfa8f1",pci_bus_id="3D:26:BB.0",device="nvidia1",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 1279
# HELP DCGM_FI_DEV_MEM_CLOCK Memory clock frequency (in MHz)
# TYPE DCGM_FI_DEV_MEM_CLOCK gauge
DCGM_FI_DEV_MEM_CLOCK{node="0",gpu="1",UUID="GPU-f9a4ee7519dd133c5f270e783acfa8f1",pci_bus_id="3D:26:BB.0",device="nvidia1",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 877
# HELP DCGM_FI_DEV_MEMORY_TEMP Memory temperature (in C)
# TYPE DCGM_FI_DEV_MEMORY_TEMP gauge
DCGM_FI_DEV_MEMORY_TEMP{node="0",gpu="1",UUID="GPU-f9a4ee7519dd133c5f270e783acfa8f1",pci_bus_id="3D:26:BB.0",device="nvidia1",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 48
# HELP DCGM_FI_DEV_GPU_TEMP GPU temperature (in C)
# TYPE DCGM_FI_DEV_GPU_TEMP gauge
DCGM_FI_DEV_GPU_TEMP{node="0",gpu="1",UUID="GPU-f9a4ee7519dd133c5f270e783acfa8f1",pci_bus_id="3D:26:BB.0",device="nvidia1",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 48
# HELP DCGM_FI_DEV_POWER_USAGE Power draw (in W)
# TYPE DCGM_FI_DEV_POWER_USAGE gauge
DCGM_FI_DEV_POWER_USAGE{node="0",gpu="1",UUID="GPU-f9a4ee7519dd133c5f270e783acfa8f1",pci_bus_id="3D:26:BB.0",device="nvidia1",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 316.58
# HELP DCGM_FI_DEV_GPU_UTIL GPU utilization (in %)
# TYPE DCGM_FI_DEV_GPU_UTIL gauge
DCGM_FI_DEV_GPU_UTIL{node="0",gpu="1",UUID="GPU-f9a4ee7519dd133c5f270e783acfa8f1",pci_bus_id="3D:26:BB.0",device="nvidia1",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 33
# HELP DCGM_FI_DEV_MEM_COPY_UTIL Memory utilization (in %)
# TYPE DCGM_FI_DEV_MEM_COPY_UTIL gauge
DCGM_FI_DEV_MEM_COPY_UTIL{node="0",gpu="1",UUID="GPU-f9a4ee7519dd133c5f270e783acfa8f1",pci_bus_id="3D:26:BB.0",device="nvidia1",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 29
# HELP DCGM_FI_DEV_FB_FREE Frame buffer memory free (in MB)
# TYPE DCGM_FI_DEV_FB_FREE gauge
DCGM_FI_DEV_FB_FREE{node="0",gpu="1",UUID="GPU-f9a4ee7519dd133c5f270e783acfa8f1",pci_bus_id="3D:26:BB.0",device="nvidia1",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 11632.64
# HELP DCGM_FI_DEV_FB_USED Frame buffer memory used (in MB)
# TYPE DCGM_FI_DEV_FB_USED gauge
DCGM_FI_DEV_FB_USED{node="0",gpu="1",UUID="GPU-f9a4ee7519dd133c5f270e783acfa8f1",pci_bus_id="3D:26:BB.0",device="nvidia1",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 4751.36
# HELP DCGM_FI_DEV_SM_CLOCK SM clock frequency (in MHz)
# TYPE DCGM_FI_DEV_SM_CLOCK gauge
DCGM_FI_DEV_SM_CLOCK{node="0",gpu="2",UUID="GPU-d795a930a31b53c90b362838be3d7d6f",pci_bus_id="24:35:0B.0",device="nvidia2",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 1314
# HELP DCGM_FI_DEV_MEM_CLOCK Memory clock frequency (in MHz)
# TYPE DCGM_FI_DEV_MEM_CLOCK gauge
DCGM_FI_DEV_MEM_CLOCK{node="0",gpu="2",UUID="GPU-d795a930a31b53c90b362838be3d7d6f",pci_bus_id="24:35:0B.0",device="nvidia2",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 877
# HELP DCGM_FI_DEV_MEMORY_TEMP Memory temperature (in C)
# TYPE DCGM_FI_DEV_MEMORY_TEMP gauge
DCGM_FI_DEV_MEMORY_TEMP{node="0",gpu="2",UUID="GPU-d795a930a31b53c90b362838be3d7d6f",pci_bus_id="24:35:0B.0",device="nvidia2",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 60
# HELP DCGM_FI_DEV_GPU_TEMP GPU temperature (in C)
# TYPE DCGM_FI_DEV_GPU_TEMP gauge
DCGM_FI_DEV_GPU_TEMP{node="0",gpu="2",UUID="GPU-d795a930a31b53c90b362838be3d7d6f",pci_bus_id="24:35:0B.0",device="nvidia2",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 60
# HELP DCGM_FI_DEV_POWER_USAGE Power draw (in W)
# TYPE DCGM_FI_DEV_POWER_USAGE gauge
DCGM_FI_DEV_POWER_USAGE{node="0",gpu="2",UUID="GPU-d795a930a31b53c90b362838be3d7d6f",pci_bus_id="24:35:0B.0",device="nvidia2",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 328.03
# HELP DCGM_FI_DEV_GPU_UTIL GPU utilization (in %)
# TYPE DCGM_FI_DEV_GPU_UTIL gauge
DCGM_FI_DEV_GPU_UTIL{node="0",gpu="2",UUID="GPU-d795a930a31b53c90b362838be3d7d6f",pci_bus_id="24:35:0B.0",device="nvidia2",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 56
# HELP DCGM_FI_DEV_MEM_COPY_UTIL Memory utilization (in %)
# TYPE DCGM_FI_DEV_MEM_COPY_UTIL gauge
DCGM_FI_DEV_MEM_COPY_UTIL{node="0",gpu="2",UUID="GPU-d795a930a31b53c90b362838be3d7d6f",pci_bus_id="24:35:0B.0",device="nvidia2",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 50
# HELP DCGM_FI_DEV_FB_FREE Frame buffer memory free (in MB)
# TYPE DCGM_FI_DEV_FB_FREE gauge
DCGM_FI_DEV_FB_FREE{node="0",gpu="2",UUID="GPU-d795a930a31b53c90b362838be3d7d6f",pci_bus_id="24:35:0B.0",device="nvidia2",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 8192.0
# HELP DCGM_FI_DEV_FB_USED Frame buffer memory used (in MB)
# TYPE DCGM_FI_DEV_FB_USED gauge
DCGM_FI_DEV_FB_USED{node="0",gpu="2",UUID="GPU-d795a930a31b53c90b362838be3d7d6f",pci_bus_id="24:35:0B.0",device="nvidia2",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 8192.0
# HELP DCGM_FI_DEV_SM_CLOCK SM clock frequency (in MHz)
# TYPE DCGM_FI_DEV_SM_CLOCK gauge
DCGM_FI_DEV_SM_CLOCK{node="0",gpu="3",UUID="GPU-f396eded927405e15b27af379411806b",pci_bus_id="DE:E9:DA.0",device="nvidia3",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 1319
# HELP DCGM_FI_DEV_MEM_CLOCK Memory clock frequency (in MHz)
# TYPE DCGM_FI_DEV_MEM_CLOCK gauge
DCGM_FI_DEV_MEM_CLOCK{node="0",gpu="3",UUID="GPU-f396eded927405e15b27af379411806b",pci_bus_id="DE:E9:DA.0",device="nvidia3",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 877
# HELP DCGM_FI_DEV_MEMORY_TEMP Memory temperature (in C)
# TYPE DCGM_FI_DEV_MEMORY_TEMP gauge
DCGM_FI_DEV_MEMORY_TEMP{node="0",gpu="3",UUID="GPU-f396eded927405e15b27af379411806b",pci_bus_id="DE:E9:DA.0",device="nvidia3",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 62
# HELP DCGM_FI_DEV_GPU_TEMP GPU temperature (in C)
# TYPE DCGM_FI_DEV_GPU_TEMP gauge
DCGM_FI_DEV_GPU_TEMP{node="0",gpu="3",UUID="GPU-f396eded927405e15b27af379411806b",pci_bus_id="DE:E9:DA.0",device="nvidia3",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 62
# HELP DCGM_FI_DEV_POWER_USAGE Power draw (in W)
# TYPE DCGM_FI_DEV_POWER_USAGE gauge
DCGM_FI_DEV_POWER_USAGE{node="0",gpu="3",UUID="GPU-f396eded927405e15b27af379411806b",pci_bus_id="DE:E9:DA.0",device="nvidia3",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 329.74
# HELP DCGM_FI_DEV_GPU_UTIL GPU utilization (in %)
# TYPE DCGM_FI_DEV_GPU_UTIL gauge
DCGM_FI_DEV_GPU_UTIL{node="0",gpu="3",UUID="GPU-f396eded927405e15b27af379411806b",pci_bus_id="DE:E9:DA.0",device="nvidia3",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 59
# HELP DCGM_FI_DEV_MEM_COPY_UTIL Memory utilization (in %)
# TYPE DCGM_FI_DEV_MEM_COPY_UTIL gauge
DCGM_FI_DEV_MEM_COPY_UTIL{node="0",gpu="3",UUID="GPU-f396eded927405e15b27af379411806b",pci_bus_id="DE:E9:DA.0",device="nvidia3",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 53
# HELP DCGM_FI_DEV_FB_FREE Frame buffer memory free (in MB)
# TYPE DCGM_FI_DEV_FB_FREE gauge
DCGM_FI_DEV_FB_FREE{node="0",gpu="3",UUID="GPU-f396eded927405e15b27af379411806b",pci_bus_id="DE:E9:DA.0",device="nvidia3",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 7700.48
# HELP DCGM_FI_DEV_FB_USED Frame buffer memory used (in MB)
# TYPE DCGM_FI_DEV_FB_USED gauge
DCGM_FI_DEV_FB_USED{node="0",gpu="3",UUID="GPU-f396eded927405e15b27af379411806b",pci_bus_id="DE:E9:DA.0",device="nvidia3",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"} 8683.52
"""

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