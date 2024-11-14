from fastapi import FastAPI, Response, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse
import uvicorn
import logging
from typing import Dict, Optional
import time
from datetime import datetime
import asyncio
from collections import defaultdict
import argparse
from contextlib import asynccontextmanager
import socket
import random

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Metrics cache with TTL
class MetricsCache:
    def __init__(self, ttl_seconds=60):
        self.cache = {}
        self.ttl = ttl_seconds
        self.base_metrics = self._load_base_metrics()
        
    def get_metrics(self, node_id: int) -> str:
        """Get metrics for a specific node, with randomization for simulation"""
        current_time = time.time()
        
        # Check cache
        if node_id in self.cache:
            cached_time, metrics = self.cache[node_id]
            if current_time - cached_time < self.ttl:
                return metrics
        
        # Generate new metrics for this node
        metrics = self._generate_node_metrics(node_id)
        self.cache[node_id] = (current_time, metrics)
        return metrics
    
    def _generate_node_metrics(self, node_id: int) -> str:
        """Generate metrics with some randomization for realism"""
        metrics_lines = []
        for gpu_id in range(4):  # 4 GPUs per node
            # Add randomization to metric values
            sm_clock = random.uniform(1300, 1400)
            gpu_util = random.uniform(70, 95)
            mem_util = random.uniform(60, 85)
            power_usage = random.uniform(300, 350)
            
            # Format metrics with node and GPU information
            gpu_metrics = """# HELP DCGM_FI_DEV_SM_CLOCK SM clock frequency (in MHz)
# TYPE DCGM_FI_DEV_SM_CLOCK gauge
DCGM_FI_DEV_SM_CLOCK{{node="0",gpu="0",UUID="GPU-65b748f6d35fc670c5ad13b8d9dba852",pci_bus_id="1F:CE:62.0",device="nvidia0",modelName="Tesla V100-SXM2-16GB",Hostname="node00",DCGM_FI_DRIVER_VERSION="450.51.06"}} 1350
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
        return gpu_metrics

class MetricsServer:
    def __init__(self, start_port: int, num_nodes: int):
        self.start_port = start_port
        self.num_nodes = num_nodes
        self.app = FastAPI()
        self.metrics_cache = MetricsCache()
        self.setup_routes()
        
    def setup_routes(self):
        @self.app.get("/metrics")
        async def get_metrics(port: Optional[int] = None):
            if port is None:
                raise HTTPException(status_code=400, detail="Port parameter is required")
            
            # Calculate node_id from port
            node_id = port - self.start_port
            if not (0 <= node_id < self.num_nodes):
                raise HTTPException(status_code=404, detail="Invalid node ID")
                
            return PlainTextResponse(self.metrics_cache.get_metrics(node_id))
            
        @self.app.get("/health")
        async def health_check():
            return JSONResponse({"status": "healthy"})
    
    def run(self, host: str = "0.0.0.0"):
        """Start the server"""
        config = uvicorn.Config(
            self.app,
            host=host,
            port=self.start_port,
            log_level="info",
            limit_concurrency=1000,
            limit_max_requests=10000,
            timeout_keep_alive=120,
            loop="uvloop"
        )
        server = uvicorn.Server(config)
        server.run()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_ports", type=int, required=True)
    parser.add_argument("--start_port", type=int, required=True)
    parser.add_argument("--total_nodes", type=int, required=True)
    args = parser.parse_args()
    
    logger.info(f"Starting metrics server for {args.total_nodes} nodes")
    server = MetricsServer(args.start_port, args.total_nodes)
    server.run()

if __name__ == "__main__":
    main()