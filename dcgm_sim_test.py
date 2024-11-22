from fastapi import FastAPI, Response, HTTPException, BackgroundTasks
from fastapi.responses import PlainTextResponse, JSONResponse
import uvicorn
import logging
from typing import Dict, Optional
import time
import resource
import sys
import psutil
from datetime import datetime
import asyncio
from collections import defaultdict
import argparse
import multiprocessing
from multiprocessing import Process, Queue
import signal
import socket
import random
import json
from functools import lru_cache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ResourceMonitor:
    @staticmethod
    def get_system_resources():
        return {
            'cpu_percent': psutil.cpu_percent(),
            'memory_percent': psutil.virtual_memory().percent,
            'open_files': len(psutil.Process().open_files()),
            'connections': len(psutil.Process().connections())
        }

    @staticmethod
    def check_resources():
        resources = ResourceMonitor.get_system_resources()
        if (resources['cpu_percent'] > 90 or 
            resources['memory_percent'] > 90 or 
            resources['open_files'] > 1000):
            return False
        return True

class MetricsCache:
    def __init__(self, ttl_seconds=60):
        self.cache = {}
        self.ttl = ttl_seconds
        self.template = '''# HELP DCGM_FI_DEV_SM_CLOCK SM clock frequency (in MHz)
# TYPE DCGM_FI_DEV_SM_CLOCK gauge
DCGM_FI_DEV_SM_CLOCK{{gpu="{gpu_id}",UUID="{uuid}",pci_bus_id="{pci_id}",device="nvidia{gpu_id}",modelName="Tesla V100-SXM2-16GB",Hostname="node{node_id:02d}",DCGM_FI_DRIVER_VERSION="450.51.06"}} {sm_clock}
# HELP DCGM_FI_DEV_GPU_UTIL GPU utilization (in %)
# TYPE DCGM_FI_DEV_GPU_UTIL gauge
DCGM_FI_DEV_GPU_UTIL{{gpu="{gpu_id}",UUID="{uuid}",pci_bus_id="{pci_id}",device="nvidia{gpu_id}",modelName="Tesla V100-SXM2-16GB",Hostname="node{node_id:02d}",DCGM_FI_DRIVER_VERSION="450.51.06"}} {gpu_util}
# HELP DCGM_FI_DEV_MEM_COPY_UTIL Memory utilization (in %)
# TYPE DCGM_FI_DEV_MEM_COPY_UTIL gauge
DCGM_FI_DEV_MEM_COPY_UTIL{{gpu="{gpu_id}",UUID="{uuid}",pci_bus_id="{pci_id}",device="nvidia{gpu_id}",modelName="Tesla V100-SXM2-16GB",Hostname="node{node_id:02d}",DCGM_FI_DRIVER_VERSION="450.51.06"}} {mem_util}
# HELP DCGM_FI_DEV_POWER_USAGE Power draw (in W)
# TYPE DCGM_FI_DEV_POWER_USAGE gauge
DCGM_FI_DEV_POWER_USAGE{{gpu="{gpu_id}",UUID="{uuid}",pci_bus_id="{pci_id}",device="nvidia{gpu_id}",modelName="Tesla V100-SXM2-16GB",Hostname="node{node_id:02d}",DCGM_FI_DRIVER_VERSION="450.51.06"}} {power_usage}
# HELP DCGM_FI_DEV_GPU_TEMP GPU temperature (in C)
# TYPE DCGM_FI_DEV_GPU_TEMP gauge
DCGM_FI_DEV_GPU_TEMP{{gpu="{gpu_id}",UUID="{uuid}",pci_bus_id="{pci_id}",device="nvidia{gpu_id}",modelName="Tesla V100-SXM2-16GB",Hostname="node{node_id:02d}",DCGM_FI_DRIVER_VERSION="450.51.06"}} {temp}'''
    
    def _get_gpu_uuid(self, node_id: int, gpu_id: int) -> str:
        """Generate deterministic UUID for GPU"""
        uuid_seed = f"node{node_id:02d}-gpu{gpu_id}"
        return f"GPU-{abs(hash(uuid_seed))}"[:36]
    
    def _get_pci_id(self, node_id: int, gpu_id: int) -> str:
        """Generate deterministic PCI ID"""
        h = abs(hash(f"pci-{node_id}-{gpu_id}"))
        return f"{h & 0xFF:02X}:{(h >> 8) & 0xFF:02X}:{(h >> 16) & 0xFF:02X}.0"
    
    def get_metrics(self, node_id: int) -> str:
        """Get metrics with efficient caching and generation"""
        current_time = time.time()
        
        if node_id in self.cache:
            cached_time, metrics = self.cache[node_id]
            if current_time - cached_time < self.ttl:
                return metrics
        
        metrics = self._generate_node_metrics(node_id)
        self.cache[node_id] = (current_time, metrics)
        return metrics
    
    def _generate_node_metrics(self, node_id: int) -> str:
        """Generate metrics for all GPUs in a node"""
        metrics_parts = []
        for gpu_id in range(4):  # 4 GPUs per node
            values = {
                'node_id': node_id,
                'gpu_id': gpu_id,
                'uuid': self._get_gpu_uuid(node_id, gpu_id),
                'pci_id': self._get_pci_id(node_id, gpu_id),
                'sm_clock': random.uniform(1300, 1400),
                'gpu_util': random.uniform(70, 95),
                'mem_util': random.uniform(60, 85),
                'power_usage': random.uniform(300, 350),
                'temp': random.uniform(50, 75)
            }
            metrics_parts.append(self.template.format(**values))
        
        return '\n'.join(metrics_parts)

class MetricsServer:
    def __init__(self, port: int, total_nodes: int):
        self.port = port
        self.total_nodes = total_nodes
        self.app = FastAPI()
        self.metrics_cache = MetricsCache()
        self.setup_routes()
    
    def setup_routes(self):
        @self.app.get("/metrics/{node_id}")
        async def get_metrics(node_id: int):
            if not (0 <= node_id < self.total_nodes):
                raise HTTPException(status_code=404, detail="Node ID not found")
            
            if not ResourceMonitor.check_resources():
                raise HTTPException(status_code=503, detail="System resources exceeded")
                
            return PlainTextResponse(self.metrics_cache.get_metrics(node_id))
        
        @self.app.get("/health")
        async def health_check():
            resources = ResourceMonitor.get_system_resources()
            return JSONResponse({
                "status": "healthy",
                "total_nodes": self.total_nodes,
                "resources": resources
            })
    
    def run(self):
        config = uvicorn.Config(
            self.app,
            host="0.0.0.0",
            port=self.port,
            log_level="info",
            limit_concurrency=1000,  # Increased for higher load
            timeout_keep_alive=30
        )
        server = uvicorn.Server(config)
        server.run()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50000)
    parser.add_argument("--total_nodes", type=int, required=True)
    args = parser.parse_args()
    
    # Set resource limits
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(hard, 50000), hard))
    
    logger.info(f"Starting DCGM simulator for {args.total_nodes} nodes on port {args.port}")
    
    server = MetricsServer(args.port, args.total_nodes)
    
    def signal_handler(signum, frame):
        logger.info("Shutting down gracefully...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        server.run()
    except Exception as e:
        logger.error(f"Error in main loop: {e}")

if __name__ == "__main__":
    main()