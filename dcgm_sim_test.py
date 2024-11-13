from wsgiref.simple_server import make_server, WSGIRequestHandler
from wsgiref.handlers import SimpleHandler  # Add this import
import logging
from fastapi import FastAPI, Response
from fastapi.responses import PlainTextResponse
from starlette.middleware.wsgi import WSGIMiddleware
from waitress import serve
import uvicorn
import multiprocessing
import time
from typing import Optional
import logging
from functools import lru_cache
import psutil
import os
import argparse
import asyncio
import signal
import sys
import socket
import errno

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
class Settings:
    def __init__(self):
        self.METRICS_CACHE_TTL = 60  # seconds
        self.WORKER_CONNECTIONS = 1000
        self.BACKLOG_SIZE = 2048
        self.KEEPALIVE = 65

settings = Settings()

# Create and expose the WSGI application at module level
def create_app():
    def app(environ, start_response):
        path = environ.get('PATH_INFO', '')
        if path == '/metrics':
            metrics = get_cached_metrics()
            status = '200 OK'
            headers = [('Content-type', 'text/plain')]
            start_response(status, headers)
            return [metrics.encode('utf-8')]
        elif path == '/health':
            status = '200 OK'
            headers = [('Content-type', 'application/json')]
            start_response(status, headers)
            return [b'{"status": "healthy"}']
        else:
            status = '404 Not Found'
            headers = [('Content-type', 'text/plain')]
            start_response(status, headers)
            return [b'Not Found']
    return app

# Export the WSGI application at module level
wsgi_app = create_app()

# Cache the metrics response
@lru_cache(maxsize=1)
def get_cached_metrics():
    CONSTANT_METRICS = """# HELP DCGM_FI_DEV_SM_CLOCK SM clock frequency (in MHz)
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
    return CONSTANT_METRICS

class MetricsServer:
    def __init__(self, host: str = "0.0.0.0", start_port: int = 51000, num_ports: int = 1, total_nodes: int = 1):
        self.host = host
        self.start_port = start_port
        self.processes = []
        self.num_ports = total_nodes  # Use total_nodes as number of ports (one per node)
        self.total_nodes = total_nodes
        self.running = True
        self.app = wsgi_app

        # Add new configurations for resource management
        self.file_limit = self.get_file_limit()
        self.max_servers = min(total_nodes, self.file_limit // 8)  # 8 FDs per server approx
        self.server_pools = []
        self.current_pool = 0

        # Get available file descriptors and adjust pool size
        self.file_limit = self.get_file_limit()
        self.max_servers_per_pool = max(1, min(
            total_nodes,
            (self.file_limit - 100) // 4  # Reserve 100 FDs for system use, 4 FDs per server
        ))
        self.num_pools = (total_nodes + self.max_servers_per_pool - 1) // self.max_servers_per_pool
        
        logger.info(f"File descriptor limit: {self.file_limit}")
        logger.info(f"Max servers per pool: {self.max_servers_per_pool}")
        logger.info(f"Number of pools required: {self.num_pools}")

        # Improve file descriptor management
        self.set_fd_limits()
        self.available_fds = self.get_available_fds()
        self.fds_per_server = 10  # Conservative estimate
        self.max_concurrent_servers = max(1, self.available_fds // self.fds_per_server)
        
        # Adjust server pools based on available FDs
        self.servers_per_pool = min(
            self.max_concurrent_servers,
            max(1, total_nodes // (os.cpu_count() or 1))
        )
        self.num_pools = (total_nodes + self.servers_per_pool - 1) // self.servers_per_pool

        logger.info(f"Available file descriptors: {self.available_fds}")
        logger.info(f"Maximum concurrent servers: {self.max_concurrent_servers}")
        logger.info(f"Servers per pool: {self.servers_per_pool}")
        logger.info(f"Number of pools: {self.num_pools}")

    def set_fd_limits(self):
        """Attempt to raise file descriptor limits"""
        try:
            import resource
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            target = 1048576  # Target 1M file descriptors
            
            # Try to set to target or hard limit
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (target, target))
            except ValueError:
                try:
                    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
                except ValueError:
                    pass  # Keep current limits if we can't increase them
            
            new_soft, new_hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            logger.info(f"File descriptor limits: soft={new_soft}, hard={new_hard}")
        except Exception as e:
            logger.warning(f"Could not adjust file descriptor limits: {e}")

    def get_available_fds(self):
        """Get current available file descriptors"""
        try:
            import resource
            soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
            return max(soft - 100, 100)  # Reserve 100 FDs for system use
        except Exception as e:
            logger.warning(f"Could not get file descriptor limits: {e}")
            return 900  # Conservative default

    def get_file_limit(self):
        """Get current process file descriptor limit"""
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        try:
            # Try to increase limit to hard limit
            resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
            return hard
        except Exception as e:
            logger.warning(f"Could not increase file limit: {e}")
            return soft

    def get_file_limit(self):
        """Get current process file descriptor limit with fallback"""
        try:
            import resource
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            # Try to increase to hard limit
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
                return hard
            except ValueError:
                return soft
        except Exception as e:
            logger.warning(f"Could not determine file descriptor limit: {e}")
            return 1024  # Conservative default

    def get_metrics_for_node(self, node_id):
        """Generate metrics for a specific node with 4 GPUs"""
        base_metrics = get_cached_metrics()
        # Split metrics into GPU groups (groups of 9 lines each - 1 metric per GPU)
        gpu_metrics = base_metrics.split('# HELP')[1:]  # Skip first empty split
        metrics_per_gpu = 9  # Number of metric types per GPU
        
        # Create metrics for all 4 GPUs for this node
        node_metrics = []
        for gpu_id in range(4):  # Always 4 GPUs per node
            for metric in gpu_metrics[:metrics_per_gpu]:  # Take one set of metrics
                # Replace node and GPU IDs
                modified_metric = metric.replace('node="0"', f'node="{node_id}"')
                modified_metric = modified_metric.replace('gpu="0"', f'gpu="{gpu_id}"')
                node_metrics.append(f"# HELP{modified_metric}")
                
        return "\n".join(node_metrics)

    def create_app_for_node(self, node_id):
        """Create a WSGI app for a specific node"""
        def app(environ, start_response):
            path = environ.get('PATH_INFO', '')
            if path == '/metrics':
                metrics = self.get_metrics_for_node(node_id)
                status = '200 OK'
                headers = [('Content-type', 'text/plain')]
                start_response(status, headers)
                return [metrics.encode('utf-8')]
            elif path == '/health':
                status = '200 OK'
                headers = [('Content-type', 'application/json')]
                start_response(status, headers)
                return [b'{"status": "healthy"}']
            else:
                status = '404 Not Found'
                headers = [('Content-type', 'text/plain')]
                start_response(status, headers)
                return [b'Not Found']
        return app

    def run_server(self, port: int, node_id: int):
        """Run server instance with optimized resource usage"""
        try:
            app = self.create_app_for_node(node_id)
            
            # Configure server with optimized settings
            server = make_server(
                self.host, 
                port, 
                app,
                handler_class=OptimizedWSGIRequestHandler
            )
            
            # Set socket options for reuse
            server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Set non-blocking mode
            server.socket.setblocking(False)
            
            logger.info(f"Server serving node {node_id} with 4 GPUs on http://{self.host}:{port}")
            
            while self.running:
                try:
                    server.handle_request()
                except socket.error as e:
                    if e.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                        raise
                    time.sleep(0.1)
                    
        except Exception as e:
            logger.error(f"Error running server for node {node_id} on port {port}: {e}")
        finally:
            logger.info(f"Shutting down server for node {node_id} on port {port}")

    def run_server(self, port: int, node_id: int):
        """Run server with improved resource management"""
        try:
            # Set socket options for better resource handling
            server = make_server(
                self.host, 
                port, 
                self.create_app_for_node(node_id),
                handler_class=OptimizedWSGIRequestHandler
            )
            
            server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            server.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            
            # Set non-blocking mode
            server.socket.setblocking(False)
            
            logger.info(f"Server for node {node_id} listening on port {port}")
            
            while self.running:
                try:
                    server.handle_request()
                except socket.error as e:
                    if e.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                        logger.error(f"Socket error for node {node_id}: {e}")
                        raise
                    time.sleep(0.1)
                except Exception as e:
                    logger.error(f"Error handling request for node {node_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Fatal error for node {node_id}: {e}")
        finally:
            try:
                server.server_close()
            except Exception:
                pass

    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, initiating force shutdown...")
        self.running = False
        self.force_shutdown()

    def force_shutdown(self):
        """Force shutdown all processes"""
        logger.info("Force shutting down all servers...")
        for p in self.processes:
            try:
                os.kill(p.pid, signal.SIGKILL)  # Force kill
            except ProcessLookupError:
                pass  # Process already dead
            except Exception as e:
                logger.error(f"Error killing process: {e}")
        
        # Clear process list
        self.processes.clear()
        
        # Exit immediately
        os._exit(0)

    def start_servers(self):
        """Start servers in pools to manage resources"""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        try:
            total_pools = (self.total_nodes + self.max_servers - 1) // self.max_servers
            logger.info(f"Starting {self.total_nodes} servers in {total_pools} pools")
            
            for pool in range(total_pools):
                start_idx = pool * self.max_servers
                end_idx = min((pool + self.max_servers), self.total_nodes)
                
                # Start servers in current pool
                pool_processes = []
                for node_id in range(start_idx, end_idx):
                    port = self.start_port + node_id
                    try:
                        p = multiprocessing.Process(
                            target=self.run_server,
                            args=(port, node_id),
                            name=f"server-node{node_id}"
                        )
                        p.start()
                        pool_processes.append(p)
                        time.sleep(0.1)  # Small delay between starts
                    except Exception as e:
                        logger.error(f"Failed to start server for node {node_id}: {e}")
                        raise
                
                self.processes.extend(pool_processes)
                self.server_pools.append(pool_processes)
                
                # Verify current pool
                time.sleep(1)
                running_servers = [p for p in pool_processes if p.is_alive()]
                if not running_servers:
                    raise RuntimeError(f"No servers successfully started in pool {pool}")
                
                logger.info(f"Successfully started pool {pool} with {len(running_servers)} servers")
            
            # Monitor all processes
            while self.running and any(p.is_alive() for p in self.processes):
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"Error in server management: {e}")
            self.force_shutdown()
            raise

# Add optimized request handler
class OptimizedWSGIRequestHandler(WSGIRequestHandler):
    def handle(self):
        """Handle a single HTTP request with optimized resource usage"""
        try:
            self.raw_requestline = self.rfile.readline(65537)
            if len(self.raw_requestline) > 65536:
                self.requestline = ''
                self.request_version = ''
                self.command = ''
                self.send_error(414)
                return
            
            if not self.parse_request():
                return

            # Use SimpleHandler instead of ServerHandler
            handler = SimpleHandler(
                self.rfile, self.wfile, self.get_stderr(), self.get_environ()
            )
            handler.request_handler = self
            handler.run(self.server.get_app())
        except socket.timeout as e:
            self.log_error("Request timed out: %r", e)
            self.close_connection = True
        except Exception as e:
            self.log_error("Error handling request: %r", e)
            self.close_connection = True

def get_optimal_workers(total_nodes: int) -> int:
    """Calculate optimal number of worker processes"""
    cpu_cores = multiprocessing.cpu_count()
    workers_per_node = max(2, cpu_cores // total_nodes)
    return workers_per_node

def monitor_resources():
    """Monitor system resources using time.sleep"""
    while True:
        cpu_percent = psutil.cpu_percent(interval=1)
        mem_percent = psutil.virtual_memory().percent
        logger.info(f"CPU Usage: {cpu_percent}%, Memory Usage: {mem_percent}%")
        if cpu_percent > 90 or mem_percent > 90:
            logger.warning("High resource usage detected!")
        time.sleep(60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start the metrics server.")
    parser.add_argument("--num_ports", type=int, required=True, help="Number of ports to use")
    parser.add_argument("--start_port", type=int, required=True, help="Starting port number")
    parser.add_argument("--total_nodes", type=int, required=True, help="Total number of nodes in the cluster")
    args = parser.parse_args()

    try:
        logger.info(f"Starting server with {args.total_nodes} nodes on ports {args.start_port} to {args.start_port + args.total_nodes - 1}")
        
        server = MetricsServer(
            start_port=args.start_port,
            num_ports=args.total_nodes,  # We use total_nodes as number of ports
            total_nodes=args.total_nodes
        )
        server.start_servers()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, force shutting down...")
        if 'server' in locals():
            server.force_shutdown()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if 'server' in locals():
            server.force_shutdown()
        sys.exit(1)