import os
from fastapi import FastAPI, WebSocket, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from ble_command import SharedData
from viatom_device import ViatomClientManager, ViatomData
from polar_device import PolarClientManager
from core_device import CoreClientManager
from influx_manager import InfluxManager
from mqtt_manager import MQTTManager
import asyncio
import json
from dotenv import load_dotenv
import logging
from logging.handlers import TimedRotatingFileHandler
from contextlib import asynccontextmanager
import sys
from ble_manager import BLEManager
from queue import Queue
from data_processor import DataProcessor
import urllib3
import warnings
import time
from typing import Dict, List
import cProfile
import pstats
import io
from fastapi.responses import JSONResponse
import psutil
import tracemalloc
import random

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

# In-memory log buffer
LOG_BUFFER: Dict[str, List[str]] = {
    'viatom': [],
    'polar': [],
    'core': [],
    'movesense': [],
    'system': []
}

class MemoryLogHandler(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        for device_type in LOG_BUFFER.keys():
            if device_type in record.name.lower():
                LOG_BUFFER[device_type].append(log_entry)
                if len(LOG_BUFFER[device_type]) > 50:
                    LOG_BUFFER[device_type].pop(0)
                break
        else:
            LOG_BUFFER['system'].append(log_entry)
            if len(LOG_BUFFER['system']) > 50:
                LOG_BUFFER['system'].pop(0)

# Create a stream handler for stdout
class IgnoreBrokenPipeHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            super().emit(record)
        except BrokenPipeError:
            pass

    def flush(self):
        # Only flush if we have something to flush
        if self.stream and hasattr(self.stream, 'flush') and not getattr(self.stream, 'closed', False):
            try:
                super().flush()
            except BrokenPipeError:
                pass

class ColoredConsoleFormatter(logging.Formatter):
    def format(self, record):
        if record.levelno == logging.WARNING:
            return f'\033[93m{super().format(record)}\033[0m'
        elif record.levelno == logging.ERROR:
            return f'\033[91m{super().format(record)}\033[0m'
        return super().format(record)

class WebLogFormatter(logging.Formatter):
    def format(self, record):
        if record.levelno == logging.WARNING:
            return f'<span style="color: yellow;">{super().format(record)}</span>'
        elif record.levelno == logging.ERROR:
            return f'<span style="color: red;">{super().format(record)}</span>'
        return super().format(record)

stdout_handler = IgnoreBrokenPipeHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(ColoredConsoleFormatter('%(asctime)s - %(name)-20s - %(levelname)-8s - %(message)s'))

# Add memory log handler
memory_handler = MemoryLogHandler()
memory_handler.setLevel(logging.DEBUG)
memory_handler.setFormatter(WebLogFormatter('%(asctime)s - %(name)-20s - %(levelname)-8s - %(message)s'))

# Configure the root logger
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)-20s - %(levelname)-8s - %(message)s',
    handlers=[stdout_handler, memory_handler]
)

# Function to set log level
def set_log_level(level):
    logging.getLogger().setLevel(level)
    stdout_handler.setLevel(level)
    memory_handler.setLevel(level)

# Get the logger for this module
logger = logging.getLogger(__name__)

app = FastAPI()

# Configure logging for FastAPI
app.logger = logging.getLogger("uvicorn.access")
app.logger.setLevel(logging.DEBUG)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

data_queue = Queue[SharedData]()
ble_manager = BLEManager(data_queue)
influx_manager = InfluxManager()
mqtt_manager = MQTTManager()

# Initialize DataProcessor
data_processor = DataProcessor(data_queue, influx_manager, mqtt_manager, ble_manager)

# Start data processing
data_processor.start()

# Flag to track if startup has been performed
startup_complete = False

# Flags to control component activity
component_status = {
    "ble_manager_active": True,
    "data_processor_active": True,
    "influx_manager_active": True,
    "mqtt_manager_active": True
}

@app.on_event("startup")
async def startup_event():
    global startup_complete
    if not startup_complete:
        logger.info("Starting BLE manager")
        asyncio.create_task(ble_manager.run())
        viatom_device_address = os.getenv('VIATOM_DEVICE_ADDRESS')
        core_device_address = os.getenv('CORE_DEVICE_ADDRESS')
        polar_device_address = os.getenv('POLAR_DEVICE_ADDRESS')
        movesense_device_address = os.getenv('MOVESENSE_DEVICE_ADDRESS')
        await ble_manager.queue_connect_to_specific_device(core_device_address, event_id="startup", reason="Startup")
        # await ble_manager.queue_connect_to_specific_device(polar_device_address, event_id="startup", reason="Startup")
        await ble_manager.queue_connect_to_specific_device(viatom_device_address, event_id="startup", reason="Startup")
        await ble_manager.queue_connect_to_specific_device(movesense_device_address, event_id="startup", reason="Startup")
        startup_complete = True

# Start data processing in a separate thread
import threading
processing_thread = threading.Thread(target=data_processor.process_data, daemon=True)
processing_thread.start()

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Disconnecting all BLE devices")
    await ble_manager.disconnect_all_devices()

@app.get("/status")
async def get_status():
    viatom_client_manager = ble_manager.get_client_manager(os.getenv('VIATOM_DEVICE_ADDRESS'))
    core_client_manager = ble_manager.get_client_manager(os.getenv('CORE_DEVICE_ADDRESS'))
    movesense_client_manager = ble_manager.get_client_manager(os.getenv('MOVESENSE_DEVICE_ADDRESS'))
    return {
        "movesense": movesense_client_manager.status if movesense_client_manager else "N/A",
        "core": core_client_manager.status if core_client_manager else "N/A",
        "viatom": viatom_client_manager.status if viatom_client_manager else "N/A",
        "system": "OK"
    }

@app.get("/logs/{device_type}")
async def get_logs(device_type: str):
    if device_type not in LOG_BUFFER:
        raise HTTPException(status_code=404, detail="Device type not found")
    logs = LOG_BUFFER[device_type].copy() if LOG_BUFFER[device_type] else []
    if logs:
        logs.reverse()
        return {"logs": logs[0:50]}
    return {"logs": ["No logs available"]}

@app.get("/", response_class=HTMLResponse)
async def get_ui():
    return """
    <html>
        <head>
            <title>BLE Device Monitor</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .device { margin-bottom: 20px; padding: 10px; border: 1px solid #ddd; border-radius: 5px; }
                .connected { background-color: #d4edda; }
                .disconnected { background-color: #f8d7da; }
                .logs { font-family: monospace; white-space: pre; background: #f8f9fa; padding: 10px; border-radius: 5px; }
                button { margin: 5px; padding: 8px 12px; cursor: pointer; }
                .section { margin-bottom: 20px; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }
                .control-panel { display: flex; flex-wrap: wrap; }
                .control-item { margin: 10px; }
                pre { white-space: pre-wrap; }
            </style>
        </head>
        <body>
            <h1>BLE Device Monitor</h1>
            
            <div class="section">
                <h2>Device Status</h2>
                <button onclick="updateStatus()">Refresh Status</button>
                <div id="devices"></div>
            </div>
            
            <div class="section">
                <h2>Component Control</h2>
                <div class="control-panel">
                    <div class="control-item">
                        <h3>BLE Manager</h3>
                        <button onclick="setComponentStatus('ble_manager_active', 'enable')">Enable</button>
                        <button onclick="setComponentStatus('ble_manager_active', 'disable')">Disable</button>
                    </div>
                    <div class="control-item">
                        <h3>Data Processor</h3>
                        <button onclick="setComponentStatus('data_processor_active', 'enable')">Enable</button>
                        <button onclick="setComponentStatus('data_processor_active', 'disable')">Disable</button>
                    </div>
                    <div class="control-item">
                        <h3>InfluxDB Manager</h3>
                        <button onclick="setComponentStatus('influx_manager_active', 'enable')">Enable</button>
                        <button onclick="setComponentStatus('influx_manager_active', 'disable')">Disable</button>
                    </div>
                    <div class="control-item">
                        <h3>MQTT Manager</h3>
                        <button onclick="setComponentStatus('mqtt_manager_active', 'enable')">Enable</button>
                        <button onclick="setComponentStatus('mqtt_manager_active', 'disable')">Disable</button>
                    </div>
                </div>
                <div id="component-status"></div>
            </div>
            
            <div class="section">
                <h2>Profiling Tools</h2>
                <div class="control-panel">
                    <div class="control-item">
                        <h3>General Profiling</h3>
                        <button onclick="runProfile()">Run 5s Profile</button>
                        <button onclick="runDetailedProfile()">Run 10s Detailed Profile</button>
                    </div>
                    <div class="control-item">
                        <h3>Data Processing</h3>
                        <button onclick="profileDataProcessing()">Profile Data Processing</button>
                    </div>
                    <div class="control-item">
                        <h3>Memory Usage</h3>
                        <button onclick="checkMemoryUsage()">Check Memory Usage</button>
                    </div>
                    <div class="control-item">
                        <h3>Tracemalloc</h3>
                        <button onclick="startTracemalloc()">Start Tracemalloc</button>
                        <button onclick="getTracemallocSnapshot()">Get Snapshot</button>
                    </div>
                </div>
                <div id="profile-results"></div>
            </div>
            
            <div class="section">
                <h2>Queue Status</h2>
                <button onclick="getQueueStatus()">Check Queue Status</button>
                <div id="queue-status"></div>
            </div>
            
            <script>
                async function updateStatus() {
                    try {
                        const response = await fetch('/status');
                        const status = await response.json();
                        
                        let html = '';
                        for (const [device, state] of Object.entries(status)) {
                            html += `
                            <div class="device ${state === 'OK' || state.includes('Connected') ? 'connected' : 'disconnected'}">
                                <h2>${device} - ${state}</h2>
                                <button onclick="fetchLogs('${device}')">Show Logs</button>
                                <div id="logs-${device}" class="logs"></div>
                            </div>
                            `;
                        }
                        document.getElementById('devices').innerHTML = html;
                    } catch (error) {
                        console.error('Error fetching status:', error);
                    }
                }
                
                async function fetchLogs(device) {
                    try {
                        const response = await fetch(`/logs/${device}`);
                        const data = await response.json();
                        document.getElementById(`logs-${device}`).innerHTML = data.logs.join('<br>');
                    } catch (error) {
                        console.error('Error fetching logs:', error);
                    }
                }
                
                async function setComponentStatus(component, status) {
                    try {
                        const response = await fetch(`/component/${component}/${status}`);
                        const data = await response.json();
                        alert(data.message);
                        getComponentStatus();
                    } catch (error) {
                        console.error('Error setting component status:', error);
                    }
                }
                
                async function getComponentStatus() {
                    try {
                        const response = await fetch('/components/status');
                        const data = await response.json();
                        document.getElementById('component-status').innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
                    } catch (error) {
                        console.error('Error getting component status:', error);
                    }
                }
                
                async function runProfile() {
                    try {
                        document.getElementById('profile-results').innerHTML = 'Running profile for 5 seconds...';
                        const response = await fetch('/profile');
                        const data = await response.json();
                        document.getElementById('profile-results').innerHTML = '<pre>' + data.profile_results + '</pre>';
                    } catch (error) {
                        console.error('Error running profile:', error);
                    }
                }
                
                async function runDetailedProfile() {
                    try {
                        document.getElementById('profile-results').innerHTML = 'Running detailed profile for 10 seconds...';
                        const response = await fetch('/profile/detailed');
                        const data = await response.json();
                        document.getElementById('profile-results').innerHTML = '<pre>' + data.profile_results + '</pre>';
                    } catch (error) {
                        console.error('Error running detailed profile:', error);
                    }
                }
                
                async function profileDataProcessing() {
                    try {
                        document.getElementById('profile-results').innerHTML = 'Profiling data processing...';
                        const response = await fetch('/profile/data_processing');
                        const data = await response.json();
                        document.getElementById('profile-results').innerHTML = '<pre>' + data.profile_results + '</pre>';
                    } catch (error) {
                        console.error('Error profiling data processing:', error);
                    }
                }
                
                async function checkMemoryUsage() {
                    try {
                        const response = await fetch('/memory');
                        const data = await response.json();
                        document.getElementById('profile-results').innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
                    } catch (error) {
                        console.error('Error checking memory usage:', error);
                    }
                }
                
                async function startTracemalloc() {
                    try {
                        const response = await fetch('/tracemalloc/start');
                        const data = await response.json();
                        alert(data.status);
                    } catch (error) {
                        console.error('Error starting tracemalloc:', error);
                    }
                }
                
                async function getTracemallocSnapshot() {
                    try {
                        const response = await fetch('/tracemalloc/snapshot');
                        const data = await response.json();
                        document.getElementById('profile-results').innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
                    } catch (error) {
                        console.error('Error getting tracemalloc snapshot:', error);
                    }
                }
                
                async function getQueueStatus() {
                    try {
                        const response = await fetch('/queue_status');
                        const data = await response.json();
                        document.getElementById('queue-status').innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
                    } catch (error) {
                        console.error('Error getting queue status:', error);
                    }
                }
                
                // Initial load
                updateStatus();
                getComponentStatus();
            </script>
        </body>
    </html>
    """

@app.get("/profile")
async def profile_app():
    pr = cProfile.Profile()
    pr.enable()
    
    # Run for 5 seconds
    start_time = time.time()
    while time.time() - start_time < 5:
        await asyncio.sleep(0.1)
    
    pr.disable()
    
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats(30)  # Print top 30 functions
    
    return JSONResponse(content={"profile_results": s.getvalue()})

@app.get("/profile/detailed")
async def profile_detailed():
    pr = cProfile.Profile()
    pr.enable()
    
    # Run for 10 seconds
    start_time = time.time()
    while time.time() - start_time < 10:
        await asyncio.sleep(0.1)
    
    pr.disable()
    
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats(50)  # Print top 50 functions
    
    # Get more detailed view
    s.write("\n\nDetailed by internal time:\n")
    ps.sort_stats('time').print_stats(50)
    
    return JSONResponse(content={"profile_results": s.getvalue()})

@app.get("/memory")
async def memory_usage():
    process = psutil.Process()
    memory_info = process.memory_info()
    
    return {
        "rss": f"{memory_info.rss / (1024 * 1024):.2f} MB",
        "vms": f"{memory_info.vms / (1024 * 1024):.2f} MB",
        "cpu_percent": process.cpu_percent(),
        "threads": len(process.threads()),
        "open_files": len(process.open_files()),
        "connections": len(process.connections())
    }

@app.get("/tracemalloc/start")
async def start_tracemalloc():
    if not tracemalloc.is_tracing():
        tracemalloc.start()
        return {"status": "Tracemalloc started"}
    return {"status": "Tracemalloc already running"}

@app.get("/tracemalloc/snapshot")
async def get_tracemalloc_snapshot():
    if not tracemalloc.is_tracing():
        return {"error": "Tracemalloc not started"}
    
    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics('lineno')
    
    result = []
    for stat in top_stats[:30]:  # Get top 30 memory allocations
        result.append({
            "file": str(stat.traceback.frame.filename),
            "line": stat.traceback.frame.lineno,
            "size": f"{stat.size / 1024:.1f} KB",
            "count": stat.count
        })
    
    return {"memory_snapshot": result}

@app.get("/queue_status")
async def queue_status():
    return {
        "queue_size": data_queue.qsize(),
        "processing_thread_alive": processing_thread.is_alive(),
        "ble_manager_status": ble_manager.get_status(),
        "data_processor_status": data_processor.get_status()
    }

@app.get("/profile/data_processing")
async def profile_data_processing():
    pr = cProfile.Profile()
    pr.enable()
    
    # Create some test data and put it in the queue
    from ble_command import SharedData
    from viatom_device import ViatomData
    import time
    import random
    
    # Add 100 test data points
    for i in range(100):
        test_data = ViatomData(
            device_address="test_device",
            hr=random.randint(60, 100),
            spo2=random.randint(95, 100),
            perfusion_index=random.randint(1, 10),
            battery=random.randint(0, 100),
            movement=random.randint(0, 100),
            timestamp=int(time.time() * 1000)
        )
        data_queue.put(test_data)
    
    # Wait for processing
    await asyncio.sleep(3)
    
    pr.disable()
    
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats(30)
    
    return JSONResponse(content={"profile_results": s.getvalue()})

@app.get("/component/{component_name}/{status}")
async def set_component_status(component_name: str, status: str):
    if component_name not in component_status:
        raise HTTPException(status_code=404, detail=f"Component {component_name} not found")
    
    if status.lower() == "enable":
        component_status[component_name] = True
        return {"message": f"{component_name} enabled"}
    elif status.lower() == "disable":
        component_status[component_name] = False
        return {"message": f"{component_name} disabled"}
    else:
        raise HTTPException(status_code=400, detail="Status must be 'enable' or 'disable'")

@app.get("/components/status")
async def get_component_status():
    return component_status

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)