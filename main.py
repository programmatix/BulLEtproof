import os
from fastapi import FastAPI, WebSocket, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from ble_command import SharedData
from viatom_device import ViatomClientManager
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
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)-20s - %(levelname)-8s - %(message)s'))

# Add memory log handler
memory_handler = MemoryLogHandler()
memory_handler.setLevel(logging.DEBUG)
memory_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)-20s - %(levelname)-8s - %(message)s'))

# Configure the root logger
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)-20s - %(levelname)-8s - %(message)s',
    handlers=[stdout_handler, memory_handler]
)

# Get the logger for this module
logger = logging.getLogger(__name__)

app = FastAPI()

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
            </style>
        </head>
        <body>
            <h1>BLE Device Monitor</h1>
            <button onclick="updateStatus()">Refresh Status</button>
            <div id="devices"></div>
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
                        document.getElementById(`logs-${device}`).textContent = data.logs.join('\\n');
                    } catch (error) {
                        console.error('Error fetching logs:', error);
                    }
                }
                
                // Initial load
                updateStatus();
            </script>
        </body>
    </html>
    """

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)