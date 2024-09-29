import os
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
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
from contextlib import asynccontextmanager
import sys
from ble_manager import BLEManager
from queue import Queue
from data_processor import DataProcessor
import urllib3
import warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

# Remove all existing handlers
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Create a stream handler for stdout
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stdout_handler.setFormatter(formatter)

# Add the stdout handler to the root logger
logging.getLogger().addHandler(stdout_handler)

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


# This started life intended to be a webapp (hence the use of FastAPI and lifecycle events) but that has not yet been implemented...
@app.on_event("startup")
async def startup_event():
    global startup_complete
    if not startup_complete:
        logger.info("Starting BLE manager")
        asyncio.create_task(ble_manager.run())
        viatom_device_address = os.getenv('VIATOM_DEVICE_ADDRESS')
        core_device_address = os.getenv('CORE_DEVICE_ADDRESS')
        polar_device_address = os.getenv('POLAR_DEVICE_ADDRESS')
        await ble_manager.queue_connect_to_specific_device(core_device_address, event_id="startup", reason="Startup")
        await ble_manager.queue_connect_to_specific_device(polar_device_address, event_id="startup", reason="Startup")
        await ble_manager.queue_connect_to_specific_device(viatom_device_address, event_id="startup", reason="Startup")
        startup_complete = True


# Start data processing in a separate thread
import threading
processing_thread = threading.Thread(target=data_processor.process_data, daemon=True)
processing_thread.start()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)