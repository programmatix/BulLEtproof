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
from logging.handlers import TimedRotatingFileHandler
from contextlib import asynccontextmanager
import sys
from ble_manager import BLEManager
from queue import Queue
from data_processor import DataProcessor
import urllib3
import warnings
import time
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

# Remove all existing handlers
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Create a TimedRotatingFileHandler for daily rollover
log_filename = 'logs/app.log'
file_handler = TimedRotatingFileHandler(
    filename=log_filename,
    when='midnight',
    interval=1,
    backupCount=30,  # Keep logs for 30 days
    encoding='utf-8'
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# Set the suffix of the log file
file_handler.suffix = "%Y-%m-%d"

# Set the extMatch with a custom function to include the date in the rotated file names
def namer(default_name):
    base_filename, ext, date = default_name.split(".")
    return f"{base_filename}.{date}.{ext}"

file_handler.namer = namer

# Create a stream handler for stdout
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.INFO)
stdout_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# Add both handlers to the root logger
logging.getLogger().addHandler(file_handler)
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