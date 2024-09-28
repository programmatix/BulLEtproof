import os
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from viatom_device import ViatomDevice
from polar_device import PolarDevice
from core_device import CoreDevice
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

data_queue = Queue()
ble_manager = BLEManager(data_queue)
influx_manager = InfluxManager()
mqtt_manager = MQTTManager()

# Initialize DataProcessor
data_processor = DataProcessor(data_queue, influx_manager, mqtt_manager)

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
        await ble_manager.queue_connect_to_specific_device(viatom_device_address)
        startup_complete = True

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down")
    influx_manager.close()
    mqtt_manager.close()

@app.post("/connect/{address}")
async def connect_device(address: str):
    def connect_and_subscribe():
        success = yield from ble_manager.connect_device(address)
        if success:
            client = ble_manager.clients[address]
            if "Checkme" in client.name:
                device = ViatomDevice(client)
            elif "Polar" in client.name:
                device = PolarDevice(client)
            elif "CORE" in client.name:
                device = CoreDevice(client)
            else:
                logger.warning(f"Unknown device type: {client.name}")
                return

            yield from device.subscribe()

    ble_manager.add_task(connect_and_subscribe())
    return {"success": True}

@app.post("/disconnect/{address}")
async def disconnect_device(address: str):
    ble_manager.add_task(ble_manager.queue_disconnect_device(address))
    return {"success": True}

@app.websocket("/ws/{address}")
async def websocket_endpoint(websocket: WebSocket, address: str):
    await websocket.accept()

    def notification_handler(sender, data):
        processed_data = process_data(sender, data)
        asyncio.create_task(websocket.send_text(json.dumps(processed_data.__dict__)))
        write_to_influx(sender, processed_data)
        publish_to_mqtt(sender, processed_data)

    client = ble_manager.clients[address]
    client.set_disconnected_callback(notification_handler)

    try:
        while True:
            await websocket.receive_text()
    except:
        logger.info(f"WebSocket connection closed for device {address}")

def process_data(sender, data):
    if isinstance(sender, ViatomDevice):
        return sender.data_handler(sender.client, data)
    elif isinstance(sender, PolarDevice):
        if len(data) == 2:
            return sender.hr_handler(sender.client, data)
        else:
            return sender.rr_handler(sender.client, data)
    elif isinstance(sender, CoreDevice):
        return sender.temp_handler(sender.client, data)

def write_to_influx(sender, data):
    measurement = type(sender).__name__.lower().replace('device', '')
    influx_manager.write_data(measurement, data.__dict__, tags={"device": sender.client.address})

def publish_to_mqtt(sender, data):
    topic = f"sensors/{type(sender).__name__.lower().replace('device', '')}/{sender.client.address}"
    mqtt_manager.publish_data(topic, data.__dict__)

# Start data processing in a separate thread
import threading
processing_thread = threading.Thread(target=data_processor.process_data, daemon=True)
processing_thread.start()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)