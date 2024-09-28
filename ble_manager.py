import asyncio
from bleak import BleakScanner, BleakClient
from bleak.exc import BleakError
import logging
from constants import RECONNECT_INTERVAL
from async_timeout import timeout as async_timeout

from core_device import CoreDevice
from polar_device import PolarDevice
from viatom_device import ViatomDevice

class BLEManager:
    def __init__(self, data_queue):
        self.devices = {}
        self.clients = {}
        self.queue = asyncio.Queue()
        self.logger = logging.getLogger(__name__)
        self.auto_connect_devices = set()
        self.last_data_received = {}
        self.core_device_address = "DB:7B:FB:C4:A9:94"  # Hardcoded CORE device address
        self.data_queue = data_queue

    async def run(self):
        while True:
            task = await self.queue.get()
            try:
                await task
            except Exception as e:
                self.logger.error(f"Error executing BLE task: {e}", exc_info=True)
            finally:
                self.queue.task_done()

    async def connect_device(self, address, max_retries=3, connection_timeout=30):
        device_name = self.get_device_name(address)
        self.logger.info(f"Attempting to connect to device {device_name}")

        for attempt in range(max_retries):
            try:
                async with async_timeout(connection_timeout):
                    client = BleakClient(address)
                    await client.connect()
                    self.clients[address] = client
                    self.logger.info(f"Successfully connected to device {device_name} on attempt {attempt + 1}")
                    await self.handle_post_connection(client, address)
                    return True
            except asyncio.TimeoutError:
                self.logger.warning(f"Connection attempt {attempt + 1} to {device_name} timed out after {connection_timeout} seconds")
            except BleakError as e:
                self.logger.error(f"BleakError on attempt {attempt + 1} connecting to device {device_name}: {e}", exc_info=True)
            except Exception as e:
                self.logger.error(f"Unexpected error on attempt {attempt + 1} connecting to device {device_name}: {e}", exc_info=True)

            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5  # Increasing wait time between retries
                self.logger.info(f"Waiting {wait_time} seconds before next connection attempt to {device_name}")
                await asyncio.sleep(wait_time)

        self.logger.error(f"Failed to connect to device {device_name} after {max_retries} attempts")
        return False

    async def handle_post_connection(self, client: BleakClient, address: str):
        asyncio.create_task(self.monitor_connection(address))
        device = CoreDevice(client, self.data_queue)
        await device.subscribe()

    async def scan_devices(self):
        self.logger.info("Scanning for BLE devices")
        devices = await BleakScanner.discover()
        for device in devices:
            self.logger.info(f"Found device: {device.name} ({device.address})")
            self.devices[device.address] = device
        self.logger.info(f"Found {len(self.devices)} devices")

    def get_device_name(self, address):
        device = self.devices.get(address)
        return f"{device.name} ({address})" if device else address

    async def disconnect_device(self, address):
        self.logger.info(f"Disconnecting from device {address}")
        if address in self.clients:
            await self.clients[address].disconnect()
            del self.clients[address]
            self.logger.info(f"Disconnected from device {address}")

    def add_task(self, coro):
        self.queue.put_nowait(coro)

    async def monitor_connection(self, address):
        while True:
            await asyncio.sleep(RECONNECT_INTERVAL)
            if address not in self.clients or not self.clients[address].is_connected:
                self.logger.info(f"Device {address} disconnected, attempting to reconnect")
                await self.connect_device(address)
            elif asyncio.get_event_loop().time() - self.last_data_received[address] > RECONNECT_INTERVAL:
                self.logger.info(f"No data received from {address} for {RECONNECT_INTERVAL} seconds, reconnecting")
                await self.disconnect_device(address)
                await self.connect_device(address)

    def update_last_data_received(self, address):
        self.last_data_received[address] = asyncio.get_event_loop().time()

    async def connect_to_core(self):
        self.logger.info(f"Attempting to connect to CORE device at {self.core_device_address}")
        success = await self.connect_device(self.core_device_address)
        if success:
            self.logger.info(f"Successfully connected to CORE device at {self.core_device_address}")
        else:
            self.logger.error(f"Failed to connect to CORE device at {self.core_device_address}")