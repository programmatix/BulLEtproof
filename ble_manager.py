import asyncio
from bleak import BleakScanner, BleakClient
from bleak.exc import BleakError
import logging
from constants import RECONNECT_INTERVAL
from async_timeout import timeout as async_timeout
import os
from dotenv import load_dotenv

from core_device import CoreDevice
from polar_device import PolarDevice
from viatom_device import ViatomDevice
from ble_command import BLECommand

load_dotenv()

class ScanCommand(BLECommand):
    async def execute(self, manager):
        manager.logger.info("Scanning for BLE devices")
        devices = await BleakScanner.discover()
        for device in devices:
            manager.logger.info(f"Found device: {device.name or 'Unknown'} ({device.address})")
            manager.devices[device.address] = {
                'name': device.name,
                'details': device
            }
        manager.logger.info(f"Found {len(manager.devices)} devices")

class ConnectCommand(BLECommand):
    def __init__(self, address, attempt=0):
        self.address = address
        self.attempt = attempt

    async def execute(self, manager):
        device_name = manager.get_device_name(self.address)
        manager.logger.info(f"Attempting to connect to device {device_name} (attempt {self.attempt + 1})")

        try:
            async with async_timeout(manager.connection_timeout):
                client = BleakClient(self.address)
                await client.connect()
                manager.clients[self.address] = client
                manager.logger.info(f"Successfully connected to device {device_name} on attempt {self.attempt + 1}")
                await manager.handle_post_connection(client, self.address)
                return True
        except asyncio.TimeoutError:
            manager.logger.warning(f"Connection attempt {self.attempt + 1} to {device_name} timed out after {manager.connection_timeout} seconds")
        except BleakError as e:
            manager.logger.error(f"BleakError on attempt {self.attempt + 1} connecting to device {device_name}: {e}", exc_info=True)
        except Exception as e:
            manager.logger.error(f"Unexpected error on attempt {self.attempt + 1} connecting to device {device_name}: {e}", exc_info=True)

        if self.attempt < manager.max_retries - 1:
            next_attempt = self.attempt + 1
            wait_time = 2 ** next_attempt
            manager.logger.info(f"Scheduling next connection attempt to {device_name} in {wait_time} seconds")
            await manager.schedule_command(ConnectCommand(self.address, next_attempt), wait_time)
        else:
            manager.logger.error(f"Failed to connect to device {device_name} after {manager.max_retries} attempts")

        return False

class DisconnectCommand(BLECommand):
    def __init__(self, address):
        self.address = address

    async def execute(self, manager):
        manager.logger.info(f"Disconnecting from device {self.address}")
        if self.address in manager.clients:
            await manager.clients[self.address].disconnect()
            del manager.clients[self.address]
            manager.logger.info(f"Disconnected from device {self.address}")

class BLEManager:
    def __init__(self, data_queue):
        self.devices = {}
        self.clients = {}
        self.logger = logging.getLogger(__name__)
        self.auto_connect_devices = set()
        self.last_data_received = {}
        self.data_queue = data_queue
        self.command_queue = asyncio.Queue()
        self.max_retries = 3
        self.connection_timeout = 30
        self.scheduled_tasks = []

    async def run(self):
        while True:
            # Process any due scheduled tasks
            current_time = asyncio.get_event_loop().time()
            due_tasks = [task for task in self.scheduled_tasks if task[0] <= current_time]
            for task in due_tasks:
                self.scheduled_tasks.remove(task)
                await self.command_queue.put(task[1])

            # Process commands from the queue
            try:
                command = await asyncio.wait_for(self.command_queue.get(), timeout=1.0)
                await command.execute(self)
                self.command_queue.task_done()
            except asyncio.TimeoutError:
                # No commands in the queue, continue to next iteration
                await asyncio.sleep(0.1)

    # async def queue_scan_and_connect_devices(self):
    #     await self.command_queue.put(ScanCommand())
    #     for address in self.auto_connect_devices:
    #         await self.command_queue.put(ConnectCommand(address))

    def get_device_name(self, address):
        device_info = self.devices.get(address, {})
        name = device_info.get('name')
        if name:
            return f"{name} ({address})"
        return address

    async def handle_post_connection(self, client: BleakClient, address: str):
        self.logger.info(f"Handling post connection for device {address}")
        device_name = self.get_device_name(address)
        
        if "Checkme" in device_name:
            device = ViatomDevice(client, self.data_queue, self)
        elif "Polar" in device_name:
            device = PolarDevice(client, self.data_queue)
        elif "CORE" in device_name:
            device = CoreDevice(client, self.data_queue)
        else:
            self.logger.warning(f"Unknown device type: {device_name}. Attempting to read device name...")
            try:
                services = await client.get_services()
                for service in services:
                    for char in service.characteristics:
                        if "2a00" in char.uuid.lower():  # Device Name characteristic
                            name = await client.read_gatt_char(char.uuid)
                            name = name.decode('utf-8')
                            self.devices[address]['name'] = name
                            device_name = self.get_device_name(address)
                            self.logger.info(f"Updated device name: {device_name}")
                            break
                    if "2a00" in char.uuid.lower():
                        break
            except Exception as e:
                self.logger.error(f"Failed to read device name: {e}")
            
            # Determine device type based on updated name or use CoreDevice as fallback
            if "Checkme" in device_name:
                device = ViatomDevice(client, self.data_queue, self)
            elif "Polar" in device_name:
                device = PolarDevice(client, self.data_queue)
            elif "CORE" in device_name:
                device = CoreDevice(client, self.data_queue)
            else:
                if address == os.getenv('CORE_DEVICE_ADDRESS'): 
                    device = CoreDevice(client, self.data_queue)
                elif address == os.getenv('VIATOM_DEVICE_ADDRESS'):
                    device = ViatomDevice(client, self.data_queue, self)
                elif address == os.getenv('POLAR_DEVICE_ADDRESS'):
                    device = PolarDevice(client, self.data_queue)
                else:
                    self.logger.warning(f"Still unknown device type: {device_name}")
                    return
        
        try:
            await device.subscribe()
            self.logger.info(f"Successfully subscribed to device {device_name}")
        except Exception as e:
            self.logger.error(f"Failed to subscribe to device {device_name}: {e}", exc_info=True)
            await self.queue_disconnect_device(address)

    def update_last_data_received(self, address):
        self.last_data_received[address] = asyncio.get_event_loop().time()

    async def queue_connect_to_specific_device(self, address):
        self.logger.info(f"Attempting to connect to device at {address}")
        await self.command_queue.put(ConnectCommand(address))

    async def queue_disconnect_device(self, address):
        await self.command_queue.put(DisconnectCommand(address))

    async def schedule_command(self, command, delay):
        execution_time = asyncio.get_event_loop().time() + delay
        self.scheduled_tasks.append((execution_time, command))
        self.scheduled_tasks.sort(key=lambda x: x[0])