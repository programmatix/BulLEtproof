import asyncio
import time
from bleak import BleakScanner, BleakClient
from bleak.exc import BleakError
import logging
from constants import RECONNECT_INTERVAL
from async_timeout import timeout as async_timeout
import os
from dotenv import load_dotenv
import threading
import uuid

from core_device import CoreClientManager
from polar_device import PolarClientManager
from viatom_device import ViatomClientManager
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

    def __str__(self):
        return "ScanCommand"

class ConnectCommand(BLECommand):
    def __init__(self, address, event_id: str, reason: str = None, attempt=0):
        self.address = address
        self.attempt = attempt
        self.event_id = event_id
        self.reason = reason
        self.logger = logging.getLogger(__name__ + "." + event_id)

    def __str__(self):
        return f"ConnectCommand[address={self.address}, attempt={self.attempt}, event_id={self.event_id}, reason={self.reason}]"

    async def execute(self, manager):
        device_name = manager.get_device_name(self.address)
        self.logger.info(f"Attempting to connect to device {device_name} (attempt {self.attempt + 1}) because {self.reason}")

        try:
            async with async_timeout(manager.connection_timeout):
                client = BleakClient(self.address)
                await client.connect()
                self.logger.info(f"Successfully connected to device {device_name} on attempt {self.attempt + 1} for reason {self.reason}")
                await manager.handle_post_connection(client, self.address)
                return True
        except asyncio.TimeoutError:
            self.logger.warning(f"Connection attempt {self.attempt + 1} to {device_name} timed out after {manager.connection_timeout} seconds")
        except BleakError as e:
            # Probably just cannot connect
            self.logger.info(f"BleakError on attempt {self.attempt + 1} connecting to device {device_name}: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error on attempt {self.attempt + 1} connecting to device {device_name}: {e}", exc_info=True)

        # Schedule next connection attempt
        next_attempt = self.attempt + 1
        wait_time = min(60, 2 ** next_attempt)
        self.logger.info(f"Scheduling next connection attempt to {device_name} in {wait_time} seconds")
        await manager.schedule_command(ConnectCommand(self.address, self.event_id, self.reason, next_attempt), wait_time)

        return False

class DisconnectCommand(BLECommand):
    def __init__(self, address, event_id: str, reason: str = None):
        self.address = address
        self.event_id = event_id
        self.reason = reason
        self.logger = logging.getLogger(__name__ + "." + event_id)

    def __str__(self):
        return f"DisconnectCommand[address={self.address}, event_id={self.event_id}, reason={self.reason}]"

    async def execute(self, manager):
        self.logger.info(f"Disconnecting from device {self.address} because {self.reason}")
        await manager.cleanup_client_manager(self.address)

class BLEManager:
    def __init__(self, data_queue):
        self.client_managers = {} 
        self.logger = logging.getLogger(__name__)
        self.auto_connect_devices = set()
        self.last_data_received = {}
        self.data_queue = data_queue
        self.logger.info(f"Data queue initialized with size: {data_queue}")
        self.command_queue = asyncio.Queue()
        # self.device_data_queue = asyncio.Queue()
        self.max_retries = 3
        self.connection_timeout = 30
        self.scheduled_tasks = []
        self.data_inactivity_timeout = 60
        # self.data_received_queue = asyncio.Queue()
        # self.data_processing_thread = threading.Thread(target=self.process_device_data_thread, daemon=True)
        # self.data_processing_thread.start()

    def generate_client_id(self, address):
        return str(uuid.uuid4())[:8]  # Use the first 8 characters of a UUID

    def generate_event_id(self, address):
        return str(uuid.uuid4())[:8] 

    async def run(self):
        while True:
            # Process any due scheduled tasks
            current_time = asyncio.get_event_loop().time()
            due_tasks = [task for task in self.scheduled_tasks if task[0] <= current_time]
            next_due_task = min(self.scheduled_tasks, key=lambda x: x[0]) if len(self.scheduled_tasks) > 0 else None   
            self.logger.info(f"Due tasks: {len(due_tasks)} out of {len(self.scheduled_tasks)} next task due in {(next_due_task[0] - current_time) if next_due_task else 'None'} seconds")
            for task in due_tasks:
                self.logger.info(f"Adding due task to command queue: {task[1]}")
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

            # Check for disconnections and data inactivity
            await self.check_client_manager_status()

    async def check_client_manager_status(self):
        current_time = time.time()
        self.logger.info(f"Checking client manager status at {current_time}")

        for address, client_manager in list(self.client_managers.items()):
            self.logger.info(f"Client manager {client_manager} exists for {address}")
            client = client_manager.client
            self.logger.info(f"Checking device status for {self.get_device_name(address)} client_manager_id={client_manager.client_id}: connected={client.is_connected} last_data_received={self.last_data_received.get(address, 0)} current_time={current_time} time_since_last_data={current_time - self.last_data_received.get(address, 0)}")
            if not client.is_connected:
                event_id = self.generate_event_id(address)
                self.logger.warning(f"[{event_id}] Detected disconnection for device {self.get_device_name(address)}")
                await self.queue_disconnect_and_reconnect(address, event_id, "Device disconnected on us")
            elif current_time - self.last_data_received.get(address, 0) > self.data_inactivity_timeout:
                event_id = self.generate_event_id(address)
                self.logger.warning(f"[{event_id}] No data received from {self.get_device_name(address)} for {self.data_inactivity_timeout} seconds")
                await self.queue_disconnect_and_reconnect(address, event_id, "No data received for too long")


    async def queue_disconnect_and_reconnect(self, address, event_id, reason: str = None):
        await self.queue_disconnect_device(address, event_id, reason)
        await self.command_queue.put(ConnectCommand(address, event_id, reason))

    def get_device_name(self, address: str) -> str:
        if address == os.getenv('CORE_DEVICE_ADDRESS'): 
            return "CORE"
        elif address == os.getenv('VIATOM_DEVICE_ADDRESS'):
            return "Viatom"
        elif address == os.getenv('POLAR_DEVICE_ADDRESS'):
            return "Polar"

        return address

    async def create_client_manager(self, client: BleakClient, address: str):
        client_id = self.generate_client_id(address)

        client_manager = None
        # The device's name is never available for some reason, so we need to check the user-specified device addresses
        if address == os.getenv('CORE_DEVICE_ADDRESS'): 
            client_manager = CoreClientManager(client, self.data_queue, client_id)
        elif address == os.getenv('VIATOM_DEVICE_ADDRESS'):
            client_manager = ViatomClientManager(client, self.data_queue, self, client_id)
        elif address == os.getenv('POLAR_DEVICE_ADDRESS'):
            client_manager = PolarClientManager(client, self.data_queue, client_id)
        else:
            self.logger.warning(f"Still unknown device type: {address}")
        
        return client_manager

    async def handle_post_connection(self, client: BleakClient, address: str):
        self.logger.info(f"Handling post connection for device {address}")
        self.update_last_data_received(address)
        device_name = self.get_device_name(address)
        
        client_manager = await self.create_client_manager(client, address)
        if client_manager is None:
            self.logger.error(f"Failed to create client manager for device {device_name}")
            return
        
        self.logger.info(f"[{client_manager.client_id}] Connected to device {device_name}")   
        
        try:
            await client_manager.subscribe()
            self.logger.info(f"[{client_manager.client_id}] Successfully subscribed to device {device_name}")
            self.client_managers[address] = client_manager 
        except Exception as e:
            event_id = self.generate_event_id(address)
            self.logger.error(f"[{event_id}] Failed to subscribe to device {device_name}: {e}", exc_info=True)
            await self.queue_disconnect_device(address, event_id, "Failed to subscribe")
    
    def update_last_data_received(self, address):
        self.logger.info(f"Updating last data received for {self.get_device_name(address)}")
        self.last_data_received[address] = time.time()


    async def queue_connect_to_specific_device(self, address, event_id, reason: str = None):
        self.logger.info(f"Attempting to connect to device at {address}")
        await self.command_queue.put(ConnectCommand(address, event_id, reason))

    async def cleanup_client_manager(self, address):
        self.logger.info(f"Cleaning up client manager {address}")
        if address in self.client_managers:
            client_manager = self.client_managers[address]
            client: BleakClient = client_manager.client
            try:
                self.logger.info(f"Cleaning up client manager {self.get_device_name(address)}")
                await client_manager.cleanup()
            except Exception as e:
                self.logger.error(f"Error cleaning up client manager {self.get_device_name(address)}: {e}", exc_info=True)
            finally:
                del self.client_managers[address]

            try:
                self.logger.info(f"Disconnecting client {self.get_device_name(address)}")
                await client.disconnect()
            except Exception as e:
                self.logger.error(f"Error disconnecting client {self.get_device_name(address)}: {e}", exc_info=True)
        else:
            self.logger.warning(f"Attempted to cleanup non-existent client manager {self.get_device_name(address)}")

    async def queue_disconnect_device(self, address, event_id, reason: str = None):
        await self.command_queue.put(DisconnectCommand(address, event_id, reason))

    async def schedule_command(self, command, delay):
        execution_time = asyncio.get_event_loop().time() + delay
        self.scheduled_tasks.append((execution_time, command))
        self.scheduled_tasks.sort(key=lambda x: x[0])

    async def notify_data_received(self, address):
        await self.data_received_queue.put(address)

    async def process_data_received_events(self):
        while not self.data_received_queue.empty():
            address = await self.data_received_queue.get()
            self.update_last_data_received(address)