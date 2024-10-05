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
from constants import get_device_name
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
        device_name = get_device_name(self.address)
        self.logger.info(f"Attempting to connect to device {device_name} (attempt {self.attempt + 1}) because {self.reason}")

        try:
            async with async_timeout(manager.connection_timeout):
                client = BleakClient(self.address)
                self.logger.info(f"Connecting to device {device_name} on attempt {self.attempt + 1} for reason {self.reason}")
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
        
        # Check if there's already a ConnectCommand for this address
        existing_connect_commands = [task for task in manager.scheduled_tasks 
                                     if isinstance(task[1], ConnectCommand) and task[1].address == self.address]
        if not existing_connect_commands:
            self.logger.info(f"Scheduling next connection attempt to {device_name} in {wait_time} seconds")
            await manager.schedule_command(ConnectCommand(self.address, self.event_id, self.reason, next_attempt), wait_time)
        else:
            self.logger.info(f"Skipping scheduling new ConnectCommand for {device_name} as one already exists")

        return False

class BLEManager:
    def __init__(self, data_queue):
        self.client_managers = []
        self.logger = logging.getLogger(__name__)
        self.auto_connect_devices = set()
        self.last_data_received = {}
        self.data_queue = data_queue
        self.logger.info(f"Data queue initialized with size: {data_queue}")
        self.command_queue = asyncio.Queue()
        self.connection_timeout = 30
        self.scheduled_tasks = []
        self.data_inactivity_timeout = 60

    def generate_client_id(self, address):
        return str(uuid.uuid4())[:8]  # Use the first 8 characters of a UUID

    def generate_event_id(self, address):
        return str(uuid.uuid4())[:8] 

    async def run(self):
        while True:
            # Process any due scheduled tasks
            try:
                current_time = asyncio.get_event_loop().time()
                due_tasks = [task for task in self.scheduled_tasks if task[0] <= current_time]
                next_due_task = min(self.scheduled_tasks, key=lambda x: x[0]) if len(self.scheduled_tasks) > 0 else None   
                self.logger.info(f"Due tasks: {len(due_tasks)} out of {len(self.scheduled_tasks)} next task due in {(next_due_task[0] - current_time) if next_due_task else 'None'} seconds")
                for task in due_tasks:
                    self.logger.info(f"Moving due task to command queue: {str(task[1])}")
                    self.scheduled_tasks.remove(task)
                    await self.command_queue.put(task[1])
                if len(self.scheduled_tasks) < 10:
                    for scheduled_task in self.scheduled_tasks:
                        self.logger.info(f"Remaining scheduled task: {str(scheduled_task[1])} due in {(scheduled_task[0] - current_time) if scheduled_task else 'None'} seconds")
            except Exception as e:
                self.logger.error(f"Error processing due tasks: {e}", exc_info=True)

            # Process commands from the queue
            self.logger.info(f"Processing commands from the queue which has {self.command_queue.qsize()} items")
            try:
                command = await asyncio.wait_for(self.command_queue.get(), timeout=1.0)
                self.logger.info(f"Processing command: {command}")
                await command.execute(self)
                self.command_queue.task_done()
            except asyncio.TimeoutError:
                self.logger.info(f"No commands in the queue at {time.time()}")
                await asyncio.sleep(0.1)

            try:
                # Check for disconnections and data inactivity
                await self.check_client_manager_status()
            except Exception as e:
                self.logger.error(f"Error checking client manager status: {e}", exc_info=True)

    async def check_client_manager_status(self):
        current_time = time.time()
        self.logger.info(f"Checking client manager status at {current_time}")
        device_statuses = []

        # Group client managers by address
        client_managers_by_address = {}
        for client_manager in self.client_managers:
            address = client_manager.address
            if address not in client_managers_by_address:
                client_managers_by_address[address] = []
            client_managers_by_address[address].append(client_manager)

        for address, managers in client_managers_by_address.items():
            # Remove extra client managers if there are more than one for the same address
            if len(managers) > 1:
                self.logger.warning(f"Found {len(managers)} client managers for {get_device_name(address)}. Removing extras.")
                for extra_manager in managers[1:]:
                    await self.disconnect_and_cleanup_client_manager(extra_manager, self.generate_event_id(address), "Removing extra client manager")
                managers = [managers[0]]

            client_manager = managers[0]
            client = client_manager.client
            self.logger.info(f"Client manager {client_manager} exists for {get_device_name(address)}")
            time_since_last_data = current_time - self.last_data_received.get(address, 0)
            self.logger.info(f"Checking device status for {get_device_name(address)} client_manager_id={client_manager.client_id}: connected={client.is_connected} last_data_received={self.last_data_received.get(address, 0)} current_time={current_time} time_since_last_data={time_since_last_data}")

            event_id = None
            reason = None

            if not client.is_connected:
                status = f"{get_device_name(address)}: Disconnected"
                event_id = self.generate_event_id(address)
                reason = "Device disconnected on us at " + time.strftime("%H:%M:%S", time.localtime())
            elif time_since_last_data > self.data_inactivity_timeout:
                status = f"{get_device_name(address)}: Connected but last data too old as {time_since_last_data:.1f}s ago"
                event_id = self.generate_event_id(address)
                reason = f"No data received for {self.data_inactivity_timeout} seconds at " + time.strftime("%H:%M:%S", time.localtime())
            else:
                status = f"{get_device_name(address)}: Connected, last data {time_since_last_data:.1f}s ago"

            if reason is not None:
                await self.disconnect_and_cleanup_and_queue_reconnect(client_manager, address, event_id, reason)

            device_statuses.append(status)

        summary = " | ".join(device_statuses)
        self.logger.info(f"Device status summary: {summary}")

    async def disconnect_and_cleanup_and_queue_reconnect(self, client_manager, address, event_id, reason: str = None):
        self.logger.warning(f"[{event_id}] reconnecting {get_device_name(address)} because {reason}")
        try:
            await self.disconnect_and_cleanup_client_manager(client_manager, event_id, reason)
        except Exception as e:
            self.logger.error(f"[{event_id}] Error disconnecting client manager for {get_device_name(address)}: {e}", exc_info=True)
            # But continue with the reconnect attempt
        await self.queue_connect(address, event_id, reason)

    async def queue_connect(self, address, event_id, reason: str = None):
        self.logger.info(f"Queueing connect for {get_device_name(address)} because {reason}")
        existing_connect_commands = [task for task in self.scheduled_tasks if isinstance(task[1], ConnectCommand) and task[1].address == address]
        if existing_connect_commands:
            self.logger.info(f"Skipping scheduling new ConnectCommand for {get_device_name(address)} as one already exists")
        else:
            await self.command_queue.put(ConnectCommand(address, event_id, reason))

    async def create_client_manager(self, client: BleakClient, address: str):
        client_id = self.generate_client_id(address)

        client_manager = None
        # The device's name is never available for some reason, so we need to check the user-specified device addresses
        if address == os.getenv('CORE_DEVICE_ADDRESS'): 
            client_manager = CoreClientManager(client, self.data_queue, client_id, address)
        elif address == os.getenv('VIATOM_DEVICE_ADDRESS'):
            client_manager = ViatomClientManager(client, self.data_queue, self, client_id, address)
        elif address == os.getenv('POLAR_DEVICE_ADDRESS'):
            client_manager = PolarClientManager(client, self.data_queue, client_id, address)
        else:
            self.logger.warning(f"Still unknown device type: {address}")
        
        return client_manager

    async def handle_post_connection(self, client: BleakClient, address: str):
        self.logger.info(f"Handling post connection for device {address}")
        self.update_last_data_received(address)
        device_name = get_device_name(address)
        
        client_manager = await self.create_client_manager(client, address)
        if client_manager is None:
            self.logger.error(f"Failed to create client manager for device {device_name}")
            return
        
        self.logger.info(f"[{client_manager.client_id}] Connected to device {device_name}")   
        
        try:
            await client_manager.subscribe()
            self.logger.info(f"[{client_manager.client_id}] Successfully subscribed to device {device_name}")
            self.client_managers.append(client_manager) 
        except Exception as e:
            event_id = self.generate_event_id(address)
            self.logger.error(f"[{event_id}] Failed to subscribe to device {device_name}: {e}", exc_info=True)
            await self.disconnect_and_cleanup_and_queue_reconnect(client_manager, address, event_id, "Failed to subscribe")
    
    def update_last_data_received(self, address):
        self.logger.info(f"Updating last data received for {get_device_name(address)}")
        self.last_data_received[address] = time.time()


    async def queue_connect_to_specific_device(self, address, event_id, reason: str = None):
        self.logger.info(f"Attempting to connect to device at {address}")
        await self.command_queue.put(ConnectCommand(address, event_id, reason))

    async def disconnect_and_cleanup_client_manager(self, client_manager, event_id, reason: str = None):
        address = client_manager.address
        self.logger.info(f"Cleaning up client manager {client_manager.client_id} for address {get_device_name(client_manager.address)}")
        client: BleakClient = client_manager.client
        self.logger.info(f"Disconnecting from device {address} because {reason}")
        try:
            await client.disconnect()
        except Exception as e:
            self.logger.error(f"Error disconnecting client {get_device_name(address)}: {e}", exc_info=True)
        try:
            self.logger.info(f"Cleaning up client manager {get_device_name(address)}")
            await client_manager.cleanup()
        except Exception as e:
            self.logger.error(f"Error cleaning up client manager {get_device_name(address)}: {e}", exc_info=True)
        finally:
            self.client_managers.remove(client_manager)

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