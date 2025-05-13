import asyncio
import time
import sys
from bleak import BleakScanner, BleakClient
from bleak.exc import BleakError
import logging
from constants import RECONNECT_INTERVAL
from async_timeout import timeout as async_timeout
import os
from dotenv import load_dotenv
import threading
import uuid
from ble_logging import get_device_logger

from core_device import CoreClientManager
from polar_device import PolarClientManager
from viatom_device import ViatomClientManager
from movesense_device import MovesenseClientManager
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
        self.logger = get_device_logger(address)

    def __str__(self):
        return f"ConnectCommand[address={get_device_name(self.address)}, attempt={self.attempt}, event_id={self.event_id}, reason={self.reason}]"

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
            self.logger.error(f"Unexpected error on attempt {self.attempt + 1} connecting to device {device_name}: {e}")

        # Schedule next connection attempt
        next_attempt = self.attempt + 1
        wait_time = min(60, 2 ** next_attempt)
        
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
        logger = logging.getLogger("ble_queue")
        try:
            while True:
                # Check if BLE manager is active
                try:
                    from main import component_status
                    if not component_status.get("ble_manager_active", True):
                        logger.debug("BLE manager is disabled, sleeping")
                        await asyncio.sleep(1)
                        continue
                except ImportError:
                    # If we can't import component_status, assume it's active
                    pass
                
                # Process any due scheduled tasks
                try:
                    current_time = asyncio.get_event_loop().time()
                    due_tasks = [task for task in self.scheduled_tasks if task[0] <= current_time]
                    next_due_task = min(self.scheduled_tasks, key=lambda x: x[0]) if len(self.scheduled_tasks) > 0 else None   
                    logger.debug(f"Due tasks: {len(due_tasks)} out of {len(self.scheduled_tasks)} next task due in {(next_due_task[0] - current_time) if next_due_task else 'None'} seconds")
                    for task in due_tasks:
                        logger.info(f"Moving due task to command queue: {str(task[1])}")
                        self.scheduled_tasks.remove(task)
                        await self.command_queue.put(task[1])
                    if len(self.scheduled_tasks) < 10:
                        for scheduled_task in self.scheduled_tasks:
                            logger.debug(f"Remaining scheduled task: {str(scheduled_task[1])} due in {(scheduled_task[0] - current_time) if scheduled_task else 'None'} seconds")
                except Exception as e:
                    self.logger.error(f"Error processing due tasks: {e}")

                # Process commands from the queue
                logger.debug(f"Processing commands from the queue which has {self.command_queue.qsize()} items")
                try:
                    command = await asyncio.wait_for(self.command_queue.get(), timeout=1.0)
                    logger.debug(f"Processing command: {command}")
                    await command.execute(self)
                    self.command_queue.task_done()
                except asyncio.TimeoutError:
                    logger.debug(f"No commands in the queue at {time.time()}")
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logger.error(f"Error processing command: {e}")

                try:
                    # Check for disconnections and data inactivity
                    await self.check_client_manager_status()
                except Exception as e:
                    logger.error(f"Error checking client manager status: {e}")

        except Exception as e:
            logger.error(f"Terminal error in BLEManager queue processing - should never happen!!: {e}")
            sys.exit(1)

    async def check_client_manager_status(self):
        logger = logging.getLogger("ble_managers")

        current_time = time.time()
        logger.debug(f"Checking client manager status at {current_time} have {len(self.client_managers)} client managers")
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
                logger.warning(f"Found {len(managers)} client managers for {get_device_name(address)}. Removing extras.")
                for extra_manager in managers[1:]:
                    await self.disconnect_and_cleanup_client_manager(extra_manager, self.generate_event_id(address), "Removing extra client manager")
                managers = [managers[0]]

            client_manager = managers[0]
            client = client_manager.client
            logger = get_device_logger(address)
            logger.debug(f"Client manager {client_manager} exists for {get_device_name(address)}")
            time_since_last_data = current_time - self.last_data_received.get(address, 0)
            logger.debug(f"Checking device status for {get_device_name(address)} client_manager_id={client_manager.client_id}: connected={client.is_connected} last_data_received={self.last_data_received.get(address, 0)} current_time={current_time} time_since_last_data={time_since_last_data}")

            event_id = None
            reason = None

            if not client.is_connected:
                status = f"{get_device_name(address)}: Is no longer connected"
                event_id = self.generate_event_id(address)
                reason = "Device is no longer connected at " + time.strftime("%H:%M:%S", time.localtime())
            elif time_since_last_data > self.data_inactivity_timeout:
                status = f"{get_device_name(address)}: Connected but last data too old as {time_since_last_data:.1f}s ago"
                event_id = self.generate_event_id(address)
                reason = f"No data received for {self.data_inactivity_timeout} seconds at " + time.strftime("%H:%M:%S", time.localtime())
            else:
                status = f"{get_device_name(address)}: Connected, last data {time_since_last_data:.1f}s ago"


            logger.debug(f"Device status for {get_device_name(address)}: {status}")

            if reason is not None:
                await self.disconnect_and_cleanup_and_queue_reconnect(client_manager, address, event_id, reason)

            if status is not None:
                client_manager.status = status                

            device_statuses.append(status)

    async def disconnect_and_cleanup_and_queue_reconnect(self, client_manager, address, event_id, reason: str = None, delete_client_manager: bool = True):
        logger = get_device_logger(address)
        logger.warning(f"[{event_id}] reconnecting {get_device_name(address)} because {reason}")
        try:
            await self.disconnect_and_cleanup_client_manager(client_manager, event_id, reason, delete_client_manager)
        except Exception as e:
            logger.error(f"[{event_id}] Error disconnecting client manager for {get_device_name(address)}: {e}")
            # But continue with the reconnect attempt
        await self.queue_connect(address, event_id, reason)

    async def queue_connect(self, address, event_id, reason: str = None):
        logger = get_device_logger(address)
        logger.info(f"Queueing connect for {get_device_name(address)} because {reason}")
        existing_connect_commands = [task for task in self.scheduled_tasks if isinstance(task[1], ConnectCommand) and task[1].address == address]
        if existing_connect_commands:
            logger.info(f"Skipping scheduling new ConnectCommand for {get_device_name(address)} as one already exists")
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
        elif address == os.getenv('MOVESENSE_DEVICE_ADDRESS'):
            client_manager = MovesenseClientManager(client, self.data_queue, self, client_id, address)
        else:
            self.logger.warning(f"Still unknown device type: {address}")
        
        return client_manager

    async def handle_post_connection(self, client: BleakClient, address: str):
        logger = get_device_logger(address)
        logger.info(f"Handling post connection for device {address}")
        self.update_last_data_received(address)
        device_name = get_device_name(address)
        
        # Ensure services are discovered before proceeding
        try:
            logger.info(f"Discovering services for device {device_name}...")
            # Some BLE implementations might need explicit service discovery
            if hasattr(client, 'get_services'):
                await client.get_services()
            elif hasattr(client, 'discover_services'):
                await client.discover_services()
            else:
                # If no explicit discovery method, wait a moment for automatic discovery
                logger.info(f"No explicit discovery method, waiting for automatic discovery")
                await asyncio.sleep(2)
                
            # Log discovered services - don't use len() directly
            if client.services:
                # Count services by iterating
                services_list = list(client.services)
                service_count = len(services_list)
                logger.info(f"Found {service_count} services on device {device_name}")
                for service in services_list:
                    logger.info(f"Service: {service.uuid}")
            else:
                logger.warning(f"No services found on device {device_name} after discovery")
        except Exception as e:
            logger.error(f"Error discovering services for device {device_name}: {e}")
            # Continue anyway as the client might have automatically discovered services
        
        client_manager = await self.create_client_manager(client, address)
        if client_manager is None:
            logger.error(f"Failed to create client manager for device {device_name}")
            return
        
        logger.info(f"[{client_manager.client_id}] Connected to device {device_name}")   
        
        try:
            await client_manager.subscribe()
            logger.info(f"[cm={client_manager.client_id}] Successfully subscribed to device {device_name}")
            self.client_managers.append(client_manager) 
        except Exception as e:
            event_id = self.generate_event_id(address)
            logger.error(f"[event={event_id}] Failed to subscribe to device {device_name}: {e}")
            await self.disconnect_and_cleanup_and_queue_reconnect(client_manager, address, event_id, "Failed to subscribe", delete_client_manager=False)
            del client_manager
    
    def update_last_data_received(self, address):
        logger = get_device_logger(address)
        logger.debug(f"Updating last data received for {get_device_name(address)}")
        self.last_data_received[address] = time.time()


    async def queue_connect_to_specific_device(self, address, event_id, reason: str = None):
        logger = get_device_logger(address)
        logger.info(f"Attempting to connect to device at {address}")
        await self.command_queue.put(ConnectCommand(address, event_id, reason))

    async def disconnect_and_cleanup_client_manager(self, client_manager, event_id, reason: str = None, delete_client_manager: bool = True):
        address = client_manager.address
        logger = get_device_logger(address)
        logger.debug(f"Cleaning up client manager {client_manager.client_id} for address {get_device_name(client_manager.address)}")
        client: BleakClient = client_manager.client
        logger.info(f"Disconnecting from device {address} because {reason}")
        try:
            await asyncio.wait_for(client.disconnect(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.error(f"Timeout while disconnecting from {get_device_name(address)}")
        except Exception as e:
            logger.error(f"Error disconnecting client {get_device_name(address)}: {e}")

        try:
            logger.debug(f"Cleaning up client manager {get_device_name(address)}")
            await client_manager.cleanup()
        except Exception as e:
            logger.error(f"Error cleaning up client manager {get_device_name(address)}: {e}")
        finally:
            if delete_client_manager:
                if client_manager in self.client_managers:
                    self.client_managers.remove(client_manager)
                else:
                    logger.warning(f"Client manager {client_manager.client_id} for address {get_device_name(address)} not found in client_managers list")

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

    async def disconnect_all_devices(self):
        self.logger.info(f"Disconnecting all {len(self.client_managers)} devices")
        for client_manager in self.client_managers:
            event_id = self.generate_event_id(client_manager.address)
            await self.disconnect_and_cleanup_client_manager(client_manager, event_id, "Shutting down")

    def get_client_manager(self, device_address):
        """
        Retrieve the client manager for the given device address.
        Returns None if no matching client manager is found.
        """
        for client_manager in self.client_managers:
            if client_manager.address == device_address:
                return client_manager
        return None
        
    def get_status(self):
        """
        Get the current status of the BLE manager including command queue size,
        number of client managers, and scheduled tasks.
        """
        return {
            "command_queue_size": self.command_queue.qsize(),
            "client_managers_count": len(self.client_managers),
            "scheduled_tasks_count": len(self.scheduled_tasks),
            "connected_devices": [
                {
                    "address": cm.address, 
                    "status": cm.status,
                    "last_data": self.last_data_received.get(cm.address, 0)
                } 
                for cm in self.client_managers
            ]
        }