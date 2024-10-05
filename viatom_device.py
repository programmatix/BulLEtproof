import asyncio
import logging
from dataclasses import dataclass
import time
from ble_command import SharedData

from bleak import BleakClient

from ble_command import BLECommand
from constants import get_device_name
from constants import UUIDs

@dataclass
class ViatomData(SharedData):
    hr: float
    spo2: float
    perfusion_index: float
    battery: float
    movement: float

class ViatomConstants:
    SERVICE_UUID = "14839ac4-7d7e-415c-9a42-167340cf2339"
    WRITE_UUID = "8B00ACE7-EB0B-49B0-BBE9-9AEE0A26E1A3"
    NOTIFY_UUID = "0734594A-A8E7-4B1A-A6B1-CD5243059A57"
    CLIENT_CHARACTERISTIC_DESCRIPTOR_UUID = "00002902-0000-1000-8000-00805f9b34fb"
    WRITE_BYTES = bytearray([0xaa, 0x17, 0xe8, 0x00, 0x00, 0x00, 0x00, 0x1b])    

class ViatomClientManager:
    def __init__(self, client, data_queue, ble_manager, client_id: str, address: str):
        self.client = client
        self.data_queue = data_queue
        self.ble_manager = ble_manager
        self.logger = logging.getLogger(f"{__name__}.{client_id}")
        self.write_char = None
        self.client_id = client_id
        self.future_request_more_data = None
        self.logger.info(f"ViatomClientManager initialized with client_id: {client_id}")
        self.dead = False
        self.address = address

    def __str__(self):
        return f"ViatomClientManager(client_id={self.client_id}, address={get_device_name(self.address)})"

    async def cleanup(self):
        self.logger.info(f"ViatomClientManager {self.client_id} being cleaned up")
        self.dead = True
        if self.future_request_more_data:
            self.future_request_more_data.cancel()

    def __del__(self):
        self.logger.info(f"ViatomClientManager {self.client_id} being deleted")
        self.cleanup()

    async def data_handler(self, sender, data):
        if self.dead:
            self.logger.info(f"Is dead, ignoring data")
            return

        if not self.client.is_connected:
            self.logger.warn(f"Device {self.client.address} is not connected")
            return

        timestamp = int(time.time() * 1e9)  # nanosecond precision
        self.logger.info(f"Received data from Viatom device: {data.hex()}")
        
        if len(data) > 1:
            no_data = True

            if data[18] == 0:
                self.logger.info(f"Device is not being worn!\tBattery: {data[14]}%")
            elif data[7] == 0 and data[8] == 0:
                self.logger.info(f"Device is calibrating...\tBattery: {data[14]}%")
            else:
                self.logger.info(f"SpO2: {data[7]}%\tHR: {data[8]} bpm\tPI: {data[17]}\tMovement: {data[16]}\tBattery: {data[14]}%")

                self.spo2 = float(data[7])
                self.hr = float(data[8])
                self.battery = float(data[14])
                self.movement = float(data[16])
                self.perfusion_index = float(data[17])

                no_data = False

            if no_data:
                self.spo2 = None
                self.hr = None
                self.battery = None
                self.movement = None
            else:
                self.data_queue.put(ViatomData(
                    hr=self.hr,
                    spo2=self.spo2,
                    perfusion_index=self.perfusion_index,
                    battery=self.battery,
                    movement=self.movement,
                    timestamp=timestamp,
                    device_address=self.client.address
                ))

            if self.future_request_more_data:
                self.future_request_more_data.cancel()

            await self.ble_manager.schedule_command(self.RequestMoreDataCommand(self), 2)

    class RequestMoreDataCommand(BLECommand):
        def __init__(self, viatom_device):
            self.viatom_device = viatom_device

        def __str__(self):
            return f"RequestMoreDataCommand(viatom_device={self.viatom_device.address})"

        async def execute(self, manager):
            if self.viatom_device.dead:
                self.viatom_device.logger.info(f"Ignoring RequestMoreDataCommand (viatom_device.dead)")
                return
            
            if not self.viatom_device.client.is_connected:
                self.viatom_device.logger.warn(f"Device {self.viatom_device.client.address} is not connected (RequestMoreDataCommand)")
                return
            
            await self.viatom_device.client.write_gatt_char(self.viatom_device.write_char, ViatomConstants.WRITE_BYTES)

    async def subscribe(self):
        service = self.client.services.get_service(ViatomConstants.SERVICE_UUID)

        self.write_char = service.get_characteristic(ViatomConstants.WRITE_UUID)

        try:
            await self.client.start_notify(
                ViatomConstants.NOTIFY_UUID,
                self.data_handler,
                response=True
            )
            self.logger.info("Notifications started successfully")
        except Exception as e:
            self.logger.error(f"Failed to start notifications: {e}")
            raise

        try:
            await self.client.write_gatt_char(ViatomConstants.WRITE_UUID, ViatomConstants.WRITE_BYTES, response=True)
            self.logger.info("Write command sent successfully")
        except Exception as e:
            self.logger.error(f"Failed to write to characteristic: {e}")
            raise


