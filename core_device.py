import asyncio
import logging
from dataclasses import dataclass
import time
from queue import Queue
from ble_command import SharedData

from constants import UUIDs

@dataclass
class CoreTempData(SharedData):
    temp: float
    unit: str
    quality: str = ""
    skin_temp: float = None

class CoreConstants:
    CLIENT_CHARACTERISTIC_CONFIG = "00002902-0000-1000-8000-00805f9b34fb"
    CORE_TEMP_SERVICE_UUID = "00002100-5b1e-4347-b07c-97b514dae121"
    CORE_TEMP_SERVICE_CHARACTERISTIC1_UUID = "00002101-5b1e-4347-b07c-97b514dae121"
    TEMPERATURE_MEASUREMENT_CHARACTERISTIC_UUID = "00002102-5b1e-4347-b07c-97b514dae121"

class CoreClientManager:
    def __init__(self, client, data_queue: asyncio.Queue, client_id: str):
        self.client = client
        self.logger = logging.getLogger(f"{__name__}.{client_id}")
        self.data_queue = data_queue
        self.client_id = client_id
        self.logger.info(f"CoreClientManager initialized with client_id: {client_id}")
        self.dead = False

    def __str__(self):
        return f"CoreClientManager(client_id={self.client_id})"

    def __del__(self):
        self.logger.info(f"CoreClientManager {self.client_id} being deleted")
        self.cleanup()

    async def cleanup(self):
        self.logger.info(f"CoreClientManager {self.client_id} being cleaned up")
        pass

    async def core_temperature_measurement_handler(self, sender, data):
        if self.dead:
            self.logger.info(f"Is dead, ignoring data")
            return

        if not self.client.is_connected:
            self.logger.warn(f"Device {self.client.address} is not connected")
            return

        try:
            timestamp = int(time.time() * 1e9)  # nanosecond precision
                
            flags = data[0]
            skin_temp_present = flags & 0x1 != 0
            core_reserved_present = flags & 0x2 != 0
            quality_and_state_present = flags & 0x4 != 0
            unit = "Fahrenheit" if flags & 0x08 else "Celsius"
            heart_rate_present = flags & 0x10 != 0

            core_temp = int.from_bytes(data[1:3], byteorder='little') / 100
            invalid_data = data[1] == 0xff and data[2] == 0x7f

            index = 3

            skin_temp = None
            if skin_temp_present:
                skin_temp = int.from_bytes(data[index:index+2], byteorder='little') / 100
                index += 2

            if core_reserved_present:
                index += 2

            quality_str = ""
            if quality_and_state_present:
                quality_and_state = data[index]
                quality = quality_and_state & 0x7
                quality_str = ["Invalid", "Poor", "Fair", "Good", "Excellent", "Unknown", "Unknown", "N/A"][quality]
                index += 1

            if heart_rate_present:
                heart_rate = data[index]
                index += 1

            self.logger.info(f"Temperature: core={core_temp} skin={skin_temp} {unit} {quality_str} invalid={invalid_data}")

            if not invalid_data:
                core_temp_data = CoreTempData(
                    temp=core_temp,
                    unit=unit,
                    device_address=self.client.address,
                    timestamp=timestamp,
                    quality=quality_str,
                    skin_temp=skin_temp
                )
                
                self.data_queue.put(core_temp_data)
        except Exception as e:
            self.logger.error(f"Error handling temperature measurement: {e}", exc_info=True)

    async def subscribe(self):
        service = self.client.services.get_service(CoreConstants.CORE_TEMP_SERVICE_UUID)
        char1 = service.get_characteristic(CoreConstants.CORE_TEMP_SERVICE_CHARACTERISTIC1_UUID)
        cccd = char1.get_descriptor(CoreConstants.CLIENT_CHARACTERISTIC_CONFIG)

        self.logger.info(f"CORE service: {service}")
        self.logger.info(f"CORE characteristic1: {char1}")
        self.logger.info(f"CORE cccd: {cccd}")

        # Don't have to write to the CCCD manually, Bleak takes care of it
        await self.client.start_notify(
            CoreConstants.CORE_TEMP_SERVICE_CHARACTERISTIC1_UUID,
            self.core_temperature_measurement_handler
        )