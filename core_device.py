import logging
from dataclasses import dataclass
import time
from queue import Queue

from constants import UUIDs

@dataclass
class CoreTempData:
    temp: float
    unit: str
    device_address: str
    timestamp: int
    quality: str = ""
    skin_temp: float = None

class CoreConstants:
    CLIENT_CHARACTERISTIC_CONFIG = "00002902-0000-1000-8000-00805f9b34fb"
    CORE_TEMP_SERVICE_UUID = "00002100-5b1e-4347-b07c-97b514dae121"
    CORE_TEMP_SERVICE_CHARACTERISTIC1_UUID = "00002101-5b1e-4347-b07c-97b514dae121"
    TEMPERATURE_MEASUREMENT_CHARACTERISTIC_UUID = "00002102-5b1e-4347-b07c-97b514dae121"

class CoreDevice:
    def __init__(self, client, data_queue: Queue):
        self.client = client
        self.logger = logging.getLogger(__name__)
        self.data_queue = data_queue

    def temperature_measurement_handler(self, sender, data):
        try:
            flags = data[0]
            unit = "Fahrenheit" if flags & 0x01 else "Celsius"
            temp_data = int.from_bytes(data[1:5], byteorder='little')
            
            if temp_data == 0x007FFFFF:
                self.logger.debug("Received cbt value IEEE11073 NaN (not a number).")
            elif temp_data == 0x007FFFFE:
                self.logger.debug("Received cbt value IEEE11073 + Infinity")
            elif temp_data == 0x00800002:
                self.logger.debug("Received cbt value IEEE11073 - Infinity")
            elif temp_data == 0x00800000:
                self.logger.debug("Received cbt value IEEE11073 NRes (Not at this resolution).")
            else:
                exponent = temp_data >> 24
                mantissa = temp_data & 0x00FFFFFF
                actual_temp = mantissa * (10 ** exponent)
                self.logger.info(f"Temperature: {actual_temp} {unit}")
                
                timestamp = int(time.time() * 1e9)  # nanosecond precision
                
                core_temp_data = CoreTempData(
                    temp=actual_temp,
                    unit=unit,
                    device_address=self.client.address,
                    timestamp=timestamp
                )
                
                self.data_queue.put(core_temp_data)
        except Exception as e:
            self.logger.error(f"Error handling temperature measurement: {e}")

    def core_temperature_measurement_handler(self, sender, data):
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
            self.logger.error(f"Error handling temperature measurement: {e}")

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