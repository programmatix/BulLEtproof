import asyncio
from functools import reduce
import logging
from dataclasses import dataclass
import struct
import time
from ble_command import SharedData

from bleak import BleakClient

from ble_command import BLECommand
from constants import get_device_name
from ble_logging import get_device_logger

@dataclass
class MovesenseHRData(SharedData):
    hr: float
    rr_intervals: list[int]

@dataclass
class MovesenseAccelData(SharedData):
    x: float
    y: float
    z: float
    sensor_timestamp: int

@dataclass
class MovesenseBatteryData(SharedData):
    level: float

class MovesenseConstants:
    WRITE_UUID = "34800001-7185-4d5d-b431-630e7050e8f0"
    NOTIFY_UUID = "34800002-7185-4d5d-b431-630e7050e8f0"
    PACKET_TYPE_DATA = 2
    PACKET_TYPE_DATA_PART2 = 3
    PACKET_TYPE_COMMAND_RESPONSE = 1
    REF_HR = 102
    REF_ACCEL = 101
    REF_BATTERY = 103

class DataView:
    def __init__(self, array, bytes_per_element=1):
        self.array = array
        self.bytes_per_element = 1

    def __get_binary(self, start_index, byte_count, signed=False):
        integers = [self.array[start_index + x] for x in range(byte_count)]
        _bytes = [integer.to_bytes(self.bytes_per_element, byteorder='little', signed=signed) for integer in integers]
        return reduce(lambda a, b: a + b, _bytes)

    def get_uint_16(self, start_index):
        bytes_to_read = 2
        return int.from_bytes(self.__get_binary(start_index, bytes_to_read), byteorder='little')

    def get_uint_8(self, start_index):
        bytes_to_read = 1
        return int.from_bytes(self.__get_binary(start_index, bytes_to_read), byteorder='little')

    def get_uint_32(self, start_index):
        bytes_to_read = 4
        binary = self.__get_binary(start_index, bytes_to_read)
        return struct.unpack('<I', binary)[0]

    def get_int_32(self, start_index):
        bytes_to_read = 4
        binary = self.__get_binary(start_index, bytes_to_read)
        return struct.unpack('<i', binary)[0]

    def get_float_32(self, start_index):
        bytes_to_read = 4
        binary = self.__get_binary(start_index, bytes_to_read)
        return struct.unpack('<f', binary)[0]

class MovesenseClientManager:
    def __init__(self, client, data_queue, ble_manager, client_id: str, address: str):
        self.client = client
        self.data_queue = data_queue
        self.ble_manager = ble_manager
        self.logger = logging.getLogger(f"movesense.{client_id}")
        self.client_id = client_id
        self.dead = False
        self.address = address
        self.logger.info(f"MovesenseClientManager initialized with client_id: {client_id}")

    def __str__(self):
        return f"MovesenseClientManager(client_id={self.client_id}, address={get_device_name(self.address)})"

    async def cleanup(self):
        if hasattr(self, 'logger'):
            self.logger.info(f"MovesenseClientManager {self.client_id} being cleaned up")
        self.dead = True

    def __del__(self):
        if hasattr(self, 'logger'):
            self.logger.info(f"MovesenseClientManager {self.client_id} being deleted")
        if not self.dead:
            asyncio.create_task(self.cleanup())

    async def data_handler(self, sender, data):
        if self.dead:
            self.logger.info(f"Is dead, ignoring data")
            return

        if not self.client.is_connected:
            self.logger.warn(f"Device {self.client.address} is not connected")
            return

        d = DataView(data)
        packet_type = d.get_uint_8(0)
        reference = d.get_uint_8(1)

        ref = "accel" if reference == MovesenseConstants.REF_ACCEL else "hr"
        ref = "battery" if reference == MovesenseConstants.REF_BATTERY else ref

        if packet_type == MovesenseConstants.PACKET_TYPE_DATA and reference == MovesenseConstants.REF_ACCEL:
            timestamp = d.get_uint_32(2)
            offset = 6 + 0 * 3 * 4
            acc_x = d.get_float_32(offset)
            acc_y = d.get_float_32(offset + 4)
            acc_z = d.get_float_32(offset + 8)
            bytes_remaining = len(data) - offset - 12
            self.logger.debug(f"Accel Data: Timestamp={timestamp}, X={acc_x}, Y={acc_y}, Z={acc_z}, Bytes remaining: {bytes_remaining}")
            self.data_queue.put(MovesenseAccelData(
                x=float(acc_x),
                y=float(acc_y),
                z=float(acc_z),
                sensor_timestamp=timestamp,
                timestamp=int(time.time() * 1e9),
                device_address=self.address
            ))
        elif packet_type == MovesenseConstants.PACKET_TYPE_DATA and reference == MovesenseConstants.REF_HR:
            buffer_len = len(data) - 2
            buffer = data[2:]
            
            hr_data = MovesenseHRData(
                hr=0.0,
                rr_intervals=[],
                timestamp=int(time.time() * 1e9),
                device_address=self.address
            )
            
            if len(buffer) >= 4:
                hr_data.hr = d.get_float_32(2)
                pos = 6
                
                while pos + 1 < len(data):
                    rr_interval = d.get_uint_16(pos)
                    pos += 2
                    
                    hr_data.rr_intervals.append(float(rr_interval))
            
            self.logger.debug(f"HR Data: HR={hr_data.hr} BPM, RR={hr_data.rr_intervals if hr_data.rr_intervals else 'none'} ms")
            self.data_queue.put(hr_data)
        elif packet_type == MovesenseConstants.PACKET_TYPE_DATA and reference == MovesenseConstants.REF_BATTERY:
            battery_level = d.get_float_32(2)
            self.logger.debug(f"Battery Data: Level={battery_level}%")
            self.data_queue.put(MovesenseBatteryData(
                level=float(battery_level),
                timestamp=int(time.time() * 1e9),
                device_address=self.address
            ))
        elif packet_type == MovesenseConstants.PACKET_TYPE_COMMAND_RESPONSE:
            status_code = d.get_uint_16(2)
            self.logger.info(f"Command Response: Reference={reference} ({ref}), Status={status_code}")
        else:
            self.logger.info(f"Received unknown data from Movesense device: {data.hex()}")

    async def subscribe(self):
        try:
            await self.client.start_notify(
                MovesenseConstants.NOTIFY_UUID,
                lambda sender, data: asyncio.create_task(self.data_handler(sender, data))
            )
            self.logger.info("Notifications started successfully")
        except Exception as e:
            self.logger.error(f"Failed to start notifications: {e}")
            raise

        try:
            await self.client.write_gatt_char(MovesenseConstants.WRITE_UUID, bytearray([1, MovesenseConstants.REF_HR]) + bytearray("/Meas/HR", "utf-8"), response=True)
            self.logger.info("Write command sent successfully for HR")
        except EOFError as e:
            self.logger.error(f"Communication error while writing to characteristic for HR: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to write to characteristic for HR: {e}")
            raise

        try:
            await self.client.write_gatt_char(MovesenseConstants.WRITE_UUID, bytearray([1, MovesenseConstants.REF_ACCEL]) + bytearray("/Meas/Acc/13", "utf-8"), response=True)
            self.logger.info("Write command sent successfully for Accel")
        except EOFError as e:
            self.logger.error(f"Communication error while writing to characteristic for Accel: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to write to characteristic for Accel: {e}")
            raise        