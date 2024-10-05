import asyncio
import logging
from dataclasses import dataclass, field
import time
import json
from datetime import datetime, timedelta

from bleak import BleakClient
from ble_command import SharedData, BLECommand
import math
from enum import Enum
from typing import List, Tuple

from constants import get_device_name

@dataclass
class PolarHRData(SharedData):
    hr: int
    rr_intervals: list
    hrv_rmssd_very_recent: float
    hrv_rmssd_somewhat_recent: float
    hrv_rmssd_less_recent: float
    hrv_sd_very_recent: float
    hrv_sd_somewhat_recent: float
    hrv_sd_less_recent: float

class PositionEnum(Enum):
    LyingBack = "LyingBack"
    Upright = "Upright"
    LyingRight = "LyingRight"
    LyingLeft = "LyingLeft"
    Unknown = "Unknown"

@dataclass
class PolarAccData(SharedData):
    x: float
    y: float
    z: float
    position: PositionEnum
    timestamp: int

@dataclass
class AccelerometerMeasurement:
    values: List[Tuple[int, int, int]]



class PolarConstants:
    HEART_RATE_SERVICE_UUID = "0000180d-0000-1000-8000-00805f9b34fb"
    HEART_RATE_MEASUREMENT_UUID = "00002a37-0000-1000-8000-00805f9b34fb"
    PMD_SERVICE_UUID = "fb005c80-02e7-f387-1cad-8acd2d8df0c8"
    PMD_CONTROL_UUID = "fb005c81-02e7-f387-1cad-8acd2d8df0c8"
    PMD_DATA_UUID = "fb005c82-02e7-f387-1cad-8acd2d8df0c8"
    POLAR_ACC_WRITE = bytearray([
        0x02,  # Start measurement
        0x02,  # ACC
        0x00,  # SAMPLE_RATE
        0x01,  # array count
        0xc8,  # 0x32=50hz 0xc800=200hz
        0x00,
        0x01,  # RESOLUTION
        0x01,  # array count
        0x10,  # 0=8 bit (0000), 1=16 bit (1000), 2=24 bit (2000)
        0x00,
        0x02,  # RANGE
        0x01,  # array count
        0x08,  # 8
        0x00
    ])

@dataclass
class HRVResult:
    hrv_rmssd_very_recent: int
    hrv_rmssd_somewhat_recent: int
    hrv_rmssd_less_recent: int
    hrv_sd_very_recent: int
    hrv_sd_somewhat_recent: int
    hrv_sd_less_recent: int
    hr: int
    rr_intervals: List[int] = field(default_factory=list)

# https://github.com/kieranabrennan/dont-hold-your-breath/blob/master/resources/Polar_Measurement_Data_Specification.pdf
# Recommends:
# Min MTU of 232 - Bluez defaults to 513
# 2mb PHY
class PolarClientManager:
    def __init__(self, client: BleakClient, data_queue: asyncio.Queue, client_id: str, address: str):
        self.client = client
        self.data_queue = data_queue
        self.logger = logging.getLogger(f"{__name__}.{client_id}")
        self.client_id = client_id
        self.start_time = time.time()
        self.last_accelerometer_data = None
        self.dead = False
        self.hr_entries = []
        self.hrv_entries = []
        self.address = address
        self.logger.info(f"PolarClientManager initialized with client_id: {client_id}")

    def __str__(self):
        return f"PolarClientManager(client_id={self.client_id}, address={get_device_name(self.address)})"

    async def cleanup(self):
        self.logger.info(f"PolarClientManager {self.client_id} being cleaned up")
        self.dead = True

    def __del__(self):
        self.logger.info(f"PolarClientManager {self.client_id} being deleted")
        self.cleanup()

    async def hr_handler(self, sender, data):
        if self.dead or not self.client.is_connected:
            return

        timestamp = int(time.time() * 1e9)
        self.logger.info(f"Received HR data from Polar device: {data.hex()}")

        result = self.handle_heart_rate_measurement(data, timestamp)
        hr_data = PolarHRData(
            hr=result.hr,
            rr_intervals=result.rr_intervals,
            hrv_rmssd_very_recent=result.hrv_rmssd_very_recent,
            hrv_rmssd_somewhat_recent=result.hrv_rmssd_somewhat_recent,
            hrv_rmssd_less_recent=result.hrv_rmssd_less_recent,
            hrv_sd_very_recent=result.hrv_sd_very_recent,
            hrv_sd_somewhat_recent=result.hrv_sd_somewhat_recent,
            hrv_sd_less_recent=result.hrv_sd_less_recent,
            timestamp=timestamp,
            device_address=self.client.address
        )
        self.data_queue.put(hr_data)

    async def acc_handler(self, sender, data):
        try:
            if self.dead or not self.client.is_connected:
                self.logger.info(f"PolarClientManager {self.client_id} is dead or not connected")
                return

            timestamp = int(time.time() * 1e9)
            acc_data: PolarAccData = self.handle_accelerometer_data(data, timestamp)
            self.logger.info(f"Received ACC data from Polar device: {acc_data}")
            if acc_data is not None:
                now = datetime.now()
                if self.last_accelerometer_data is None or now > self.last_accelerometer_data + timedelta(seconds=5):
                    self.last_accelerometer_data = now
                    self.data_queue.put(acc_data)

        except Exception as e:
            self.logger.error(f"Error processing accelerometer data: {e}", exc_info=True)

    @staticmethod
    def position(x: int, y: int, z: int) -> PositionEnum:
        if z > 800:
            return PositionEnum.LyingBack
        elif y < -600:
            return PositionEnum.LyingRight
        elif y > 600:
            return PositionEnum.LyingLeft
        elif x < -500:
            return PositionEnum.Upright
        return PositionEnum.Unknown

    @staticmethod
    def calculate_average_rr(rr_intervals: List[int]) -> float:
        if not rr_intervals:
            return 0.0
        return sum(rr_intervals) / len(rr_intervals)

    @staticmethod
    def calculate_average_hr_in_bpm(average_rr: float) -> float:
        if average_rr == 0.0:
            return 0.0
        return 60 / (average_rr / 1000)

    @staticmethod
    def calculate_sdnn(rr_intervals: List[int], average_rr: float) -> float:
        if len(rr_intervals) < 2:
            return 0.0
        sum_squared_diff = sum((rr - average_rr) ** 2 for rr in rr_intervals)
        return math.sqrt(sum_squared_diff / (len(rr_intervals) - 1))

    @staticmethod
    def calculate_rmssd(rr_intervals: List[int]) -> float:
        if len(rr_intervals) < 2:
            return 0.0
        sum_squared_diff = sum((rr_intervals[i+1] - rr_intervals[i]) ** 2 for i in range(len(rr_intervals) - 1))
        return math.sqrt(sum_squared_diff / (len(rr_intervals) - 1))

    @staticmethod
    def calculate_sd_hrv(rr_intervals: List[int]) -> float:
        if len(rr_intervals) < 2:
            return 0.0
        sum_abs_diff = sum(abs(rr_intervals[i+1] - rr_intervals[i]) for i in range(len(rr_intervals) - 1))
        return sum_abs_diff / (len(rr_intervals) - 1)

    def handle_heart_rate_measurement(self, value: bytearray, time: int) -> HRVResult:
        flags = value[0]
        format_uint16 = (flags & 0x01) != 0
        heart_rate = int.from_bytes(value[1:3], byteorder='little') if format_uint16 else value[1]

        sensor_contact_status = "Sensor contact feature is supported" if (flags & 0x02) != 0 else "Sensor contact feature is not supported"
        energy_expended = int.from_bytes(value[3:5], byteorder='little') if (flags & 0x08) != 0 else None

        rr_intervals = []
        if (flags & 0x10) != 0:
            offset = 2
            for i in range(offset, len(value), 2):
                rr_interval = int.from_bytes(value[i:i+2], byteorder='little')
                rr_intervals.append(int((rr_interval / 1024.0) * 1000.0))

        self.logger.info(f"Raw HR data: flags={flags}, heart_rate={heart_rate}, rr_intervals={rr_intervals}")

        hr_entry = (time, float(heart_rate))
        self.hr_entries.append(hr_entry)

        if rr_intervals:
            hrv = sum(rr_intervals) / len(rr_intervals)
            hrv_entry = (time, float(hrv))
            self.hrv_entries.append(hrv_entry)
        else:
            self.logger.warning("No RR intervals received")

        current_time_seconds = time / 1e9
        self.hr_entries = [entry for entry in self.hr_entries if entry[0] >= current_time_seconds - 5]
        self.hrv_entries = [entry for entry in self.hrv_entries if entry[0] >= current_time_seconds - 60 * 3]

        # self.logger.info(f"HR entries after filtering: {self.hr_entries}")
        # self.logger.info(f"HRV entries after filtering: {self.hrv_entries}")

        average_hrv = sum(entry[1] for entry in self.hrv_entries) / len(self.hrv_entries) if self.hrv_entries else 0
        # self.logger.info(f"Average HRV: {average_hrv}")

        self.hrv_entries = [entry for entry in self.hrv_entries if average_hrv / 2 <= entry[1] <= average_hrv * 2]

        very_recent_hrv_entries = [entry for entry in self.hrv_entries if entry[0] > current_time_seconds - 10]
        somewhat_recent_hrv_entries = [entry for entry in self.hrv_entries if entry[0] > current_time_seconds - 60]
        less_recent_hrv_entries = self.hrv_entries

        # self.logger.info(f"Very recent HRV entries: {very_recent_hrv_entries}")
        # self.logger.info(f"Somewhat recent HRV entries: {somewhat_recent_hrv_entries}")
        # self.logger.info(f"Less recent HRV entries: {less_recent_hrv_entries}")

        hrv_rmssd_very_recent = float(self.calculate_rmssd([entry[1] for entry in very_recent_hrv_entries]))
        hrv_rmssd_somewhat_recent = float(self.calculate_rmssd([entry[1] for entry in somewhat_recent_hrv_entries]))
        hrv_rmssd_less_recent = float(self.calculate_rmssd([entry[1] for entry in less_recent_hrv_entries]))
        hrv_sd_very_recent = float(self.calculate_sd_hrv([entry[1] for entry in very_recent_hrv_entries]))
        hrv_sd_somewhat_recent = float(self.calculate_sd_hrv([entry[1] for entry in somewhat_recent_hrv_entries]))
        hrv_sd_less_recent = float(self.calculate_sd_hrv([entry[1] for entry in less_recent_hrv_entries]))
        hr = float(heart_rate)

        # self.logger.info(f"HRV calculations: rmssd_very_recent={hrv_rmssd_very_recent}, rmssd_somewhat_recent={hrv_rmssd_somewhat_recent}, rmssd_less_recent={hrv_rmssd_less_recent}")
        # self.logger.info(f"HRV calculations: sd_very_recent={hrv_sd_very_recent}, sd_somewhat_recent={hrv_sd_somewhat_recent}, sd_less_recent={hrv_sd_less_recent}")

        self.logger.info(f"HR: {heart_rate} rrIntervals: {rr_intervals} rmssd={hrv_rmssd_very_recent} sdnn={hrv_sd_very_recent}")

        return HRVResult(
            hrv_rmssd_very_recent,
            hrv_rmssd_somewhat_recent,
            hrv_rmssd_less_recent,
            hrv_sd_very_recent,
            hrv_sd_somewhat_recent,
            hrv_sd_less_recent,
            hr,
            rr_intervals
        )

    @staticmethod
    def convert_array_to_signed_int(data: bytearray, offset: int, length: int) -> int:
        byte_arr = data[offset:offset + length]
        result = int.from_bytes(byte_arr, byteorder='little', signed=False)
        shift = 8 * (4 - length)
        return (result << shift) >> shift

    @staticmethod
    def convert_to_unsigned_long(data: bytearray, offset: int, length: int) -> int:
        byte_arr = data[offset:offset + length]
        return int.from_bytes(byte_arr, byteorder='little', signed=False)

    def handle_accelerometer_data(self, data: bytearray, external_timestamp: int) -> PolarAccData:
        if data[0] == 0x02:
            timestamp = int.from_bytes(data[1:9], byteorder='little') / 1e9  # timestamp of the last sample
            frame_type = data[9]
            resolution = (frame_type + 1) * 8  # 16 bit
            time_step = 0.005  # 200 Hz sample rate
            step = math.ceil(resolution / 8)
            samples = data[10:]
            n_samples = math.floor(len(samples) / (step * 3))
            sample_timestamp = timestamp - (n_samples - 1) * time_step
            offset = 0

            while offset < len(samples):
                x = float(int.from_bytes(samples[offset:offset+step], byteorder='little', signed=True))
                offset += step
                y = float(int.from_bytes(samples[offset:offset+step], byteorder='little', signed=True))
                offset += step
                z = float(int.from_bytes(samples[offset:offset+step], byteorder='little', signed=True))
                offset += step

                self.accelerometer_x = x
                self.accelerometer_y = y
                self.accelerometer_z = z
                sample_timestamp += time_step

            position = self.position(x, y, z)
            return PolarAccData(x=x, y=y, z=z, position=position, timestamp=external_timestamp, device_address=self.client.address)
        else:
            self.logger.error(f"Invalid frame type: {data[0]} {data}", exc_info=True)
            return None

    @staticmethod
    def position(x: int, y: int, z: int) -> PositionEnum:
        if z > 800:
            return PositionEnum.LyingBack
        elif y < -600:
            return PositionEnum.LyingRight
        elif y > 600:
            return PositionEnum.LyingLeft
        elif x < -500:
            return PositionEnum.Upright
        return PositionEnum.Unknown


    async def subscribe(self):
        await self.subscribe_for_accelerometer()
        await self.subscribe_for_heart_rate()

    # N.b. accel data only arrives after 30 seconds for some reason
    async def subscribe_for_accelerometer(self):
        await self.client.write_gatt_char(PolarConstants.PMD_CONTROL_UUID, PolarConstants.POLAR_ACC_WRITE, response=True)
        await self.client.start_notify(PolarConstants.PMD_DATA_UUID, self.acc_handler, response=True)


    async def subscribe_for_heart_rate(self):
        await self.client.start_notify(PolarConstants.HEART_RATE_MEASUREMENT_UUID, self.hr_handler, response=True)