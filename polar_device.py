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

@dataclass
class PolarAccData(SharedData):
    x: float
    y: float
    z: float
    position: str

@dataclass
class AccelerometerMeasurement:
    values: List[Tuple[int, int, int]]

class PositionEnum(Enum):
    LyingBack = "LyingBack"
    Upright = "Upright"
    LyingRight = "LyingRight"
    LyingLeft = "LyingLeft"
    Unknown = "Unknown"

@dataclass
class Position:
    pos: PositionEnum
    x: int
    y: int
    z: int

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

class PolarClientManager:
    def __init__(self, client: BleakClient, data_queue: asyncio.Queue, client_id: str):
        self.client = client
        self.data_queue = data_queue
        self.logger = logging.getLogger(f"{__name__}.{client_id}")
        self.client_id = client_id
        self.start_time = time.time()
        self.last_accelerometer_data = None
        self.dead = False
        self.hr_entries = []
        self.hrv_entries = []
        self.logger.info(f"PolarClientManager initialized with client_id: {client_id}")

    def __str__(self):
        return f"PolarClientManager(client_id={self.client_id})"

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
        self.logger.debug(f"Received HR data from Polar device: {data.hex()}")

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
        if self.dead or not self.client.is_connected:
            return

        self.logger.debug(f"Received ACC data from Polar device: {data.hex()}")
        acc_data = self.handle_accelerometer_data(data)
        if acc_data is not None:
            pos = self.position(acc_data.x, acc_data.y, acc_data.z)
        
            now = datetime.now()
            if self.last_accelerometer_data is None or now > self.last_accelerometer_data + timedelta(seconds=5):
                self.last_accelerometer_data = now
                timestamp = int(time.time() * 1e9)
                acc_data = PolarAccData(
                    x=acc_data.x,
                    y=acc_data.y,
                    z=acc_data.z,
                    position=pos,
                    timestamp=timestamp,
                    device_address=self.client.address
                )
                self.data_queue.put(acc_data)

    @staticmethod
    def position(x: int, y: int, z: int) -> Position:
        if z > 800:
            return Position(PositionEnum.LyingBack, x, y, z)
        elif y < -600:
            return Position(PositionEnum.LyingRight, x, y, z)
        elif y > 600:
            return Position(PositionEnum.LyingLeft, x, y, z)
        elif x < -500:
            return Position(PositionEnum.Upright, x, y, z)
        return Position(PositionEnum.Unknown, x, y, z)

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
    def calculate_graham_hrv(rr_intervals: List[int]) -> float:
        if len(rr_intervals) < 2:
            return 0.0
        sum_abs_diff = sum(abs(rr_intervals[i+1] - rr_intervals[i]) for i in range(len(rr_intervals) - 1))
        return sum_abs_diff / (len(rr_intervals) - 1)

    @staticmethod
    def calculate_hrv(rr_intervals: List[int]) -> float:
        if len(rr_intervals) < 2:
            return 0.0
        rmssd = PolarDevice.calculate_rmssd(rr_intervals)
        return math.log(rmssd)

    def handle_ecg_quality_measurement(self, value: bytearray) -> int:
        out = value[0] & 0xff
        self.logger.info(f"ECG Quality: {out}")
        return out

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

        hr_entry = (time, float(heart_rate))
        self.hr_entries.append(hr_entry)

        if rr_intervals:
            hrv = sum(rr_intervals) / len(rr_intervals)
            hrv_entry = (time, float(hrv))
            self.hrv_entries.append(hrv_entry)

        self.hr_entries = [entry for entry in self.hr_entries if entry[0] >= time - 5]
        self.hrv_entries = [entry for entry in self.hrv_entries if entry[0] >= time - 60 * 3]

        average_hrv = sum(entry[1] for entry in self.hrv_entries) / len(self.hrv_entries) if self.hrv_entries else 0
        self.hrv_entries = [entry for entry in self.hrv_entries if average_hrv / 2 <= entry[1] <= average_hrv * 2]

        very_recent_hrv_entries = [entry for entry in self.hrv_entries if entry[0] > time - 10]
        somewhat_recent_hrv_entries = [entry for entry in self.hrv_entries if entry[0] > time - 60]
        less_recent_hrv_entries = self.hrv_entries

        hrv_rmssd_very_recent = float(self.calculate_rmssd([entry[1] for entry in very_recent_hrv_entries]))
        hrv_rmssd_somewhat_recent = float(self.calculate_rmssd([entry[1] for entry in somewhat_recent_hrv_entries]))
        hrv_rmssd_less_recent = float(self.calculate_rmssd([entry[1] for entry in less_recent_hrv_entries]))
        hrv_graham_very_recent = float(self.calculate_graham_hrv([entry[1] for entry in very_recent_hrv_entries]))
        hrv_graham_somewhat_recent = float(self.calculate_graham_hrv([entry[1] for entry in somewhat_recent_hrv_entries]))
        hrv_graham_less_recent = float(self.calculate_graham_hrv([entry[1] for entry in less_recent_hrv_entries]))
        hr = float(sum(entry[1] for entry in self.hr_entries if entry[1] > 0) / len(self.hr_entries)) if self.hr_entries else 0

        self.logger.info(f"HR: {heart_rate} rrIntervals: {rr_intervals} rmssd={hrv_rmssd_very_recent} graham={hrv_graham_very_recent} recent={[entry[1] for entry in very_recent_hrv_entries]}")

        return HRVResult(
            hrv_rmssd_very_recent,
            hrv_rmssd_somewhat_recent,
            hrv_rmssd_less_recent,
            hrv_graham_very_recent,
            hrv_graham_somewhat_recent,
            hrv_graham_less_recent,
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

    def handle_accelerometer_measurement(self, value: bytearray) -> AccelerometerMeasurement:
        flags = value[0] & 0xff
        expected_values = 25

        if len(value) < 1 + (expected_values * 2 * 3):
            raise ValueError("Value must have enough bytes for flags and XYZ values.")

        xyz_values = []
        index = 1  # Start after the flag byte

        # Extract X values
        for _ in range(expected_values):
            x = ((value[index + 1] & 0xFF) << 8) | (value[index] & 0xFF)
            index += 2
            xyz_values.append((x, 0, 0))

        # Extract Y values
        for i in range(expected_values):
            y = ((value[index + 1] & 0xFF) << 8) | (value[index] & 0xFF)
            index += 2
            xyz_values[i] = (xyz_values[i][0], y, xyz_values[i][2])

        # Extract Z values
        for i in range(expected_values):
            z = ((value[index + 1] & 0xFF) << 8) | (value[index] & 0xFF)
            index += 2
            xyz_values[i] = (xyz_values[i][0], xyz_values[i][1], z)

        self.logger.info("Accelerometer data changed")
        return AccelerometerMeasurement(xyz_values)

    def handle_accelerometer_data(self, data: bytearray):
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
                x = int.from_bytes(samples[offset:offset+step], byteorder='little', signed=True)
                offset += step
                y = int.from_bytes(samples[offset:offset+step], byteorder='little', signed=True)
                offset += step
                z = int.from_bytes(samples[offset:offset+step], byteorder='little', signed=True)
                offset += step

                self.accelerometer_x = x
                self.accelerometer_y = y
                self.accelerometer_z = z
                sample_timestamp += time_step
        else:
            self.logger.error(f"Invalid frame type: {frame_type} {data}")

    @staticmethod
    def position(x: int, y: int, z: int) -> Position:
        if z > 800:
            return Position(PositionEnum.LyingBack, x, y, z)
        elif y < -600:
            return Position(PositionEnum.LyingRight, x, y, z)
        elif y > 600:
            return Position(PositionEnum.LyingLeft, x, y, z)
        elif x < -500:
            return Position(PositionEnum.Upright, x, y, z)
        return Position(PositionEnum.Unknown, x, y, z)


    async def subscribe(self):
        await self.subscribe_for_accelerometer()
        await self.subscribe_for_heart_rate()

    async def subscribe_for_accelerometer(self):
        pmd_service = self.client.services.get_service(PolarConstants.PMD_SERVICE_UUID)
        pmd_control = pmd_service.get_characteristic(PolarConstants.PMD_CONTROL_UUID)
        pmd_data = pmd_service.get_characteristic(PolarConstants.PMD_DATA_UUID)

        await self.client.write_gatt_char(pmd_control, PolarConstants.POLAR_ACC_WRITE)
        await self.client.start_notify(PolarConstants.PMD_DATA_UUID, self.acc_handler)

    async def subscribe_for_heart_rate(self):
        hr_service = self.client.services.get_service(PolarConstants.HEART_RATE_SERVICE_UUID)
        hr_characteristic = hr_service.get_characteristic(PolarConstants.HEART_RATE_MEASUREMENT_UUID)
        await self.client.start_notify(PolarConstants.HEART_RATE_MEASUREMENT_UUID, self.hr_handler)