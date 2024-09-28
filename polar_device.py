import logging
from dataclasses import dataclass

from constants import UUIDs

@dataclass
class PolarHRData:
    hr: int

@dataclass
class PolarRRData:
    rr: int

@dataclass
class PolarAccData:
    x: float
    y: float
    z: float

class PolarDevice:
    def __init__(self, client):
        self.client = client
        self.logger = logging.getLogger(__name__)

    async def subscribe(self):
        self.logger.info(f"Subscribing to Polar device {self.client.address}")
        await self.client.start_notify(UUIDs.POLAR_HR, self.hr_handler)
        await self.client.start_notify(UUIDs.POLAR_RR, self.rr_handler)
        await self.client.start_notify(UUIDs.POLAR_ACC, self.acc_handler)

    def acc_handler(self, sender, data):
        self.logger.debug(f"Received ACC data from Polar device {sender}: {data.hex()}")
        processed_data = self.process_acc_data(data)
        self.logger.info(f"Processed Polar ACC data: {processed_data}")
        return processed_data

    def process_acc_data(self, data):
        # Assuming the acceleration data is in 16-bit integers
        x = int.from_bytes(data[0:2], byteorder='little', signed=True) / 1000
        y = int.from_bytes(data[2:4], byteorder='little', signed=True) / 1000
        z = int.from_bytes(data[4:6], byteorder='little', signed=True) / 1000
        self.logger.debug(f"Processed Polar ACC data: {x}, {y}, {z}")
        return PolarAccData(x=x, y=y, z=z)

    def hr_handler(self, sender, data):
        self.logger.debug(f"Received HR data from Polar device {sender}: {data.hex()}")
        processed_data = self.process_hr_data(data)
        self.logger.info(f"Processed Polar HR data: {processed_data}")
        return processed_data

    def rr_handler(self, sender, data):
        self.logger.debug(f"Received RR data from Polar device {sender}: {data.hex()}")
        processed_data = self.process_rr_data(data)
        self.logger.info(f"Processed Polar RR data: {processed_data}")
        return processed_data

    def process_hr_data(self, data):
        flags = data[0]
        hr = data[1]
        rr_intervals = []
        if flags & 0x10:  # Check if RR interval data is present
            i = 2
            while i < len(data):
                rr = int.from_bytes(data[i:i+2], byteorder='little')
                rr_intervals.append(rr)
                i += 2
        return PolarHRData(hr=hr, rr_intervals=rr_intervals)

    def process_rr_data(self, data):
        rr_interval = int.from_bytes(data[0:2], byteorder='little')
        return PolarRRData(rr=rr_interval)