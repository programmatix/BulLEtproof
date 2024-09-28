import logging
from dataclasses import dataclass

from constants import UUIDs

@dataclass
class ViatomData:
    hr: int
    spo2: int
    pi: float

class ViatomDevice:
    def __init__(self, client):
        self.client = client
        self.logger = logging.getLogger(__name__)

    async def subscribe(self):
        self.logger.info(f"Subscribing to Viatom device {self.client.address}")
        await self.client.start_notify(UUIDs.VIATOM_DATA, self.data_handler)

    def data_handler(self, sender, data):
        self.logger.debug(f"Received data from Viatom device {sender}: {data.hex()}")
        processed_data = self.process_data(data)
        self.logger.info(f"Processed Viatom data: {processed_data}")
        return processed_data

    def process_data(self, data):
        hr = data[8]
        spo2 = data[7]
        pi = data[17]
        battery = data[14]
        movement = data[16]
        return ViatomData(hr=hr, spo2=spo2, pi=pi, battery=battery, movement=movement)