from dataclasses import dataclass

class BLECommand:
    async def execute(self, manager):
        pass


@dataclass
class SharedData:
    device_address: str
    timestamp: int