import os
from dotenv import load_dotenv

load_dotenv()

class UUIDs:
    VIATOM_DATA = "14839AC4-7D7E-415C-9A42-167340CF2339"
    POLAR_HR = "00002a37-0000-1000-8000-00805f9b34fb"
    POLAR_RR = "00002a39-0000-1000-8000-00805f9b34fb"
    CORE_TEMP = "00002a6e-0000-1000-8000-00805f9b34fb"
    POLAR_ACC = "00002713-0000-1000-8000-00805f9b34fb"

RECONNECT_INTERVAL = 60  # seconds


def get_device_name(address: str) -> str:
    if address == os.getenv('CORE_DEVICE_ADDRESS'): 
        return f"CORE(address={address})"
    elif address == os.getenv('VIATOM_DEVICE_ADDRESS'):
        return f"Viatom(address={address})"
    elif address == os.getenv('POLAR_DEVICE_ADDRESS'):
        return f"Polar(address={address})"

    return address