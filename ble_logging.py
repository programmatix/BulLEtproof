import logging
import os

def get_core_logger():
    return logging.getLogger("core")

def get_viatom_logger():
    return logging.getLogger("viatom")

def get_polar_logger():
    return logging.getLogger("polar")

def get_movesense_logger():
    return logging.getLogger("movesense")

def get_device_logger(device_id):
    if device_id == os.getenv('CORE_DEVICE_ADDRESS'):
        return get_core_logger()
    elif device_id == os.getenv('VIATOM_DEVICE_ADDRESS'):
        return get_viatom_logger()
    elif device_id == os.getenv('POLAR_DEVICE_ADDRESS'):
        return get_polar_logger()
    elif device_id == os.getenv('MOVESENSE_DEVICE_ADDRESS'):
        return get_movesense_logger()
    else:
        return logging.getLogger(__name__ + ".unknown")
