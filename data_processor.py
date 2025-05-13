import logging
from queue import Full, Queue, Empty
from threading import Thread, Lock, Event
import json
import time
from typing import List, Dict, Any, TypeVar, Generic, Deque
from collections import deque

from ble_command import SharedData
from core_device import CoreTempData
from polar_device import PolarAccData, PolarHRData
from viatom_device import ViatomData
from movesense_device import MovesenseBatteryData, MovesenseHRData, MovesenseAccelData

T = TypeVar('T')

class BoundedQueue(Generic[T]):
    """A queue with a maximum size that drops oldest items when full."""
    
    def __init__(self, maxsize: int = 100_000):
        self.queue: Deque[T] = deque(maxlen=maxsize)
        self.maxsize = maxsize
        self.lock = Lock()
        self.not_empty = Event()
        self._size = 0  # Track size to avoid frequent lock acquisition
    
    def put(self, item: T, block: bool = True, timeout: float = None) -> bool:
        """Add an item to the queue, dropping oldest if full."""
        with self.lock:
            self.queue.append(item)
            self._size = len(self.queue)  # Update size under lock
            self.not_empty.set()
            return True
    
    def get(self, block: bool = True, timeout: float = None) -> T:
        """Remove and return an item from the queue."""
        if not block:
            with self.lock:
                if self._size:
                    item = self.queue.popleft()
                    self._size = len(self.queue)
                    return item
                raise Empty("Queue is empty")
        
        if block and timeout is None:
            while True:
                with self.lock:
                    if self._size:
                        item = self.queue.popleft()
                        self._size = len(self.queue)
                        return item
                # Use a shorter timeout to reduce CPU usage
                self.not_empty.wait(timeout=0.5)  # Wait longer to reduce thread synchronization
        else:
            end_time = None if timeout is None else time.time() + timeout
            
            while True:
                with self.lock:
                    if self._size:
                        item = self.queue.popleft()
                        self._size = len(self.queue)
                        return item
                
                if timeout is not None:
                    remaining = end_time - time.time()
                    if remaining <= 0:
                        raise Empty("Queue is empty")
                    wait_time = min(remaining, 0.5)  # Wait longer to reduce thread synchronization
                else:
                    wait_time = 0.5
                
                self.not_empty.wait(timeout=wait_time)
    
    def qsize(self) -> int:
        """Return the approximate size of the queue."""
        return self._size  # Use cached size to avoid lock acquisition
    
    def empty(self) -> bool:
        """Return True if the queue is empty, False otherwise."""
        return self._size == 0  # Use cached size to avoid lock acquisition
    
    def full(self) -> bool:
        """Return True if the queue is full, False otherwise."""
        # This queue is never full in the traditional sense
        # It automatically drops oldest items
        return False
    
class DataProcessor:
    def __init__(self, data_queue: Queue[SharedData], influx_manager, mqtt_manager, ble_manager):
        self.data_queue = data_queue
        self.influx_queue = BoundedQueue[Dict](maxsize=100_000)
        self.mqtt_queue = BoundedQueue[Dict](maxsize=100_000)
        self.influx_manager = influx_manager
        self.mqtt_manager = mqtt_manager
        self.ble_manager = ble_manager
        self.logger = logging.getLogger(__name__)
        self.dropped_influx = 0
        self.dropped_mqtt = 0
        
        # Batch settings
        self.influx_batch_size = 100
        self.mqtt_batch_size = 20
        self.max_batch_wait_time = 1.0  # seconds

    def start(self):
        Thread(target=self.process_data, daemon=True).start()
        Thread(target=self.handle_influx_queue, daemon=True).start()
        Thread(target=self.handle_mqtt_queue, daemon=True).start()

    def process_data(self):
        self.logger.info("Starting data processing")
        while True:
            try:
                data: SharedData = self.data_queue.get()
                
                # Check if data processing is enabled
                from main import component_status
                if not component_status.get("data_processor_active", True):
                    self.logger.debug("Data processor is disabled, skipping processing")
                    continue
                
                self.logger.debug(f"Processing data: {data} num_dropped_influx: {self.dropped_influx} num_dropped_mqtt: {self.dropped_mqtt} influx_queue_size: {self.influx_queue.qsize()} mqtt_queue_size: {self.mqtt_queue.qsize()}")

                self.ble_manager.update_last_data_received(data.device_address)

                if isinstance(data, CoreTempData):
                    self.process_core_for_influx(data)
                    self.process_core_for_mqtt(data)
                elif isinstance(data, ViatomData):
                    self.process_viatom_for_influx(data)
                    self.process_viatom_for_mqtt(data)
                elif isinstance(data, PolarHRData):
                    self.process_polar_hr_for_influx(data)
                    self.process_polar_hr_for_mqtt(data)
                elif isinstance(data, PolarAccData):
                    self.process_polar_acc_for_influx(data)
                    self.process_polar_acc_for_mqtt(data)
                elif isinstance(data, MovesenseHRData): 
                    self.process_movesense_hr_for_influx(data)
                    self.process_movesense_hr_for_mqtt(data)
                elif isinstance(data, MovesenseAccelData):
                    self.process_movesense_accel_for_influx(data)
                    self.process_movesense_accel_for_mqtt(data)
                elif isinstance(data, MovesenseBatteryData):
                    self.process_movesense_battery_for_mqtt(data)
                else:
                    self.logger.warning(f"Unknown data type: {data}")
            except Exception as e:
                self.logger.error(f"Error processing data: {e}", exc_info=True)

    def add_to_influx_queue(self, influx_data: dict):
        try:
            # With BoundedQueue, this will always succeed but may drop oldest items
            self.influx_queue.put(influx_data, block=False)
        except Exception as e:
            self.logger.error(f"Unexpected error adding to influx queue: {e}", exc_info=True)
            self.dropped_influx += 1

    def add_to_mqtt_queue(self, mqtt_data: dict):
        try:
            # With BoundedQueue, this will always succeed but may drop oldest items
            self.mqtt_queue.put(mqtt_data, block=False)
        except Exception as e:
            self.logger.error(f"Unexpected error adding to mqtt queue: {e}", exc_info=True)
            self.dropped_mqtt += 1

    def process_core_for_influx(self, core_temp_data: CoreTempData):
        influx_data = {
            "measurement": "android_temp",
            "tags": {
                "model": "Minix",
            },
            "fields": {
                "temp": core_temp_data.temp,
                "skinTemp": core_temp_data.skin_temp,
                "dataQuality": core_temp_data.quality
            },
            "time": core_temp_data.timestamp
        }
        self.add_to_influx_queue(influx_data)

    def process_core_for_mqtt(self, core_temp_data: CoreTempData):
        mqtt_data = {
            "temp": int(core_temp_data.temp)
        }
        if core_temp_data.skin_temp is not None:
            mqtt_data["skinTemp"] = int(core_temp_data.skin_temp)
        
        mqtt_message = {
            "topic": "xl/core/temp",
            "payload": mqtt_data
        }
        self.add_to_mqtt_queue(mqtt_message)

    def process_viatom_for_influx(self, viatom_data: ViatomData):
        influx_data = {
            "measurement": "android_o2",
            "tags": {
                "model": "Minix",
                "source": "Viatom"
            },
            "fields": {
                "hr": viatom_data.hr,
                "o2": viatom_data.spo2,
                "pi": viatom_data.perfusion_index,
                "battery": viatom_data.battery,
                "movement": viatom_data.movement
            },
            "time": viatom_data.timestamp
        }
        self.add_to_influx_queue(influx_data)

    def process_viatom_for_mqtt(self, viatom_data: ViatomData):
        mqtt_data = {
            "o2": int(viatom_data.spo2),
            "hr": int(viatom_data.hr),
            "perfusionIndex": int(viatom_data.perfusion_index),
            "battery": int(viatom_data.battery),
            "movement": int(viatom_data.movement)
        }
        
        mqtt_message = {
            "topic": "xl/viatom/data",
            "payload": mqtt_data
        }
        self.add_to_mqtt_queue(mqtt_message)


    def process_polar_hr_for_influx(self, polar_data: PolarHRData):
        influx_data = {
            "measurement": "android_hr",
            "tags": {
                "model": "Minix",
                "source": "Polar"
            },
            "fields": {
                "hr": polar_data.hr,
                "hrv": polar_data.hrv_rmssd_very_recent,
                "hrvRMSSDVeryRecent": polar_data.hrv_rmssd_very_recent,
                "hrvRMSSDSomewhatRecent": polar_data.hrv_rmssd_somewhat_recent,
                "hrvRMSSDLessRecent": polar_data.hrv_rmssd_less_recent,
                "hrvSDVeryRecent": polar_data.hrv_sd_very_recent,
                "hrvSDSomewhatRecent": polar_data.hrv_sd_somewhat_recent,
                "hrvSDLessRecent": polar_data.hrv_sd_less_recent,
                "rrIntervals": ','.join(map(str, polar_data.rr_intervals)),
            },
            "time": polar_data.timestamp
        }
        self.add_to_influx_queue(influx_data)

    def process_polar_hr_for_mqtt(self, polar_data: PolarHRData):
        mqtt_data = {
            "hr": int(polar_data.hr),
            "hrvRMSSDVeryRecent": int(polar_data.hrv_rmssd_very_recent),
            "hrvRMSSDSomewhatRecent": int(polar_data.hrv_rmssd_somewhat_recent),
            "hrvRMSSDLessRecent": int(polar_data.hrv_rmssd_less_recent),
            "hrvSDVeryRecent": int(polar_data.hrv_sd_very_recent),
            "hrvSDSomewhatRecent": int(polar_data.hrv_sd_somewhat_recent),
            "hrvSDLessRecent": int(polar_data.hrv_sd_less_recent),
            "rrIntervals": ','.join(map(str, polar_data.rr_intervals)),
        }
        
        mqtt_message = {
            "topic": "xl/polar/hr",
            "payload": mqtt_data
        }
        self.add_to_mqtt_queue(mqtt_message)

    def process_movesense_hr_for_influx(self, movesense_data: MovesenseHRData):
        influx_data = {
            "measurement": "android_hr",
            "tags": {
                "model": "Minix",
                "source": "Movesense"
            },
            "fields": {
                "hr": movesense_data.hr,
                "rrIntervals": ','.join(map(str, movesense_data.rr_intervals)),
            },
            "time": movesense_data.timestamp
        }
        
        # Include HRV in the existing fields if it's valid (not -1)
        if movesense_data.hrv != -1:
            influx_data["fields"]["hrv_int"] = int(movesense_data.hrv)
            
        self.add_to_influx_queue(influx_data)

    def process_movesense_hr_for_mqtt(self, movesense_data: MovesenseHRData):
        mqtt_data = {
            "hr": int(movesense_data.hr),
            "rrIntervals": ','.join(map(str, movesense_data.rr_intervals)),
        }
        
        # Include HRV in MQTT data if it's valid
        if movesense_data.hrv != -1:
            mqtt_data["hrv"] = movesense_data.hrv
        
        mqtt_message = {
            "topic": "xl/polar/hr",
            "payload": mqtt_data
        }
        self.add_to_mqtt_queue(mqtt_message)

    def process_polar_acc_for_influx(self, polar_data: PolarAccData):
        influx_data = {
            "measurement": "android_accel",
            "tags": {
                "model": "Minix",
                "source": "Polar"
            },
            "fields": {
                "x": polar_data.x,
                "y": polar_data.y,
                "z": polar_data.z,
                "position": polar_data.position.value,
            },
            "time": polar_data.timestamp
        }
        self.add_to_influx_queue(influx_data)

    def process_polar_acc_for_mqtt(self, polar_data: PolarAccData):
        mqtt_data = {
            "x": int(polar_data.x),
            "y": int(polar_data.y),
            "z": int(polar_data.z),
            "position": polar_data.position.value
        }
        
        mqtt_message = {
            "topic": "xl/movesense/accelerometer",
            "payload": mqtt_data
        }
        self.add_to_mqtt_queue(mqtt_message)

    def process_movesense_accel_for_influx(self, movesense_data: MovesenseAccelData):
        influx_data = {
            "measurement": "android_accel",
            "tags": {
                "model": "Minix",
                "source": "Movesense"
            },
            "fields": {
                "x": movesense_data.x,
                "y": movesense_data.y,
                "z": movesense_data.z,
                "ts": float(movesense_data.sensor_timestamp)
            },
            "time": movesense_data.timestamp
        }
        self.add_to_influx_queue(influx_data)

    def process_movesense_accel_for_mqtt(self, movesense_data: MovesenseAccelData):
        mqtt_data = {
            "x": int(movesense_data.x),
            "y": int(movesense_data.y),
            "z": int(movesense_data.z),
        }
        
        mqtt_message = {
            "topic": "xl/movesense/accelerometer",
            "payload": mqtt_data
        }
        self.add_to_mqtt_queue(mqtt_message)

    def process_movesense_battery_for_mqtt(self, movesense_data: MovesenseBatteryData):
        mqtt_data = {
            "level": int(movesense_data.level),
        }
        mqtt_message = {
            "topic": "xl/movesense/battery",
            "payload": mqtt_data
        }   
        self.add_to_mqtt_queue(mqtt_message)

    def handle_influx_queue(self):
        while True:
            try:
                # Check if influx manager is active
                from main import component_status
                if not component_status.get("influx_manager_active", True):
                    self.logger.debug("InfluxDB manager is disabled, sleeping")
                    time.sleep(1)
                    continue
                    
                batch: List[Dict[str, Any]] = []
                last_time = time.time()
                
                # Get first item (blocking)
                batch.append(self.influx_queue.get())
                
                # Try to get more items up to batch size or timeout
                while len(batch) < self.influx_batch_size and (time.time() - last_time) < self.max_batch_wait_time:
                    try:
                        # Non-blocking get with timeout
                        item = self.influx_queue.get(block=True, timeout=self.max_batch_wait_time - (time.time() - last_time))
                        batch.append(item)
                    except Empty:
                        # Timeout reached, process what we have
                        break
                
                # Write batch to InfluxDB
                if batch:
                    self.logger.debug(f"Writing {len(batch)} items to InfluxDB")
                    self.influx_manager.write_batch(batch)
            except Exception as e:
                self.logger.error(f"Error writing batch to InfluxDB: {e}", exc_info=True)

    def handle_mqtt_queue(self):
        while True:
            try:
                # Check if mqtt manager is active
                from main import component_status
                if not component_status.get("mqtt_manager_active", True):
                    self.logger.debug("MQTT manager is disabled, sleeping")
                    time.sleep(1)
                    continue
                    
                batch = {}  # Dictionary to store latest message per topic
                last_time = time.time()
                
                # Get first item (blocking)
                first_message = self.mqtt_queue.get()
                topic = first_message['topic']
                batch[topic] = first_message['payload']
                
                # Try to get more items up to batch size or timeout
                while len(batch) < self.mqtt_batch_size and (time.time() - last_time) < self.max_batch_wait_time:
                    try:
                        # Non-blocking get with timeout
                        item = self.mqtt_queue.get(block=True, timeout=self.max_batch_wait_time - (time.time() - last_time))
                        topic = item['topic']
                        # Keep only the latest message for each topic
                        batch[topic] = item['payload']
                    except Empty:
                        # Timeout reached, process what we have
                        break
                
                # Publish latest message for each topic
                for topic, payload in batch.items():
                    self.mqtt_manager.publish_data(topic, payload)
                    self.logger.debug(f"Published latest data to {topic}")
            except Exception as e:
                self.logger.error(f"Error publishing to MQTT: {e}", exc_info=True)
                
    def get_status(self):
        """
        Get the current status of the data processor including queue sizes and dropped messages.
        """
        return {
            "data_queue_size": self.data_queue.qsize(),
            "influx_queue_size": self.influx_queue.qsize(),
            "mqtt_queue_size": self.mqtt_queue.qsize(),
            "dropped_influx_messages": self.dropped_influx,
            "dropped_mqtt_messages": self.dropped_mqtt,
            "influx_batch_size": self.influx_batch_size,
            "mqtt_batch_size": self.mqtt_batch_size,
            "max_batch_wait_time": self.max_batch_wait_time
        }