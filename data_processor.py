import logging
from queue import Queue
from threading import Thread
import json

from ble_command import SharedData
from core_device import CoreTempData
from polar_device import PolarAccData, PolarHRData
from viatom_device import ViatomData

class DataProcessor:
    def __init__(self, data_queue: Queue[SharedData], influx_manager, mqtt_manager, ble_manager):
        self.data_queue = data_queue
        self.influx_queue = Queue()
        self.mqtt_queue = Queue()
        self.influx_manager = influx_manager
        self.mqtt_manager = mqtt_manager
        self.ble_manager = ble_manager
        self.logger = logging.getLogger(__name__)

    def start(self):
        Thread(target=self.process_data, daemon=True).start()
        Thread(target=self.handle_influx_queue, daemon=True).start()
        Thread(target=self.handle_mqtt_queue, daemon=True).start()

    def process_data(self):
        self.logger.info("Starting data processing")
        while True:
            try:
                data: SharedData = self.data_queue.get()
                self.logger.info(f"Processing data: {data}")

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
                else:
                    self.logger.warning(f"Unknown data type: {data}")
            except Exception as e:
                self.logger.error(f"Error processing data: {e}", exc_info=True)

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
        self.influx_queue.put(influx_data)

    def process_core_for_mqtt(self, core_temp_data: CoreTempData):
        mqtt_data = {
            "temp": core_temp_data.temp
        }
        if core_temp_data.skin_temp is not None:
            mqtt_data["skinTemp"] = core_temp_data.skin_temp
        
        mqtt_message = {
            "topic": "xl/core/temp",
            "payload": json.dumps(mqtt_data)
        }
        self.mqtt_queue.put(mqtt_message)

    def process_viatom_for_influx(self, viatom_data: ViatomData):
        influx_data = {
            "measurement": "android_o2",
            "tags": {
                "model": "Minix",
                "source": "Viatom"
            },
            "fields": {
                "hr": viatom_data.hr,
                "spo2": viatom_data.spo2,
                "pi": viatom_data.perfusion_index,
                "battery": viatom_data.battery,
                "movement": viatom_data.movement
            },
            "time": viatom_data.timestamp
        }
        self.influx_queue.put(influx_data)

    def process_viatom_for_mqtt(self, viatom_data: ViatomData):
        mqtt_data = {
            "o2": viatom_data.spo2,
            "hr": viatom_data.hr,
            "perfusionIndex": viatom_data.perfusion_index,
            "battery": viatom_data.battery,
            "movement": viatom_data.movement
        }
        
        mqtt_message = {
            "topic": "xl/viatom/data",
            "payload": json.dumps(mqtt_data)
        }
        self.mqtt_queue.put(mqtt_message)


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
        self.influx_queue.put(influx_data)

    def process_polar_hr_for_mqtt(self, polar_data: PolarHRData):
        mqtt_data = {
            "hr": polar_data.hr,
            "hrvRMSSDVeryRecent": polar_data.hrv_rmssd_very_recent,
            "hrvRMSSDSomewhatRecent": polar_data.hrv_rmssd_somewhat_recent,
            "hrvRMSSDLessRecent": polar_data.hrv_rmssd_less_recent,
            "hrvSDVeryRecent": polar_data.hrv_sd_very_recent,
            "hrvSDSomewhatRecent": polar_data.hrv_sd_somewhat_recent,
            "hrvSDLessRecent": polar_data.hrv_sd_less_recent,
            "rrIntervals": ','.join(map(str, polar_data.rr_intervals)),
        }
        
        mqtt_message = {
            "topic": "xl/polar/hr",
            "payload": json.dumps(mqtt_data)
        }
        self.mqtt_queue.put(mqtt_message)

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
        self.influx_queue.put(influx_data)

    def process_polar_acc_for_mqtt(self, polar_data: PolarAccData):
        mqtt_data = {
            "x": polar_data.x,
            "y": polar_data.y,
            "z": polar_data.z,
            "position": polar_data.position.value
        }
        
        mqtt_message = {
            "topic": "xl/polar/accelerometer",
            "payload": json.dumps(mqtt_data)
        }
        self.mqtt_queue.put(mqtt_message)

    def handle_influx_queue(self):
        while True:
            try:
                influx_data = self.influx_queue.get()
                self.influx_manager.write_data(influx_data)
            except Exception as e:
                self.logger.error(f"Error writing to InfluxDB: {e}")

    def handle_mqtt_queue(self):
        while True:
            try:
                mqtt_message = self.mqtt_queue.get()
                self.mqtt_manager.publish_data(mqtt_message['topic'], mqtt_message['payload'])
            except Exception as e:
                self.logger.error(f"Error publishing to MQTT: {e}")