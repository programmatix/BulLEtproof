import logging
from queue import Queue
from threading import Thread
import json

from core_device import CoreTempData

class DataProcessor:
    def __init__(self, data_queue: Queue, influx_manager, mqtt_manager):
        self.data_queue = data_queue
        self.influx_queue = Queue()
        self.mqtt_queue = Queue()
        self.influx_manager = influx_manager
        self.mqtt_manager = mqtt_manager
        self.logger = logging.getLogger(__name__)

    def start(self):
        Thread(target=self.process_data, daemon=True).start()
        Thread(target=self.handle_influx_queue, daemon=True).start()
        Thread(target=self.handle_mqtt_queue, daemon=True).start()

    def process_data(self):
        while True:
            try:
                core_temp_data = self.data_queue.get()
                self.logger.info(f"Processing data: {core_temp_data}")
                self.process_for_influx(core_temp_data)
                self.process_for_mqtt(core_temp_data)
            except Exception as e:
                self.logger.error(f"Error processing data: {e}")

    def process_for_influx(self, core_temp_data: 'CoreTempData'):
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

    def process_for_mqtt(self, core_temp_data: 'CoreTempData'):
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