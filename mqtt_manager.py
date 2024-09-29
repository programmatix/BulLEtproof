import logging
import paho.mqtt.client as mqtt
import json
import os
import threading
import time

class MQTTManager:
    def __init__(self):
        self.client = mqtt.Client()
        self.client.username_pw_set(os.getenv('MQTT_USERNAME'), os.getenv('MQTT_PASSWORD'))
        self.logger = logging.getLogger(__name__)
        self.connected = False
        self.connect_thread = threading.Thread(target=self._connect_loop, daemon=True)
        self.connect_thread.start()
        
    def _connect_loop(self):
        while True:
            try:
                self.client.connect(os.getenv('MQTT_HOST'), int(os.getenv('MQTT_PORT')))
                self.client.loop_start()
                self.connected = True
                self.logger.info("Connected to MQTT broker")
                break
            except Exception as e:
                self.logger.error(f"Failed to connect to MQTT broker: {e}", exc_info=True)
                time.sleep(5)  # Wait 5 seconds before trying again

    def publish_data(self, topic, data):
        self.logger.info(f"Publishing data to MQTT: {data}")
        if not self.connected:
            self.logger.warning("Not connected to MQTT broker. Message not sent.")
            return
        try:
            self.client.publish(topic, json.dumps(data))
        except Exception as e:
            self.logger.error(f"Error publishing to MQTT: {e}", exc_info=True)

    def close(self):
        if self.connected:
            self.client.loop_stop()
            self.client.disconnect()
        self.connected = False