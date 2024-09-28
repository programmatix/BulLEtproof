import logging
import paho.mqtt.client as mqtt
import json
import os

class MQTTManager:
    def __init__(self):
        self.client = mqtt.Client()
        self.client.username_pw_set(os.getenv('MQTT_USERNAME'), os.getenv('MQTT_PASSWORD'))
        self.client.connect(os.getenv('MQTT_HOST'), int(os.getenv('MQTT_PORT')))
        self.client.loop_start()
        self.logger = logging.getLogger(__name__)
        
    def publish_data(self, topic, data):
        self.logger.info(f"Publishing data to MQTT: {data}")
        try:
            self.client.publish(topic, json.dumps(data))
        except Exception as e:
            self.logger.error(f"Error publishing to MQTT: {e}", exc_info=True)

    def close(self):
        self.client.loop_stop()
        self.client.disconnect()