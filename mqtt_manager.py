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
        self.client.enable_logger(logging.getLogger("paho.mqtt"))
        logging.getLogger("paho.mqtt").setLevel(logging.DEBUG)
        self.client.tls_set_context(None)
        self.client.tls_insecure_set(True)

    def _connect_loop(self):
        while True:
            try:
                if self.connected:
                    time.sleep(5)
                    continue
                self.logger.info("Connecting to MQTT broker")
                self.client.connect(os.getenv('MQTT_HOST'), int(os.getenv('MQTT_PORT')))
                self.client.loop_start()
                self.logger.info("Connected to MQTT broker")
                self.connected = True
            except Exception as e:
                self.logger.error(f"Failed to connect to MQTT broker: {e}", exc_info=True)
                time.sleep(5)  # Wait 5 seconds before trying again

    def publish_data(self, topic, data):
        self.logger.info(f"Publishing data to MQTT: {topic} {data}")
        if not self.connected:
            self.logger.warning("Not connected to MQTT broker. Message not sent.")
            return
        try:
            msg = self.client.publish(topic, json.dumps(data), retain=True)
            self.logger.info(f"Waiting for data to be published: {data} {msg.rc}")
            msg.wait_for_publish()
            self.logger.info(f"Data published: {data}")
        except Exception as e:
            self.logger.error(f"Error publishing to MQTT: {e}", exc_info=True)
            self.connected = False

    def close(self):
        if self.connected:
            self.client.loop_stop()
            self.client.disconnect()
        self.connected = False