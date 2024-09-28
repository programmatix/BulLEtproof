from influxdb import InfluxDBClient
import os
import logging

class InfluxManager:
    def __init__(self):
        self.client = InfluxDBClient(
            host=os.getenv('INFLUX_HOST'),
            port=int(os.getenv('INFLUX_PORT')),
            username=os.getenv('INFLUX_USERNAME'),
            password=os.getenv('INFLUX_PASSWORD'),
            database=os.getenv('INFLUX_DATABASE'),
            ssl=True,
            verify_ssl=False
            )
        self.logger = logging.getLogger(__name__)

    def write_data(self, data):
        self.logger.info(f"Writing data to InfluxDB: {data}")
        try:
            self.client.write_points([data])
            self.logger.info(f"Successfully wrote data to InfluxDB: {data}")
        except Exception as e:
            self.logger.error(f"Error writing to InfluxDB: {e}", exc_info=True)
            self.logger.info(f"Failed data point: {data}")
            
    def close(self):
        self.client.close()
        self.logger.info("InfluxDB connection closed")