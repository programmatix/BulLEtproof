# -*- coding: utf-8 -*-
"""
Python gatt_sensordata_app client example using the Bleak GATT client.

This example is based on the examples in the Bleak repo: https://github.com/hbldh/bleak
"""

import logging
import asyncio
import platform
import signal
from bleak import BleakClient
from bleak import _logger as logger
from bleak import discover
from functools import reduce
from typing import List
import struct
import sys

WRITE_CHARACTERISTIC_UUID = (
    "34800001-7185-4d5d-b431-630e7050e8f0"
)

NOTIFY_CHARACTERISTIC_UUID = (
    "34800002-7185-4d5d-b431-630e7050e8f0"
)   


# https://stackoverflow.com/a/56243296
class DataView:
    def __init__(self, array, bytes_per_element=1):
        """
        bytes_per_element is the size of each element in bytes.
        By default we are assume the array is one byte per element.
        """
        self.array = array
        self.bytes_per_element = 1

    def __get_binary(self, start_index, byte_count, signed=False):
        integers = [self.array[start_index + x] for x in range(byte_count)]
        _bytes = [integer.to_bytes(
            self.bytes_per_element, byteorder='little', signed=signed) for integer in integers]
        return reduce(lambda a, b: a + b, _bytes)

    def get_uint_16(self, start_index):
        bytes_to_read = 2
        return int.from_bytes(self.__get_binary(start_index, bytes_to_read), byteorder='little')

    def get_uint_8(self, start_index):
        bytes_to_read = 1
        return int.from_bytes(self.__get_binary(start_index, bytes_to_read), byteorder='little')

    def get_uint_32(self, start_index):
        bytes_to_read = 4
        binary = self.__get_binary(start_index, bytes_to_read)
        return struct.unpack('<I', binary)[0]  # <f for little endian

    def get_int_32(self, start_index):
        bytes_to_read = 4
        binary = self.__get_binary(start_index, bytes_to_read)
        return struct.unpack('<i', binary)[0]  # < for little endian

    def get_float_32(self, start_index):
        bytes_to_read = 4
        binary = self.__get_binary(start_index, bytes_to_read)
        return struct.unpack('<f', binary)[0]  # <f for little endian



async def run_queue_consumer(queue: asyncio.Queue):
    while True:
        data = await queue.get()
        if data is None:
            logger.info(
                "Got message from client about disconnection. Exiting consumer loop..."
            )
            break
        else:
            # print to stdout
            print(data)

PACKET_TYPE_DATA = 2
PACKET_TYPE_DATA_PART2 = 3
ongoing_data_update = None

async def run_ble_client( end_of_serial: str, sensor_types: List[str], queue: asyncio.Queue):

    # Check the device is available
    # devices = await discover()
    # found = False
    # address = None
    # #logger.info("devices: %d", len(devices))
    # for d in devices:
    #     logger.info("device: %s", d.name)
    #     if d.name and d.name.endswith(end_of_serial):
    #         logger.info("device found")
    #         address = d.address
    #         found = True
    #         break

    # This event is set if device disconnects or ctrl+c is pressed
    disconnected_event = asyncio.Event()

    def raise_graceful_exit(*args):
        disconnected_event.set()

    async def notification_handler(sender, data):
        d = DataView(data)
        packet_type = d.get_uint_8(0)
        reference = d.get_uint_8(1)

        
        if packet_type == 2 and reference == 102:  # HR data
            timestamp = d.get_uint_32(2)  # 4 bytes for timestamp
            hr_value = d.get_uint_8(6)    # 1 byte for HR value
            rr_interval = d.get_uint_16(7) if len(data) >= 9 else None  # 2 bytes for RR interval
            
            print(f"HR Data: Timestamp={timestamp}, HR={hr_value} BPM, RR={rr_interval} ms")
            return

        print("notification_handler", packet_type, reference, data)
        # return

        global ongoing_data_update
        if packet_type == PACKET_TYPE_DATA:
            if reference == 100:
                timestamp = d.get_uint_32(2)
                for i in range(0, 16):
                    row_timestamp = timestamp + int(i * 1000 / 200)
                    sample_mV = d.get_int_32(6 + i * 4) * 0.38 * 0.001
                    msg_row = "ECG,{},{:.3f}".format(row_timestamp, sample_mV)
                    await queue.put(msg_row)
            elif reference == 101:
                ongoing_data_update = d
            elif reference == 102:
                hr_value = d.get_uint_8(2)
                rr_interval = d.get_uint_16(3) if len(d.array) > 4 else None
                msg_row = "HR,{},{},{}".format(d.get_uint_32(2), hr_value, rr_interval)
                await queue.put(msg_row)
            else:
                ongoing_data_update = d
        elif packet_type == PACKET_TYPE_DATA_PART2 and ongoing_data_update and ongoing_data_update.get_uint_8(1) == 101:
            combined_data = DataView(ongoing_data_update.array + data[2:])
            timestamp = combined_data.get_uint_32(2)
            for i in range(0, 8):
                row_timestamp = timestamp + int(i * 1000 / 13)
                offset = 6 + i * 3 * 4
                msg_row = "ACC,{},{:.2f},{:.2f},{:.2f}".format(
                    row_timestamp,
                    combined_data.get_float_32(offset),
                    combined_data.get_float_32(offset + 4),
                    combined_data.get_float_32(offset + 8)
                )
                await queue.put(msg_row)
            ongoing_data_update = None

    async def reconnect_and_subscribe(client, sensor_types):
        await asyncio.sleep(2)  # Wait for 2 seconds
        try:
            # Reconnect
            await client.connect()
            logger.info("Reconnected successfully!")

            # Re-subscribe to notifications
            await client.start_notify(NOTIFY_CHARACTERISTIC_UUID, notification_handler)

            # Re-subscribe to sensor data streams
            if "ECG" in sensor_types:
                await client.write_gatt_char(WRITE_CHARACTERISTIC_UUID, bytearray([1, 100]) + bytearray("/Meas/ECG/200", "utf-8"), response=True)
            if "IMU9" in sensor_types:
                await client.write_gatt_char(WRITE_CHARACTERISTIC_UUID, bytearray([1, 99]) + bytearray("/Meas/IMU9/104", "utf-8"), response=True)
            if "Acc" in sensor_types:
                await client.write_gatt_char(WRITE_CHARACTERISTIC_UUID, bytearray([1, 101]) + bytearray("/Meas/Acc/13", "utf-8"), response=True)
            if "HR" in sensor_types:
                await client.write_gatt_char(WRITE_CHARACTERISTIC_UUID, bytearray([1, 102]) + bytearray("/Meas/HR", "utf-8"), response=True)

        except Exception as e:
            logger.error(f"Failed to reconnect or re-subscribe: {e}")

    def disconnect_callback(client):
        logger.info("Disconnected callback called!")
        # disconnected_event.set()
        # asyncio.create_task(reconnect_and_subscribe(client, sensor_types))

    # address = "74-92-BA-10-E2-D6"
    address = "74:92:BA:10:E2:D6"
    if True:
        async with BleakClient(address, disconnected_callback=disconnect_callback) as client:
            # Known BLE service and characteristic UUIDs with human-readable names
            known_services = {
                "0000180d-0000-1000-8000-00805f9b34fb": "Heart Rate",
                "0000180a-0000-1000-8000-00805f9b34fb": "Device Information",
                "34800000-7185-4d5d-b431-630e7050e8f0": "Movesense Custom",
            }
            known_characteristics = {
                "00002a37-0000-1000-8000-00805f9b34fb": "Heart Rate Measurement",
                "00002a29-0000-1000-8000-00805f9b34fb": "Manufacturer Name String",
                "34800001-7185-4d5d-b431-630e7050e8f0": "Movesense Write",
                "34800002-7185-4d5d-b431-630e7050e8f0": "Movesense Notify",
            }

            # List all services and characteristics with properties and labels
            # services = await client.get_services()
            # for service in services:
            #     service_name = known_services.get(service.uuid, "Unknown Service")
            #     logger.info(f"Service UUID: {service.uuid} ({service_name})")
            #     for char in service.characteristics:
            #         char_name = known_characteristics.get(char.uuid, "Unknown Characteristic")
            #         properties = []
            #         if "read" in char.properties:
            #             properties.append("readable")
            #         if "write" in char.properties:
            #             properties.append("writable")
            #         if "notify" in char.properties:
            #             properties.append("notifiable")
            #         if "indicate" in char.properties:
            #             properties.append("indicate")
            #         logger.info(f"  Characteristic UUID: {char.uuid} ({char_name})")
            #         logger.info(f"    Properties: {', '.join(properties)}")

            # Add signal handler for ctrl+c
            signal.signal(signal.SIGINT, raise_graceful_exit)
            signal.signal(signal.SIGTERM, raise_graceful_exit)

            try:
                logger.info("Enabling notifications")
                await client.start_notify(NOTIFY_CHARACTERISTIC_UUID, notification_handler)
            except Exception as e:
                logger.error(f"Failed to enable notifications: {e}")
                await queue.put(None)
                return

            # Send HELLO command and log the response
            # logger.info("Sending HELLO command")
            # hello_command = bytearray([0, 0])
            # await client.write_gatt_char(WRITE_CHARACTERISTIC_UUID, hello_command, response=True)
            # logger.info("HELLO command sent. Waiting for response...")



            # # Rest of the existing code for subscribing to datastream
            # logger.info("Subscribing datastream")
            if "ECG" in sensor_types:
                await client.write_gatt_char(WRITE_CHARACTERISTIC_UUID, bytearray([1, 100]) + bytearray("/Meas/ECG/200", "utf-8"), response=True)
            if "IMU9" in sensor_types:
                await client.write_gatt_char(WRITE_CHARACTERISTIC_UUID, bytearray([1, 99]) + bytearray("/Meas/IMU9/104", "utf-8"), response=True)
            if "Acc" in sensor_types:
                await client.write_gatt_char(WRITE_CHARACTERISTIC_UUID, bytearray([1, 101]) + bytearray("/Meas/Acc/13", "utf-8"), response=True)
            if "HR" in sensor_types:
                await client.write_gatt_char(WRITE_CHARACTERISTIC_UUID, bytearray([1, 102]) + bytearray("/Meas/HR", "utf-8"), response=True)

            # Run until disconnect event is set
            await disconnected_event.wait()
            logger.info("Disconnect set by ctrl+c or real disconnect event. Check Status:")

            # Check the connection status to infer if the device disconnected or ctrl+c was pressed
            status = client.is_connected
            logger.info("Connected: {}".format(status))

            # Block until ctrl-c is pressed
            await disconnected_event.wait()
            
            # If status is connected, unsubscribe and stop notifications
            if status:
                logger.info("Unsubscribe")
                await client.write_gatt_char(WRITE_CHARACTERISTIC_UUID, bytearray([2, 99]), response=True)
                await client.write_gatt_char(WRITE_CHARACTERISTIC_UUID, bytearray([2, 100]), response=True)
                await client.write_gatt_char(WRITE_CHARACTERISTIC_UUID, bytearray([2, 101]), response=True)
                await client.write_gatt_char(WRITE_CHARACTERISTIC_UUID, bytearray([2, 102]), response=True)
                logger.info("Stop notifications")
                await client.stop_notify(NOTIFY_CHARACTERISTIC_UUID)
            
            # Signal consumer to exit
            await queue.put(None)
            
            await asyncio.sleep(1.0)

    else:
        # Signal consumer to exit
        await queue.put(None)
        print("Sensor  ******" + end_of_serial, "not found!")



async def main(end_of_serial: str, sensor_types: List[str]):

    queue = asyncio.Queue()

    client_task = run_ble_client(end_of_serial, sensor_types, queue)
    consumer_task = run_queue_consumer(queue)
    await asyncio.gather(client_task, consumer_task)
    logger.info("Main method done.")

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    # print usage if command line arg not given
    if len(sys.argv)<2:
        print("Usage: python movesense_sensor_data <end_of_sensor_name> <sensor_type>")
        print("sensor_type must be either 'IMU9', 'ECG', or omitted to run both")
        exit(1)
    end_of_serial = sys.argv[1]
    sensor_type = sys.argv[2] if len(sys.argv) > 2 else ""
    

    sensor_types = [sensor_type]
    # Ensure valid sensor type and run the corresponding function
    # if sensor_type == "":
    #     sensor_types = ["IMU9", "ECG"]
    # elif sensor_type in ["IMU9", "ECG"]:
    #     sensor_types = [sensor_type]
    # else:
    #     print("Error: sensor_type must be either 'IMU9' or 'ECG'")
    #     exit(1)
    asyncio.run(main(end_of_serial, sensor_types))