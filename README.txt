# BulLEtproof

BulLEtproof is part of my PSG stack, and is designed to connect to a variety of BLE devices I use, and stream in the data into InfluxDB and MQTT.

It currently supports Viatom CheckMe devices, Polar devices including the H10 I use, and CORE temperature devices.

The software is bespoke to my setup and unlikely to be generally useful; I'm publishing it in case it's a convenient reference for others trying to do similar things.

The name comes because the first iteration of this was an Android app, but the regular struggles with data dropouts lead me to seek a more robust BLE solution.

It uses Python and the Bleak library, and should run easily wherever those do.

