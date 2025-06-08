# BulLEtproof

BulLEtproof is part of my PSG stack, and is designed to connect to a variety of BLE devices I use, and stream in the data into InfluxDB and MQTT.

It currently supports Viatom CheckMe devices, Polar devices including the H10 I use, and CORE temperature devices.

The software is bespoke to my setup and unlikely to be generally useful; I'm publishing it in case it's a convenient reference for others trying to do similar things.

The name comes because the first iteration of this was an Android app, but the regular struggles with data dropouts lead me to seek a more robust BLE solution.

It uses Python and the Bleak library, and should run easily wherever those do.

## Bluetooth Adapter Stuck Issue Fix

If the Bluetooth adapter gets stuck in discovery mode (causing "Operation already in progress" errors), you can set up a daily restart of the Bluetooth service using systemd:

### Setup Instructions

1. Copy the systemd files to the system directory:
   ```bash
   sudo cp bluetooth-restart.service /etc/systemd/system/
   sudo cp bluetooth-restart.timer /etc/systemd/system/
   ```

2. Reload systemd and enable the timer:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable bluetooth-restart.timer
   sudo systemctl start bluetooth-restart.timer
   ```

3. Verify the timer is active:
   ```bash
   systemctl status bluetooth-restart.timer
   systemctl list-timers bluetooth-restart.timer
   ```

The Bluetooth service will now restart daily at 12 PM (noon). You can modify the time by editing the `OnCalendar` line in `bluetooth-restart.timer`.

**Note**: This will briefly disconnect all Bluetooth devices when it runs.

