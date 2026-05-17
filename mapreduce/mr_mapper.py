#!/usr/bin/env python3
import sys
import csv

if len(sys.argv) != 2:
    sys.exit("Usage: mr_mapper.py [sensor|building|device]")

mode = sys.argv[1]
reader = csv.reader(sys.stdin)

for row in reader:
    if len(row) != 10:
        continue

    timestamp, device_id, building, floor, room, sensor_type, event_type, value, status, battery_level = row

    if timestamp == "timestamp":
        continue

    if mode == "sensor":
        print("{}\t1".format(sensor_type))

    elif mode == "building":
        if status in ("WARNING", "ERROR"):
            print("{}\t1".format(building))

    elif mode == "device":
        print("{}\t1".format(device_id))
