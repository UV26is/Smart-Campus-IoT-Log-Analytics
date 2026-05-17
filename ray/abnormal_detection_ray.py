import csv
import os
import ray

INPUT_FILE = "data/iot_logs.csv"
OUTPUT_FILE = "outputs_ray/abnormal_devices.csv"


@ray.remote
def process_chunk(lines):
    device_stats = {}

    for line in lines:
        row = next(csv.reader([line]))

        if len(row) != 10:
            continue

        timestamp, device_id, building, floor, room, sensor_type, event_type, value, status, battery_level = row

        if timestamp == "timestamp":
            continue

        if device_id not in device_stats:
            device_stats[device_id] = {
                "building": building,
                "low_battery": False,
                "error_count": 0,
                "high_temp_count": 0
            }

        try:
            battery = float(battery_level)
            if battery < 20:
                device_stats[device_id]["low_battery"] = True
        except ValueError:
            pass

        if status == "ERROR":
            device_stats[device_id]["error_count"] += 1

        if sensor_type == "temperature":
            try:
                temp = float(value)
                if temp > 32:
                    device_stats[device_id]["high_temp_count"] += 1
            except ValueError:
                pass

    return device_stats


def split_lines(lines, num_chunks):
    chunk_size = max(1, len(lines) // num_chunks)
    chunks = []

    for i in range(0, len(lines), chunk_size):
        chunks.append(lines[i:i + chunk_size])

    return chunks


def merge_results(partial_results):
    merged = {}

    for partial in partial_results:
        for device_id, stats in partial.items():
            if device_id not in merged:
                merged[device_id] = {
                    "building": stats["building"],
                    "low_battery": False,
                    "error_count": 0,
                    "high_temp_count": 0
                }

            merged[device_id]["low_battery"] = (
                merged[device_id]["low_battery"] or stats["low_battery"]
            )
            merged[device_id]["error_count"] += stats["error_count"]
            merged[device_id]["high_temp_count"] += stats["high_temp_count"]

    return merged


def detect_abnormal_devices(merged):
    results = []

    for device_id, stats in merged.items():
        if stats["low_battery"]:
            results.append((device_id, stats["building"], "low battery"))

        if stats["error_count"] >= 3:
            results.append((device_id, stats["building"], "repeated errors"))

        if stats["high_temp_count"] >= 3:
            results.append((device_id, stats["building"], "repeated high temperature"))

    return sorted(results)


def main():
    ray.init()

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        lines = f.readlines()

    num_chunks = max(2, os.cpu_count() or 2)
    chunks = split_lines(lines, num_chunks)

    result_refs = [process_chunk.remote(chunk) for chunk in chunks]
    partial_results = ray.get(result_refs)

    merged = merge_results(partial_results)
    abnormal_devices = detect_abnormal_devices(merged)

    os.makedirs("outputs_ray", exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("device_id,building,reason\n")
        for device_id, building, reason in abnormal_devices:
            f.write("{},{},{}\n".format(device_id, building, reason))

    print("Ray abnormal-device detection completed.")
    print("Output file:", OUTPUT_FILE)
    print("Number of abnormal device records:", len(abnormal_devices))

    ray.shutdown()


if __name__ == "__main__":
    main()
