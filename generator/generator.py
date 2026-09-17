"""
SpinWatch - Washing Machine Telemetry Stream Generator (High Velocity & Variety)
Simulates continuous multi-sensor telemetry stream for 1,000 washing machines across 13 branches.
Demonstrates Big Data Velocity & Variety (Temperature, Vibration, Water Pressure, Power, Error Codes).
Pushes telemetry to REST Framework Ingress API -> Apache Kafka topic 'machine-readings'.
"""

import time
import uuid
import random
import argparse
import datetime
import requests

BRANCHES = [
    "Kigali", "Musanze", "Huye", "Rubavu", "Rusizi", "Nyagatare",
    "Rwamagana", "Gicumbi", "Kamembe", "Karongi", "Nyanza", "Bugesera", "Kamonyi"
]

ERROR_CODES = ["NONE", "NONE", "NONE", "NONE", "E01_OVERHEAT", "E02_VIBRATION", "E03_PRESSURE_DROP", "E04_POWER_SURGE"]

MACHINES = [
    {
        "machine_id": f"WM_{i:04d}",
        "branch": random.choice(BRANCHES)
    }
    for i in range(1, 1001)
]

def generate_telemetry():
    parser = argparse.ArgumentParser(description="SpinWatch Telemetry Stream Generator")
    parser.add_argument("--tps", type=float, default=5.0, help="Readings per second (default: 5)")
    parser.add_argument("--target-url", type=str, default="http://localhost:8000/api/readings/", help="Target REST API URL")
    args = parser.parse_args()

    print(f"[*] Starting Multi-Sensor Generator for 1,000 Washers across 13 Branches at {args.tps} TPS...")
    print(f"[*] Data Ingress Target (Django/REST): {args.target_url}")
    print("[*] Press Ctrl+C to stop.")

    while True:
        machine = random.choice(MACHINES)
        mid = machine["machine_id"]

        # Varied sensor metrics demonstrating Big Data Variety
        current_temp = round(random.uniform(30.0, 102.0), 2)
        vibration_hz = round(random.uniform(12.0, 115.0), 1)
        power_kw = round(random.uniform(1.2, 7.8), 2)
        water_pressure_bar = round(random.uniform(1.0, 4.8), 2)
        error_code = random.choice(ERROR_CODES) if current_temp > 70.0 else "NONE"

        status = "ALERT" if (current_temp > 70.0 or vibration_hz > 90.0) else "NORMAL"
        breakdown_soon = 1 if (current_temp > 70.0 or vibration_hz > 90.0) else 0
        now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()

        reading = {
            "reading_id": str(uuid.uuid4()),
            "machine_id": mid,
            "branch": machine["branch"],
            "cycle_temperature": current_temp,
            "vibration_hz": vibration_hz,
            "power_kw": power_kw,
            "water_pressure_bar": water_pressure_bar,
            "error_code": error_code,
            "timestamp": now_str,
            "status": status,
            "breakdown_soon": breakdown_soon
        }

        try:
            resp = requests.post(args.target_url, json=reading, timeout=2.0)
            if resp.status_code not in (200, 201, 202):
                print(f"[!] REST Ingress API HTTP {resp.status_code}: {resp.text}")
        except Exception as e:
            print(f"[!] Telemetry delivery warning: {e}")

        time.sleep(1.0 / args.tps)

if __name__ == "__main__":
    generate_telemetry()
