"""
SpinWatch - Washing Machine Telemetry Stream Generator
Continuous telemetry generator producing machine_id, branch, cycle_temperature, timestamp, status, breakdown_soon.
Monitors cycle_temperature (°C) to track machine performance and heat drift.
"""

import time
import uuid
import random
import argparse
import datetime
import requests

BRANCHES = ["Kigali", "Musanze", "Huye", "Rubavu", "Rusizi", "Nyagatare"]

# Setup 30 washing machines across 6 nationwide branches
MACHINES = [
    {
        "machine_id": f"WM_{i:04d}",
        "branch": random.choice(BRANCHES)
    }
    for i in range(1, 31)
]

# Baseline cycle temperature per machine (°C)
base_temperature = {
    m["machine_id"]: random.uniform(40.0, 55.0) for m in MACHINES
}

def generate_telemetry():
    parser = argparse.ArgumentParser(description="SpinWatch Telemetry Stream Generator")
    parser.add_argument("--tps", type=float, default=3.0, help="Readings per second (default: 3)")
    parser.add_argument("--target-url", type=str, default="http://localhost:8000/api/readings/", help="Target REST API URL")
    args = parser.parse_args()

    print(f"[*] Starting Telemetry Generator at {args.tps} TPS...")
    print(f"[*] Target Endpoint: {args.target_url}")
    print("[*] Press Ctrl+C to stop.")

    while True:
        machine = random.choice(MACHINES)
        mid = machine["machine_id"]

        # Thermal drift simulation
        drift = random.uniform(-0.5, 0.9)
        base_temperature[mid] += drift
        base_temperature[mid] = max(30.0, min(base_temperature[mid], 85.0))

        current_temp = round(base_temperature[mid], 2)
        status = "ALERT" if current_temp > 70.0 else "NORMAL"
        now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()

        reading = {
            "reading_id": str(uuid.uuid4()),
            "machine_id": mid,
            "branch": machine["branch"],
            "cycle_temperature": current_temp,
            "timestamp": now_str,
            "status": status,
            "breakdown_soon": 1 if current_temp > 68.0 else 0
        }

        try:
            resp = requests.post(args.target_url, json=reading, timeout=2.0)
            if resp.status_code not in (200, 201, 202):
                print(f"[!] API HTTP {resp.status_code}: {resp.text}")
        except Exception as e:
            print(f"[!] Delivery issue: {e}")

        time.sleep(1.0 / args.tps)

if __name__ == "__main__":
    generate_telemetry()
