#!/usr/bin/env python3
"""
SpinWatch - MapReduce Mapper (Heat Monitoring)
Reads JSON formatted machine telemetry lines from standard input.
Outputs tab-separated (branch, 1) tuples for any reading where cycle_temperature > 70.0°C.
"""

import sys
import json

def mapper():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
            branch = record.get("branch", "Unknown")
            temp = float(record.get("cycle_temperature", 0.0))
            
            # Filter for heat alert threshold
            if temp > 70.0:
                print(f"{branch}\t1")
        except Exception:
            continue

if __name__ == "__main__":
    mapper()
