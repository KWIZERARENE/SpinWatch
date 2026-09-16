"""
SpinWatch - HDFS & MySQL Data Seeder
Populates local HDFS directory (/data/machines/raw/) with Parquet telemetry files,
writes analytical predictions & insights to HDFS storage (/data/machines/predictions/ & insights/),
and populates MySQL operational tables (machine_status, readings_log).
"""

import os
import json
import uuid
import random
import datetime
import subprocess

try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False

BRANCHES = ["Kigali", "Musanze", "Huye", "Rubavu", "Rusizi", "Nyagatare"]
MACHINES = [f"WM_{i:04d}" for i in range(1, 31)]

MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "laundry_ops"
}

def seed_data():
    print("[*] Starting SpinWatch HDFS & MySQL Data Seeder...")
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    
    # 1. Generate 200 telemetry records across 30 machines
    records = []
    machine_status_rows = []
    
    for i, mid in enumerate(MACHINES):
        branch = random.choice(BRANCHES)
        temp = round(random.uniform(42.0, 78.0), 2)
        status = "ALERT" if temp > 70.0 else "NORMAL"
        breakdown_soon = 1 if temp > 68.0 else 0
        reading_id = str(uuid.uuid4())
        ts = (datetime.datetime.now() - datetime.timedelta(minutes=random.randint(1, 120))).strftime("%Y-%m-%d %H:%M:%S")

        rec = {
            "reading_id": reading_id,
            "machine_id": mid,
            "branch": branch,
            "cycle_temperature": temp,
            "txn_timestamp": ts,
            "status": status,
            "breakdown_soon": breakdown_soon
        }
        records.append(rec)
        machine_status_rows.append((mid, branch, temp, status, ts))

    # 2. Land Parquet files in HDFS raw target directory
    raw_dir = os.path.join(os.getcwd(), "data", "machines", "raw", f"dt={today_str}")
    os.makedirs(raw_dir, exist_ok=True)
    parquet_path = os.path.join(raw_dir, "part-0000.parquet")
    json_path = os.path.join(raw_dir, "part-0000.json")

    if PARQUET_AVAILABLE:
        df = pd.DataFrame(records)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_path)
        print(f"[+] Written {len(records)} Parquet records to local store: {parquet_path}")
    else:
        with open(json_path, "w") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")
        print(f"[+] Written {len(records)} JSON telemetry records to local store: {json_path}")

    # 3. Save Analytical Predictions & Insights to HDFS Analytical Storage (NOT MySQL)
    pred_dir = os.path.join(os.getcwd(), "data", "machines", "predictions")
    insight_dir = os.path.join(os.getcwd(), "data", "machines", "insights")
    os.makedirs(pred_dir, exist_ok=True)
    os.makedirs(insight_dir, exist_ok=True)

    sample_predictions = [
        {"machine_id": "WM_0001", "branch": "Kigali", "prediction": 1, "scored_at": today_str + " 18:00:00"},
        {"machine_id": "WM_0004", "branch": "Kigali", "prediction": 1, "scored_at": today_str + " 18:00:00"},
        {"machine_id": "WM_0006", "branch": "Rusizi", "prediction": 1, "scored_at": today_str + " 18:00:00"},
        {"machine_id": "WM_0012", "branch": "Huye", "prediction": 1, "scored_at": today_str + " 18:00:00"}
    ]
    with open(os.path.join(pred_dir, "latest_predictions.json"), "w") as f:
        json.dump(sample_predictions, f, indent=2)

    sample_insights = {
        "avg_by_branch": [
            {"branch": "Kigali", "avg_temperature": 66.5},
            {"branch": "Rubavu", "avg_temperature": 61.1},
            {"branch": "Musanze", "avg_temperature": 58.2},
            {"branch": "Rusizi", "avg_temperature": 54.8},
            {"branch": "Huye", "avg_temperature": 52.4},
            {"branch": "Nyagatare", "avg_temperature": 49.3}
        ],
        "time_in_alert": [
            {"machine_id": "WM_0004", "branch": "Kigali", "alert_count": 18},
            {"machine_id": "WM_0001", "branch": "Kigali", "alert_count": 12},
            {"machine_id": "WM_0006", "branch": "Rusizi", "alert_count": 9}
        ]
    }
    with open(os.path.join(insight_dir, "latest_insights.json"), "w") as f:
        json.dump(sample_insights, f, indent=2)

    print("[+] Saved analytical predictions & insights directly into HDFS Analytical Store")

    # 4. Populate MySQL Operational Tables ONLY (machine_status, readings_log)
    if MYSQL_AVAILABLE:
        try:
            conn = mysql.connector.connect(**MYSQL_CONFIG)
            cursor = conn.cursor()

            # Seed machine_status
            cursor.executemany("""
                REPLACE INTO machine_status (machine_id, branch, cycle_temperature, status, last_updated)
                VALUES (%s, %s, %s, %s, %s)
            """, machine_status_rows)

            # Seed readings_log
            log_tuples = [(r["reading_id"], r["machine_id"], r["branch"], r["cycle_temperature"], r["txn_timestamp"], r["status"], r["breakdown_soon"]) for r in records]
            cursor.executemany("""
                INSERT IGNORE INTO readings_log (reading_id, machine_id, branch, cycle_temperature, txn_timestamp, status, breakdown_soon)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, log_tuples)

            conn.commit()
            cursor.close()
            conn.close()
            print("[+] Successfully populated MySQL `laundry_ops` operational tables!")
        except Exception as e:
            print(f"[!] MySQL seed issue: {e}")

if __name__ == "__main__":
    seed_data()
