"""
SpinWatch - HDFS & MySQL Data Seeder
Populates local HDFS directory (/data/machines/raw/) with Parquet telemetry files
and inserts initial machine status, readings, predictions, and PySpark insights into MySQL laundry_ops.
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

    # 2. Land Parquet files in HDFS target directory
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

    # Upload to HDFS cluster if hdfs CLI is present
    try:
        cmd = ["hdfs", "dfs", "-mkdir", "-p", f"/data/machines/raw/dt={today_str}"]
        subprocess.run(cmd, capture_output=True)
        target = parquet_path if PARQUET_AVAILABLE else json_path
        cmd_copy = ["hdfs", "dfs", "-put", "-f", target, f"/data/machines/raw/dt={today_str}/"]
        res = subprocess.run(cmd_copy, capture_output=True)
        if res.returncode == 0:
            print(f"[+] Successfully uploaded data into HDFS cluster: /data/machines/raw/dt={today_str}/")
        else:
            print("[!] HDFS cluster upload bypassed (running in local disk mode).")
    except Exception as e:
        print(f"[!] HDFS CLI not accessible ({e}). Data landed in local filesystem at data/machines/raw.")

    # 3. Populate MySQL Database tables
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

            # Seed predictions
            pred_tuples = [(r["machine_id"], r["branch"], r["breakdown_soon"], r["txn_timestamp"]) for r in records[:15]]
            cursor.executemany("""
                REPLACE INTO predictions (machine_id, branch, prediction, scored_at)
                VALUES (%s, %s, %s, %s)
            """, pred_tuples)

            # Seed insights
            cursor.execute("REPLACE INTO insight_avg_temp_by_branch VALUES ('Kigali', 66.50), ('Musanze', 58.20), ('Huye', 52.40), ('Rubavu', 61.10), ('Rusizi', 54.80), ('Nyagatare', 49.30)")
            cursor.execute("REPLACE INTO insight_time_in_alert VALUES ('WM_0004', 'Kigali', 18), ('WM_0001', 'Kigali', 12), ('WM_0006', 'Rusizi', 9)")
            cursor.execute("REPLACE INTO insight_busiest_alert_hour VALUES (14, 28), (15, 22), (11, 19), (16, 15)")

            conn.commit()
            cursor.close()
            conn.close()
            print("[+] Successfully populated MySQL `laundry_ops` tables with initial telemetry!")
        except Exception as e:
            print(f"[!] MySQL seed issue: {e}")

if __name__ == "__main__":
    seed_data()
