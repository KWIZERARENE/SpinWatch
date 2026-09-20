"""
SpinWatch - HDFS & MySQL Data Seeder (1,000 Washers / 13 Branches)
Populates local HDFS directory (/data/machines/raw/dt=YYYY-MM-DD/) with Parquet telemetry files
and inserts initial machine status, readings, predictions, and PySpark insights.
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

BRANCHES = [
    "Kigali", "Musanze", "Huye", "Rubavu", "Rusizi", "Nyagatare",
    "Rwamagana", "Gicumbi", "Kamembe", "Karongi", "Nyanza", "Bugesera", "Kamonyi"
]


def build_machine_catalog(size=1000):
    """Generate 1,000 unique random washers instead of an incrementing WM_0001..WM_1000 sequence."""
    machine_numbers = random.sample(range(1, 5000), size)
    return [f"WM_{machine_no:04d}" for machine_no in machine_numbers]


MACHINES = build_machine_catalog(1000)

MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "laundry_ops"
}

def seed_data():
    print("[*] Starting SpinWatch HDFS & MySQL Data Seeder (1,000 Washers / 13 Branches)...")
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    yesterday_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    
    # 1. Generate 1,000 telemetry records across 13 branches
    records = []
    machine_status_rows = []
    
    for i, mid in enumerate(MACHINES):
        branch = random.choice(BRANCHES)
        # Realistic, wide variation across the 1,000-machine fleet, no sequential numbering.
        base_temp = random.gauss(58.0, 19.0)
        if random.random() < 0.28:
            base_temp += random.uniform(10.0, 30.0)
        temp = round(max(25.0, min(110.0, base_temp)), 2)

        status = "ALERT" if temp > 70.0 else "NORMAL"
        breakdown_soon = 1 if temp > 68.0 else 0
        reading_id = str(uuid.uuid4())
        ts = (datetime.datetime.now() - datetime.timedelta(minutes=random.randint(1, 300))).strftime("%Y-%m-%d %H:%M:%S")

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
        machine_status_rows.append((mid, reading_id, branch, temp, status, breakdown_soon, ts))

    # 2. Land Parquet files in HDFS raw target directory (Today & Historical Yesterday)
    for date_tag in [today_str, yesterday_str]:
        raw_dir = os.path.join(os.getcwd(), "data", "machines", "raw", f"dt={date_tag}")
        os.makedirs(raw_dir, exist_ok=True)
        parquet_path = os.path.join(raw_dir, "part-0000.parquet")
        json_path = os.path.join(raw_dir, "part-0000.json")

        if PARQUET_AVAILABLE:
            df = pd.DataFrame(records)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, parquet_path)
            print(f"[+] Written {len(records)} Parquet records to HDFS store: {parquet_path}")
        else:
            with open(json_path, "w") as f:
                for r in records:
                    f.write(json.dumps(r) + "\n")
            print(f"[+] Written {len(records)} JSON telemetry records to HDFS store: {json_path}")

    # 3. Save Analytical Predictions & Insights to HDFS Analytical Storage
    pred_dir = os.path.join(os.getcwd(), "data", "machines", "predictions")
    insight_dir = os.path.join(os.getcwd(), "data", "machines", "insights")
    os.makedirs(pred_dir, exist_ok=True)
    os.makedirs(insight_dir, exist_ok=True)

    # Save predictions for all 1,000 machines (0 = Good Condition, 1 = Breakdown Risk)
    sample_predictions = [
        {"machine_id": rec["machine_id"], "branch": rec["branch"], "prediction": rec["breakdown_soon"], "scored_at": today_str + " 18:00:00"}
        for rec in records
    ]
    with open(os.path.join(pred_dir, "latest_predictions.json"), "w") as f:
        json.dump(sample_predictions, f, indent=2)

    # Compute branch averages across 13 branches
    branch_totals = {}
    branch_counts = {}
    good_condition_count = 0
    normal_temp_count = 0

    for r in records:
        b = r["branch"]
        branch_totals[b] = branch_totals.get(b, 0.0) + r["cycle_temperature"]
        branch_counts[b] = branch_counts.get(b, 0) + 1
        if 30.0 <= r["cycle_temperature"] <= 70.0:
            normal_temp_count += 1
        if r["breakdown_soon"] == 0:
            good_condition_count += 1

    avg_by_branch = [
        {"branch": b, "avg_temperature": round(branch_totals[b] / branch_counts[b], 2)}
        for b in branch_totals
    ]
    avg_by_branch.sort(key=lambda x: x["avg_temperature"], reverse=True)

    hour_counts = {}
    for r in records:
        if r["status"] == "ALERT":
            try:
                hr = int(r["txn_timestamp"].split(" ")[1].split(":")[0])
                hour_counts[hr] = hour_counts.get(hr, 0) + 1
            except Exception:
                pass

    busiest_hour = [
        {"hour": hr, "alert_count": cnt}
        for hr, cnt in sorted(hour_counts.items(), key=lambda x: x[1], reverse=True)
    ]
    time_in_alert = [
        {"machine_id": r["machine_id"], "branch": r["branch"], "alert_count": random.randint(5, 25)}
        for r in records if r["status"] == "ALERT"
    ][:15]

    sample_insights = {
        "avg_by_branch": avg_by_branch,
        "time_in_alert": time_in_alert,
        "busiest_hour": busiest_hour,
        "good_condition_count": good_condition_count,
        "normal_temp_count": normal_temp_count,
        "total_machines": len(records)
    }
    with open(os.path.join(insight_dir, "latest_insights.json"), "w") as f:
        json.dump(sample_insights, f, indent=2)

    print(f"[+] Saved HDFS predictions ({good_condition_count} Good Condition) & PySpark insights to analytical store.")

    # Populate SQLite Fallback Mirror
    try:
        import sqlite3
        sqlite_db = os.path.join(os.getcwd(), "data", "laundry_ops.sqlite3")
        s_conn = sqlite3.connect(sqlite_db)
        s_cur = s_conn.cursor()
        s_cur.execute("""
            CREATE TABLE IF NOT EXISTS machine_status (
                machine_id TEXT PRIMARY KEY,
                reading_id TEXT,
                branch TEXT,
                cycle_temperature REAL,
                status TEXT,
                breakdown_soon INTEGER DEFAULT 0,
                last_updated TEXT
            )
        """)
        s_cur.execute("""
            CREATE TABLE IF NOT EXISTS readings_log (
                reading_id TEXT PRIMARY KEY,
                machine_id TEXT,
                branch TEXT,
                cycle_temperature REAL,
                txn_timestamp TEXT,
                status TEXT,
                breakdown_soon INTEGER DEFAULT 0
            )
        """)
        s_cur.executemany("""
            INSERT OR REPLACE INTO machine_status (machine_id, reading_id, branch, cycle_temperature, status, breakdown_soon, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [(r["machine_id"], r["reading_id"], r["branch"], r["cycle_temperature"], r["status"], r["breakdown_soon"], r["txn_timestamp"]) for r in records])
        s_cur.executemany("""
            INSERT OR IGNORE INTO readings_log (reading_id, machine_id, branch, cycle_temperature, txn_timestamp, status, breakdown_soon)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [(r["reading_id"], r["machine_id"], r["branch"], r["cycle_temperature"], r["txn_timestamp"], r["status"], r["breakdown_soon"]) for r in records])
        s_conn.commit()
        s_conn.close()
        print(f"[+] Successfully populated SQLite fallback mirror at {sqlite_db}")
    except Exception as sqle:
        print(f"[!] SQLite seed warning: {sqle}")

    # 4. Populate MySQL Operational Tables (machine_status, readings_log)
    if MYSQL_AVAILABLE:
        try:
            conn = mysql.connector.connect(**MYSQL_CONFIG)
            cursor = conn.cursor()

            # Seed machine_status
            cursor.executemany("""
                REPLACE INTO machine_status (machine_id, reading_id, branch, cycle_temperature, status, breakdown_soon, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
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
            print(f"[+] Successfully populated MySQL `laundry_ops` with {len(machine_status_rows)} washers!")
        except Exception as e:
            print(f"[!] MySQL seed issue: {e}")

if __name__ == "__main__":
    seed_data()
