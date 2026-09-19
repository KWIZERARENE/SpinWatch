# SpinWatch — Complete Line-by-Line Code Documentation

This document provides a **comprehensive, line-by-line technical breakdown** of every source file in the SpinWatch project repository. Each code block is shown exactly as it appears in the source, followed by a detailed explanation of what every line does and why.

---

## Project Directory Structure

```
SpinWatch/                                    ← Project Root
│
├── README.md                                 ← Full project documentation & API reference
├── requirements.txt                          ← Python dependencies (requests, kafka-python, mysql-connector, pyspark, django, drf)
├── run_project.py                            ← Master runner — launches all 6 services in one command
├── SpinWatch_Postman_Collection.json         ← Postman API collection for testing all endpoints
├── init_hdfs_dirs.bat                        ← Windows batch script to create HDFS directories
├── start_spinwatch_infra.bat                 ← Infrastructure startup batch script
│
├── sql/                                      ← MySQL Database Schema
│   └── 01_schema.sql                         ← CREATE DATABASE + CREATE TABLE statements with indexes
│
├── generator/                                ← Telemetry Stream Simulator
│   └── generator.py                          ← Generates continuous IoT sensor readings for 3,000 washers
│
├── producer/                                 ← REST Ingress API + Kafka Producer
│   ├── app.py                                ← HTTP server on Port 8000 (11 inspection endpoints)
│   └── producer.py                           ← Kafka producer wrapper class (keyed by machine_id)
│
├── consumer/                                 ← Kafka Consumer + MySQL/HDFS Writer
│   └── consumer.py                           ← Reads Kafka, writes MySQL + HDFS, fills live buffer
│
├── connect/                                  ← Kafka Connect Sink Configurations
│   ├── mysql-sink.json                       ← MySQL sink connector config
│   └── hdfs-sink.json                        ← HDFS sink connector config
│
├── spark/                                    ← PySpark Analytics & Machine Learning
│   ├── insights.py                           ← Batch analytics: branch averages, alert frequency, hourly peaks
│   └── train_model.py                        ← MLlib LogisticRegression failure prediction model trainer
│
├── dashboard/                                ← Web Dashboard UI (Port 8050)
│   ├── app.py                                ← Dashboard HTTP server + REST API endpoints
│   ├── templates/
│   │   └── index.html                        ← Main dashboard HTML (cards, tables, charts, inspectors)
│   └── static/
│       └── css/
│           └── style.css                     ← Dashboard theme CSS (light/dark mode)
│
├── api/                                      ← Django REST Framework API Browser (Port 8001)
│   ├── manage.py                             ← Django CLI entry point
│   ├── spinwatch_api/
│   │   ├── __init__.py                       ← Package init
│   │   ├── settings.py                       ← Django + DRF configuration
│   │   ├── urls.py                           ← Root URL routing → /api/
│   │   └── wsgi.py                           ← WSGI application entry point
│   └── endpoints/
│       ├── __init__.py                       ← Package init
│       ├── views.py                          ← 12 DRF APIView classes (all SpinWatch endpoints)
│       └── urls.py                           ← URL → View mapping
│
├── scripts/                                  ← Data Seeding & HDFS Upload Utilities
│   ├── seed_hdfs_data.py                     ← Generates initial historical data (1,000 records)
│   └── upload_to_hdfs.py                     ← Uploads local data/ to HDFS cluster
│
├── data/                                     ← Local HDFS Mirror (Auto-created)
│   └── machines/
│       ├── raw/
│       │   └── dt=YYYY-MM-DD/                ← Date-partitioned raw readings (JSON Lines)
│       │       └── stream_history.json
│       ├── insights/
│       │   └── latest_insights.json          ← PySpark computed analytics
│       └── predictions/
│           └── latest_predictions.json       ← MLlib breakdown predictions
│
└── docs/                                     ← Documentation
    ├── CODE_LINE_BY_LINE.md                  ← This file — complete code walkthrough
    └── TEAM_PRESENTATION_GUIDE.md            ← Live demo script for team presentations
```

---

## 1. Database Schema (`sql/01_schema.sql`)

### Lines 1–5: Database Creation

```sql
-- SpinWatch Database Schema (MySQL)
-- Operational Storage ONLY (Strictly separate from HDFS Analytical Storage & PySpark Predictions)
-- All queries ordered by timestamp DESC so NEWEST records are always returned first.

CREATE DATABASE IF NOT EXISTS laundry_ops;
USE laundry_ops;
```

- **Line 1–3**: SQL comments documenting the file's purpose. Emphasizes that MySQL holds ONLY operational data — PySpark analytics live in HDFS.
- **Line 5**: `CREATE DATABASE IF NOT EXISTS laundry_ops;` — Creates the MySQL database named `laundry_ops` only if it doesn't already exist. The `IF NOT EXISTS` guard makes this script safely re-runnable (idempotent).
- **Line 6**: `USE laundry_ops;` — Selects the newly created database as the active schema for all subsequent SQL statements.

### Lines 7–22: Table `machine_status` — Live Operational State

```sql
CREATE TABLE IF NOT EXISTS machine_status (
    machine_id        VARCHAR(20)   PRIMARY KEY,
    branch            VARCHAR(50)   NOT NULL,
    cycle_temperature DECIMAL(5,2)  NOT NULL,
    status            VARCHAR(10)   NOT NULL,
    last_updated      DATETIME      NOT NULL,
    INDEX idx_last_updated (last_updated),
    INDEX idx_status (status)
);
```

- **Line 1**: `CREATE TABLE IF NOT EXISTS machine_status` — Creates the table only if it doesn't exist. This is the **live state table** — exactly one row per washing machine.
- **Line 2**: `machine_id VARCHAR(20) PRIMARY KEY` — The washer unit ID (e.g., `WM_0007`). `PRIMARY KEY` ensures uniqueness — there can never be two rows for the same machine. `VARCHAR(20)` accommodates IDs up to 20 characters.
- **Line 3**: `branch VARCHAR(50) NOT NULL` — The city/branch location where the machine is installed (e.g., `Kigali`, `Musanze`). `NOT NULL` prevents empty branch fields from being inserted.
- **Line 4**: `cycle_temperature DECIMAL(5,2) NOT NULL` — The latest temperature reading in °C. `DECIMAL(5,2)` means 5 total digits with 2 after the decimal point (range: -999.99 to 999.99), giving exact precision for values like `74.35°C`.
- **Line 5**: `status VARCHAR(10) NOT NULL` — Either `'NORMAL'` or `'ALERT'`. `ALERT` is assigned when `cycle_temperature > 70.0°C`. `VARCHAR(10)` fits both values.
- **Line 6**: `last_updated DATETIME NOT NULL` — The UTC timestamp of when this machine's state was last refreshed by the consumer. Used for `ORDER BY last_updated DESC` — newest activity first.
- **Line 7**: `INDEX idx_last_updated (last_updated)` — Creates a B-tree index on `last_updated` column. This makes `ORDER BY last_updated DESC` queries extremely fast (index scan instead of full table sort).
- **Line 8**: `INDEX idx_status (status)` — Creates a B-tree index on `status` column. Enables fast `WHERE status = 'ALERT'` filtering for the dashboard without scanning every row.

**How this table is written to**: The Kafka Consumer executes `REPLACE INTO machine_status (machine_id, ...) VALUES (...)` on every incoming reading. `REPLACE INTO` atomically deletes the existing row for that `machine_id` (if any) and inserts the new one, keeping exactly one row per machine at all times.

### Lines 24–37: Table `readings_log` — Rolling Telemetry Audit Log

```sql
CREATE TABLE IF NOT EXISTS readings_log (
    reading_id        CHAR(36)      PRIMARY KEY,
    machine_id        VARCHAR(20)   NOT NULL,
    branch            VARCHAR(50)   NOT NULL,
    cycle_temperature DECIMAL(5,2)  NOT NULL,
    txn_timestamp     DATETIME      NOT NULL,
    status            VARCHAR(10)   NOT NULL,
    breakdown_soon    TINYINT       DEFAULT 0,
    INDEX idx_txn_timestamp (txn_timestamp),
    INDEX idx_machine_id (machine_id),
    INDEX idx_status (status)
);
```

- **Line 1**: `CREATE TABLE IF NOT EXISTS readings_log` — The append-only telemetry audit log. Every individual sensor reading lands as a separate row here.
- **Line 2**: `reading_id CHAR(36) PRIMARY KEY` — UUID v4 string (e.g., `d7f8a1e2-0000-4000-8000-000000000001`). `CHAR(36)` is fixed-length for UUID format (32 hex + 4 dashes). `PRIMARY KEY` ensures no duplicate readings.
- **Line 3**: `machine_id VARCHAR(20) NOT NULL` — Which washer generated this reading. Denormalized (copied from `machine_status`) for query speed — avoids JOIN overhead.
- **Line 4**: `branch VARCHAR(50) NOT NULL` — Branch location, also denormalized for direct filtering without JOINs.
- **Line 5**: `cycle_temperature DECIMAL(5,2) NOT NULL` — Temperature at the moment this reading was captured.
- **Line 6**: `txn_timestamp DATETIME NOT NULL` — UTC timestamp of the reading. Named `txn_timestamp` (transaction timestamp) to distinguish from MySQL's internal timestamps. **All API queries use `ORDER BY txn_timestamp DESC`** — newest readings first.
- **Line 7**: `status VARCHAR(10) NOT NULL` — `NORMAL` or `ALERT` at the time of this reading.
- **Line 8**: `breakdown_soon TINYINT DEFAULT 0` — Predictive label: `1` = machine is predicted to overheat in the next cycle, `0` = normal. `DEFAULT 0` means readings without explicit prediction default to "no risk."
- **Line 9**: `INDEX idx_txn_timestamp (txn_timestamp)` — Index for fast `ORDER BY txn_timestamp DESC LIMIT 50` queries. Critical for the dashboard's "50 newest readings" view.
- **Line 10**: `INDEX idx_machine_id (machine_id)` — Index for fast `WHERE machine_id = 'WM_0007'` lookups when inspecting a specific machine's history.
- **Line 11**: `INDEX idx_status (status)` — Index for fast `WHERE status = 'ALERT'` filtering to count active alerts.

**How this table is written to**: Consumer executes `INSERT INTO readings_log (...) VALUES (...) ON DUPLICATE KEY UPDATE cycle_temperature=VALUES(cycle_temperature), status=VALUES(status)`. The `ON DUPLICATE KEY UPDATE` handles Kafka at-least-once redeliveries idempotently — if the same `reading_id` UUID arrives twice (after a crash/restart), the row is updated rather than duplicated.

---

## 2. Telemetry Stream Generator (`generator/generator.py`)

### Lines 1–6: Module Docstring

```python
"""
SpinWatch - Washing Machine Telemetry Stream Generator (High Velocity & Variety)
Simulates continuous multi-sensor telemetry stream for 1,000 washing machines across 13 branches.
Demonstrates Big Data Velocity & Variety (Temperature, Vibration, Water Pressure, Power, Error Codes).
Pushes telemetry to REST Framework Ingress API -> Apache Kafka topic 'machine-readings'.
"""
```

- **Lines 1–6**: Python docstring explaining the module's role. This is the **data source** for the entire pipeline — it simulates IoT sensor readings from the washer fleet.

### Lines 8–13: Imports

```python
import time
import uuid
import random
import argparse
import datetime
import requests
```

- **Line 8**: `time` — Used for `time.sleep(1.0 / args.tps)` to control the generation rate (readings per second).
- **Line 9**: `uuid` — Used for `uuid.uuid4()` to generate globally unique reading IDs.
- **Line 10**: `random` — Used for `random.choice()` (pick random machine), `random.uniform()` (generate realistic sensor values).
- **Line 11**: `argparse` — CLI argument parsing for `--tps` (throughput) and `--target-url` (REST API destination).
- **Line 12**: `datetime` — Used for `datetime.datetime.now(datetime.timezone.utc).isoformat()` to generate UTC ISO 8601 timestamps.
- **Line 13**: `requests` — HTTP library for `requests.post()` to send readings to the REST Ingress API on Port 8000.

### Lines 15–20: Branch & Error Code Constants

```python
BRANCHES = [
    "Kigali", "Musanze", "Huye", "Rubavu", "Rusizi", "Nyagatare",
    "Rwamagana", "Gicumbi", "Kamembe", "Karongi", "Nyanza", "Bugesera", "Kamonyi"
]

ERROR_CODES = ["DEMAGED ", "UNKNOWN", "NO WATER ", "LEAKAGE", "E01_OVERHEAT", "E02_VIBRATION", "E03_PRESSURE_DROP", "E04_POWER_SURGE"]
```

- **Lines 15–18**: `BRANCHES` — List of 13 Rwandan cities where washing machines are installed. Each machine is randomly assigned to one of these branches at startup.
- **Line 20**: `ERROR_CODES` — Fault codes reported by the washer's internal diagnostics. Only assigned when `cycle_temperature > 70.0°C`. Includes machine-specific codes (`E01_OVERHEAT`, `E02_VIBRATION`) and general codes (`LEAKAGE`, `UNKNOWN`).

### Lines 22–28: Machine Fleet Initialization

```python
MACHINES = [
    {
        "machine_id": f"WM_{i:04d}",
        "branch": random.choice(BRANCHES)
    }
    for i in range(1, 3001)
]
```

- **Lines 22–28**: List comprehension creating 3,000 machine objects. `f"WM_{i:04d}"` generates zero-padded IDs: `WM_0001`, `WM_0002`, ..., `WM_3000`. Each machine is randomly assigned to one of the 13 branches using `random.choice(BRANCHES)`.

### Lines 30–34: CLI Argument Parsing

```python
def generate_telemetry():
    parser = argparse.ArgumentParser(description="SpinWatch Telemetry Stream Generator")
    parser.add_argument("--tps", type=float, default=5.0, help="Readings per second (default: 5)")
    parser.add_argument("--target-url", type=str, default="http://localhost:8000/api/readings/", help="Target REST API URL")
    args = parser.parse_args()
```

- **Line 30**: `def generate_telemetry():` — Main function containing the infinite generation loop.
- **Line 31**: Creates an `ArgumentParser` instance for CLI flag parsing.
- **Line 32**: `--tps` flag — Controls how many readings per second are generated. Default is 5.0 TPS. Demo mode uses `--tps 3`.
- **Line 33**: `--target-url` flag — Where to POST readings. Defaults to the REST Ingress API on `http://localhost:8000/api/readings/`.
- **Line 34**: `args = parser.parse_args()` — Parses the command-line arguments into `args.tps` and `args.target_url`.

### Lines 40–67: Main Generation Loop

```python
    while True:
        machine = random.choice(MACHINES)
        mid = machine["machine_id"]

        current_temp = round(random.uniform(30.0, 102.0), 2)
        vibration_hz = round(random.uniform(12.0, 115.0), 1)
        power_kw = round(random.uniform(1.2, 7.8), 2)
        water_pressure_bar = round(random.uniform(1.0, 4.8), 2)
        error_code = random.choice(ERROR_CODES) if current_temp > 70.0 else "NORMAL"

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
```

- **Line 40**: `while True:` — Infinite loop — generator runs continuously until manually stopped with `Ctrl+C`.
- **Line 41**: `random.choice(MACHINES)` — Picks one random machine from the 3,000-machine fleet.
- **Line 42**: Extracts the `machine_id` string (e.g., `WM_0007`).
- **Line 44**: `random.uniform(30.0, 102.0)` — Generates a random temperature between 30°C (cold rinse) and 102°C (severe overheat). `round(..., 2)` keeps 2 decimal places.
- **Line 45**: `random.uniform(12.0, 115.0)` — Motor vibration frequency. Normal: 12–90 Hz. Above 90 Hz indicates bearing wear.
- **Line 46**: `random.uniform(1.2, 7.8)` — Electrical power consumption in kW.
- **Line 47**: `random.uniform(1.0, 4.8)` — Water intake pressure in bar.
- **Line 48**: Error code is assigned only when temperature exceeds 70°C. Otherwise, set to `"NORMAL"`.
- **Line 50**: **Alert rule**: `status = "ALERT"` if temperature > 70°C OR vibration > 90 Hz. Both conditions indicate machine stress.
- **Line 51**: `breakdown_soon = 1` under the same conditions — heuristic prediction label.
- **Line 52**: UTC ISO 8601 timestamp (e.g., `2026-09-19T18:30:00+00:00`).
- **Lines 54–66**: Constructs the complete reading dictionary with all 11 fields matching the project schema.

### Lines 69–78: HTTP POST Delivery

```python
        try:
            resp = requests.post(args.target_url, json=reading, timeout=(3.0, 5.0))
            if resp.status_code not in (200, 201, 202):
                print(f"[!] REST Ingress API HTTP {resp.status_code}: {resp.text}")
        except requests.exceptions.ConnectionError:
            print(f"[!] REST API unreachable at {args.target_url} — is producer/app.py running?")
        except Exception as e:
            print(f"[!] Telemetry delivery warning: {e}")

        time.sleep(1.0 / args.tps)
```

- **Line 70**: `requests.post(args.target_url, json=reading, timeout=(3.0, 5.0))` — Sends the reading as JSON POST body to the REST API. `timeout=(3.0, 5.0)` means: 3-second connect timeout, 5-second read timeout.
- **Lines 71–72**: If the API returns a non-success status code, prints a warning.
- **Lines 73–74**: If the REST API is unreachable (server not running), catches the `ConnectionError` and prints a helpful message.
- **Line 78**: `time.sleep(1.0 / args.tps)` — Waits between readings. At 3 TPS: sleeps 0.333 seconds. At 5 TPS: sleeps 0.2 seconds.

---

## 3. Kafka Producer (`producer/producer.py`)

### Lines 1–4: Module Docstring

```python
"""
Kafka Producer Module for SpinWatch Heat Telemetry
Keyed by machine_id to ensure order guarantee per washing machine on Kafka topic partitions.
"""
```

- **Lines 1–4**: Docstring highlighting the critical design decision: messages are keyed by `machine_id` for partition-level ordering.

### Lines 6–16: Imports & Kafka Availability Check

```python
import json
import logging

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SpinWatchProducer")
```

- **Line 6**: `json` — For JSON serialization of reading payloads.
- **Line 7**: `logging` — Structured logging instead of `print()`.
- **Lines 9–12**: Tries to import `KafkaProducer` from `kafka-python`. If the package isn't installed, sets `KAFKA_AVAILABLE = False` and the producer runs in standalone fallback mode (logging only, no Kafka).
- **Lines 15–16**: Configures logging at `INFO` level with a named logger `"SpinWatchProducer"`.

### Lines 18–32: `MachineTelemetryProducer` Class

```python
class MachineTelemetryProducer:
    def __init__(self, bootstrap_servers="localhost:9092", topic="machine-readings"):
        self.topic = topic
        self.producer = None
        if KAFKA_AVAILABLE:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers.split(","),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    retries=3
                )
                logger.info(f"KafkaProducer connected to {bootstrap_servers}, topic: {topic}")
            except Exception as e:
                logger.warning(f"Could not connect KafkaProducer: {e}. Running in standalone fallback mode.")
```

- **Line 18**: Class definition — wraps `KafkaProducer` with SpinWatch-specific configuration.
- **Line 19**: Constructor accepts `bootstrap_servers` (Kafka broker address) and `topic` name.
- **Line 20**: Stores the topic name for later use in `send_reading()`.
- **Line 21**: `self.producer = None` — Default to None; set only if Kafka connects successfully.
- **Line 23**: Only attempts connection if `kafka-python` is installed.
- **Line 25**: `bootstrap_servers.split(",")` — Splits comma-separated broker addresses into a list (supports multi-broker clusters).
- **Line 26**: `key_serializer` — Encodes the partition key (`machine_id` string) to UTF-8 bytes. `if k else None` handles the rare case of a missing key.
- **Line 27**: `value_serializer` — Serializes the entire reading dict to a JSON string, then encodes to UTF-8 bytes. This is what Kafka stores in the topic partition.
- **Line 28**: `retries=3` — If a publish fails (transient broker issue), retry up to 3 times automatically before raising an error.

### Lines 34–48: `send_reading()` Method

```python
    def send_reading(self, reading: dict) -> bool:
        mid = reading.get("machine_id")
        if self.producer:
            try:
                future = self.producer.send(self.topic, key=mid, value=reading)
                future.get(timeout=2.0)
                logger.debug(f"Published reading for {mid} to Kafka topic {self.topic}")
                return True
            except Exception as e:
                logger.error(f"Error publishing to Kafka: {e}")
                return False
        else:
            logger.info(f"[Fallback Log Only] Reading for {mid}: temp={reading.get('cycle_temperature')}°C, status={reading.get('status')}")
            return True
```

- **Line 34**: Method signature — takes a reading dict, returns `True` on success, `False` on failure.
- **Line 35**: Extracts `machine_id` to use as the Kafka partition key.
- **Line 37**: `self.producer.send(self.topic, key=mid, value=reading)` — **This is the critical line.** By passing `key=mid`, Kafka hashes the `machine_id` to determine which partition this message goes to. All messages with the same key (`WM_0007`) always go to the same partition, guaranteeing strict per-machine ordering.
- **Line 38**: `future.get(timeout=2.0)` — Blocks synchronously until the broker acknowledges the message (within 2 seconds). This ensures the message was actually persisted before returning success.
- **Line 45–46**: If Kafka is unavailable (`self.producer is None`), logs the reading locally and returns `True` so the pipeline continues in standalone mode.

---

## 4. Kafka Telemetry Consumer (`consumer/consumer.py`)

### Lines 1–8: Module Docstring

```python
"""
SpinWatch - Kafka Telemetry Consumer Application
Reads from Kafka topic 'machine-readings' (Consumer Group: 'maintenance-tracker').
- Updates operational database MySQL 'laundry_ops' (tables: machine_status, readings_log).
- Appends raw telemetry into HDFS historical directory (/data/machines/raw/dt=YYYY-MM-DD/).
- Sends live streaming buffer to Dashboard endpoint for instant real-time visualization.
- Uses explicit manual offset commits for strictly guaranteed at-least-once delivery.
"""
```

- **Lines 1–8**: Docstring documenting the consumer's three responsibilities: MySQL write, HDFS append, and live buffer broadcast. Emphasizes the manual offset commit strategy.

### Lines 10–42: Imports, Configuration & Constants

```python
import os, sys, json, logging, datetime, urllib.request

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

MYSQL_CONFIG = {
    "host": "127.0.0.1", "port": 3306,
    "user": "root", "password": "",
    "database": "laundry_ops"
}

LIVE_CONSUMER_BUFFER = []
```

- **Lines 10–11**: Standard library imports plus `urllib.request` for optional dashboard broadcast.
- **Lines 17–19**: `PROJECT_ROOT` — Computes the project root directory (two levels up from `consumer/consumer.py`). Adds it to `sys.path` so cross-module imports work.
- **Lines 21–32**: Optional imports for `KafkaConsumer` and `mysql.connector` with graceful degradation flags.
- **Lines 36–41**: MySQL connection config — connects to `localhost:3306` as `root` with empty password, targeting `laundry_ops` database.
- **Line 45**: `LIVE_CONSUMER_BUFFER = []` — In-memory rolling buffer holding the 50 most recent readings. The dashboard reads this directly for the "Kafka Live Telemetry Stream" tab.

### Lines 47–65: Kafka Consumer Connection

```python
def process_messages(bootstrap_servers="localhost:9092", topic="machine-readings", group_id="maintenance-tracker"):
    global LIVE_CONSUMER_BUFFER

    if not KAFKA_AVAILABLE:
        logger.info("[Standalone Mode] Kafka-python not available. Using local pipeline buffer.")
        return

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers.split(","),
        group_id=group_id,
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        request_timeout_ms=5000
    )
```

- **Line 47**: Function signature with default Kafka config. `group_id="maintenance-tracker"` identifies this consumer group — enabling horizontal scaling.
- **Line 57**: `enable_auto_commit=False` — **Critical for at-least-once delivery.** Disables automatic offset commits. The consumer will only commit offsets manually via `consumer.commit()` after a successful MySQL write.
- **Line 58**: `value_deserializer` — Converts the raw UTF-8 bytes back to a Python dict using `json.loads()`.
- **Line 59**: `auto_offset_reset="earliest"` — On first connection or lost offsets, starts reading from the very beginning of the topic. No readings are ever skipped.
- **Line 60**: `request_timeout_ms=5000` — 5-second timeout for broker communication.

### Lines 78–143: Message Processing Loop

```python
    for msg in consumer:
        reading = msg.value
        mid = reading.get("machine_id")
        branch = reading.get("branch")
        temp = float(reading.get("cycle_temperature", 0.0))
        
        status = "ALERT" if temp > 70.0 else "NORMAL"
        breakdown_soon = 1 if temp > 68.0 else 0

        # 1. Append to HDFS Historical Storage
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        raw_dir = os.path.join(PROJECT_ROOT, "data", "machines", "raw", f"dt={today_str}")
        os.makedirs(raw_dir, exist_ok=True)
        with open(os.path.join(raw_dir, "stream_history.json"), "a") as f:
            f.write(json.dumps(reading) + "\n")

        # 2. Update MySQL Operational Storage
        cursor = db_conn.cursor()
        cursor.execute("REPLACE INTO machine_status (...) VALUES (%s, %s, %s, %s, %s)", (...))
        cursor.execute("INSERT INTO readings_log (...) VALUES (...) ON DUPLICATE KEY UPDATE ...", (...))
        db_conn.commit()
        cursor.close()

        # 3. Buffer for Dashboard Live Stream
        LIVE_CONSUMER_BUFFER.append(reading)
        if len(LIVE_CONSUMER_BUFFER) > 50:
            LIVE_CONSUMER_BUFFER = LIVE_CONSUMER_BUFFER[-50:]

        # 4. Manual offset commit AFTER successful processing
        consumer.commit()
```

- **Line 79**: `for msg in consumer:` — Blocking iterator that reads one message at a time from Kafka. Blocks when no messages are available.
- **Line 80**: `msg.value` — The deserialized reading dict (already decoded by `value_deserializer`).
- **Lines 81–83**: Extracts `machine_id`, `branch`, and `cycle_temperature` from the reading.
- **Line 94**: `status = "ALERT"` — Re-validates the ALERT threshold server-side (don't trust the generator's label blindly).
- **Line 95**: `breakdown_soon = 1 if temp > 68.0` — Conservative threshold: starts predicting breakdown risk at 68°C (2°C before the ALERT threshold).
- **Lines 101–105**: **HDFS Append** — Creates the date-partitioned directory `data/machines/raw/dt=2026-09-19/` and appends the JSON reading as a single line to `stream_history.json`. `os.makedirs(raw_dir, exist_ok=True)` creates the directory tree if it doesn't exist.
- **Lines 117–122**: **MySQL Write** — `REPLACE INTO machine_status` upserts the live state. `INSERT INTO readings_log ... ON DUPLICATE KEY UPDATE` appends the reading idempotently.
- **Line 123**: `db_conn.commit()` — Commits the MySQL transaction. Only after this succeeds...
- **Lines 138–140**: **Live Buffer** — Appends reading to in-memory buffer. Caps at 50 entries (oldest are dropped).
- **Line 143**: `consumer.commit()` — **Commits the Kafka offset ONLY after the MySQL write succeeds.** If the consumer crashes between the MySQL write and this commit, the message will be redelivered from Kafka on restart — achieving at-least-once delivery guarantee.

---

## 5. REST Ingress & Pipeline Inspection API (`producer/app.py`)

### Lines 1–14: Module Docstring

```python
"""
SpinWatch - Unified REST API Ingress & Pipeline Inspection Server
Exposes simple REST endpoints to test, query, and verify data at every stage of the Big Data Pipeline:
- Point 1: POST & GET /api/readings/ (Generator Ingress Stream) & GET /api/generator/status
- Point 2: GET /api/kafka/status (Kafka Topic & Partition Inspector)
- Point 3a: GET /api/connect/status (Kafka Connect Sinks Inspector)
- Point 3b: GET /api/hdfs/raw (HDFS Historical Raw Data Inspector)
- Point 3c: GET /api/sql/readings (MySQL Operational Storage Inspector)
- Point 4a: GET /api/predictions (PySpark MLlib Predictions from HDFS)
- Point 4b: GET /api/insights (PySpark Analytics from HDFS)
- Point 5: GET /api/consumer/live (Consumer Live Stream Buffer)
"""
```

### Key Architecture Points

This file serves as **both the data ingress point and the pipeline inspector**:

1. **`POST /api/readings/`** — Receives readings from the generator, publishes them to Kafka, syncs them to MySQL and HDFS in standalone mode.
2. **`GET /api/readings/`** — Returns the 100 most recent ingress readings from an in-memory buffer.
3. **`GET /api/sql/readings`** — Queries MySQL `readings_log` with `ORDER BY txn_timestamp DESC LIMIT 50` — **newest records always first**.
4. **All data is returned ordered newest first** throughout the API.

### Lines 274–290: MySQL Operational Storage Endpoint (Newest First)

```python
        elif path in ["/api/sql/readings", "/api/stage3/sql"]:
            # ORDER BY txn_timestamp DESC — newest readings returned first
            readings = query_mysql(
                "SELECT reading_id, machine_id, branch, cycle_temperature, txn_timestamp, status, breakdown_soon "
                "FROM readings_log ORDER BY txn_timestamp DESC LIMIT 50"
            )
            # ORDER BY last_updated DESC — most recently active machines first
            status_summary = query_mysql(
                "SELECT machine_id, branch, status, cycle_temperature, last_updated "
                "FROM machine_status ORDER BY last_updated DESC"
            )
```

- **Lines 276–279**: Queries the 50 most recent readings from `readings_log`, ordered by `txn_timestamp DESC`. Includes `reading_id` as the first column so the dashboard can display it.
- **Lines 281–284**: Queries all machine states from `machine_status`, ordered by `last_updated DESC` — machines that most recently reported activity appear first.

---

## 6. PySpark Batch Insights (`spark/insights.py`)

### Lines 19–36: PySpark Session & Data Loading

```python
def run_pyspark_insights(hdfs_raw_path="hdfs://localhost:9000/data/machines/raw"):
    spark = SparkSession.builder \
        .appName("SpinWatch-Heat-Insights") \
        .getOrCreate()

    try:
        df = spark.read.parquet(hdfs_raw_path)
    except Exception as e:
        local_raw = "./data/machines/raw"
        df = spark.read.option("recursiveFileLookup", "true").json(local_raw)
```

- **Line 19**: Function accepts the HDFS path. Defaults to `hdfs://localhost:9000/data/machines/raw`.
- **Lines 25–27**: Creates a PySpark `SparkSession` named `"SpinWatch-Heat-Insights"`.
- **Lines 30–31**: Attempts to read from HDFS Parquet files first.
- **Lines 33–35**: **Fallback**: If HDFS is unavailable, reads from local JSON files with `recursiveFileLookup=true` (traverses all `dt=*` subdirectories).

### Lines 40–57: Three Analytical Insights

```python
    # Insight 1: Average cycle temperature per branch
    avg_temp_by_branch = df.groupBy("branch") \
        .agg(F.round(F.avg("cycle_temperature"), 2).alias("avg_temperature"))

    # Insight 2: Machines with highest frequency of heat ALERT readings
    time_in_alert = df.filter(F.col("cycle_temperature") > 70.0) \
        .groupBy("machine_id", "branch") \
        .agg(F.count("*").alias("alert_count")) \
        .orderBy(F.col("alert_count").desc())

    # Insight 3: Hour of day with the most ALERT readings nationwide
    busiest_alert_hour = df.filter(F.col("cycle_temperature") > 70.0) \
        .withColumn("hour", F.hour(F.col("txn_timestamp"))) \
        .groupBy("hour") \
        .agg(F.count("*").alias("alert_count")) \
        .orderBy(F.col("alert_count").desc())
```

- **Lines 41–42**: **Insight 1** — Groups by `branch`, computes `AVG(cycle_temperature)` rounded to 2 decimals. Output: which city runs hottest?
- **Lines 45–48**: **Insight 2** — Filters readings where temp > 70°C, groups by `machine_id` and `branch`, counts occurrences. Output: which machines overheat most often?
- **Lines 52–57**: **Insight 3** — Filters ALERT readings, extracts the hour from the timestamp, groups by hour, counts. Output: at what hour of day do overheats peak?

---

## 7. PySpark MLlib Model Trainer (`spark/train_model.py`)

### Lines 47–61: Feature Engineering & Pipeline

```python
    # Lead-window to label breakdown_soon
    w = Window.partitionBy("machine_id").orderBy("txn_timestamp")
    data = df.withColumn("next_temp", F.lead("cycle_temperature", 1).over(w)) \
        .withColumn("breakdown_soon", (F.col("next_temp") > 70.0).cast("int")) \
        .filter(F.col("next_temp").isNotNull())

    # Categorical indexing & feature assembly
    branch_indexer = StringIndexer(inputCol="branch", outputCol="branch_idx", handleInvalid="keep")
    assembler = VectorAssembler(inputCols=["cycle_temperature", "branch_idx"], outputCol="features")
    lr = LogisticRegression(labelCol="breakdown_soon", featuresCol="features", maxIter=20)
    pipeline = Pipeline(stages=[branch_indexer, assembler, lr])
```

- **Line 48**: `Window.partitionBy("machine_id").orderBy("txn_timestamp")` — Creates a window that partitions by machine and sorts by timestamp within each partition. This ensures the `lead()` function looks at the **next chronological reading for the same machine**.
- **Line 49**: `F.lead("cycle_temperature", 1).over(w)` — Gets the temperature of the **next reading** for this machine. This is the "future" temperature we're trying to predict.
- **Line 50**: `(F.col("next_temp") > 70.0).cast("int")` — If the next reading's temp > 70°C, label `breakdown_soon = 1`. Otherwise `0`. This is the ground truth training label.
- **Line 51**: Filters out rows where `next_temp` is null (the last reading for each machine has no "next").
- **Line 54**: `StringIndexer` — Converts categorical `branch` strings ("Kigali", "Musanze", etc.) into numeric indices (0, 1, 2, ...) that the ML model can process.
- **Line 55**: `VectorAssembler` — Combines `cycle_temperature` (numeric) and `branch_idx` (indexed category) into a single feature vector column named `"features"`.
- **Line 56**: `LogisticRegression` — Binary classification model predicting `breakdown_soon` (0 or 1). `maxIter=20` limits training to 20 optimization iterations.
- **Line 57**: `Pipeline` — Chains the three stages: indexer → assembler → classifier. Calling `pipeline.fit()` runs all stages sequentially.

### Lines 63–84: Train, Evaluate & Save

```python
    train_df, test_df = data.randomSplit([0.8, 0.2], seed=42)
    model = pipeline.fit(train_df)

    preds = model.transform(test_df)
    auc = BinaryClassificationEvaluator(labelCol="breakdown_soon").evaluate(preds)
    precision = MulticlassClassificationEvaluator(labelCol="breakdown_soon", metricName="weightedPrecision").evaluate(preds)
    recall = MulticlassClassificationEvaluator(labelCol="breakdown_soon", metricName="weightedRecall").evaluate(preds)

    model.write().overwrite().save("hdfs://localhost:9000/data/machines/models/lr_heat_v1")
```

- **Line 64**: 80/20 train/test split with fixed seed `42` for reproducibility.
- **Line 65**: `pipeline.fit(train_df)` — Trains the full pipeline on 80% of data.
- **Lines 67–70**: Evaluates on the 20% test set using three metrics:
  - **AUC**: Overall model quality (0.5 = random, 1.0 = perfect).
  - **Precision**: Fraction of "breakdown predicted" that are actually real breakdowns.
  - **Recall**: Fraction of real breakdowns that the model catches. **This is the most important metric** — missing a real breakdown (False Negative) is far more costly than a false alarm.
- **Line 72**: Saves the trained model artifact to HDFS.

---

## 8. Web Dashboard Server (`dashboard/app.py`)

### Lines 52–99: HTTP Request Handler

```python
class DashboardHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if path == "/" or path == "/index.html":
            self.serve_template("templates/index.html")
        elif path == "/api/status":
            self.handle_api_status()
        elif path == "/api/predictions":
            self.handle_api_predictions()
        elif path == "/api/insights":
            self.handle_api_insights()
        elif path == "/api/sql/readings":
            self.handle_api_sql_readings()
        elif path == "/api/consumer/live":
            self.handle_api_consumer_live()
        elif path == "/api/hdfs/dates":
            self.handle_api_hdfs_dates()
        elif path == "/api/hdfs/history":
            self.handle_api_hdfs_history(dt)
```

- **Lines 52–53**: `DashboardHandler` extends Python's built-in `SimpleHTTPRequestHandler`, adding custom routing.
- **Line 55**: Root path (`/`) serves the main HTML template.
- **Lines 56–68**: Each `/api/*` path is routed to a dedicated handler method that queries MySQL, HDFS, or the consumer buffer.

### Lines 309–317: MySQL Readings Endpoint (with `reading_id`, Newest First)

```python
    def handle_api_sql_readings(self):
        rows = query_mysql(
            "SELECT reading_id, machine_id, branch, cycle_temperature, "
            "txn_timestamp, status, breakdown_soon "
            "FROM readings_log ORDER BY txn_timestamp DESC LIMIT 50"
        )
```

- **Lines 309–314**: Queries the 50 newest readings from `readings_log`. **Includes `reading_id`** as the first column so the dashboard HTML table can display the full UUID. **`ORDER BY txn_timestamp DESC`** ensures the most recent readings appear at the top.

---

## 9. Dashboard HTML Template (`dashboard/templates/index.html`)

### Lines 1–10: HTML Head & Meta

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>SpinWatch | Reading & Predicted Temperature Telemetry</title>
  <link rel="stylesheet" href="/static/css/style.css">
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
```

- **Line 1**: HTML5 doctype declaration.
- **Line 7**: Links to the external CSS file for light/dark theme support.
- **Line 8**: Loads Chart.js library from CDN for the live temperature chart and branch analytics bar chart.

### Lines 109–126: MySQL Operational Table (with Reading ID Header)

```html
<table>
  <thead>
    <tr>
      <th>Reading ID</th>
      <th>Washer ID</th>
      <th>Branch</th>
      <th>Reading Temperature (MySQL)</th>
      <th>Operational Status</th>
      <th>Breakdown</th>
      <th>Last Updated</th>
      <th>Actions</th>
    </tr>
  </thead>
  <tbody id="table-body"></tbody>
</table>
```

- **Line 113**: `Reading ID` — New column header displaying the UUID `reading_id` from the MySQL `readings_log` table. This uniquely identifies each telemetry reading.
- **Line 114**: `Washer ID` — The `machine_id` (e.g., `WM_0007`).
- **Line 115**: `Branch` — City location.
- **Line 116**: `Reading Temperature` — The `cycle_temperature` value in °C.
- **Line 117**: `Operational Status` — `NORMAL` (green badge) or `ALERT` (red badge).
- **Line 118**: `Breakdown` — The `breakdown_soon` label: `⚠️ 1 (Risk)` or `0 (Normal)`.
- **Line 119**: `Last Updated` — `last_updated` or `txn_timestamp` value.
- **Line 120**: `Actions` — "Inspect" button opening the diagnostic modal.

### Lines 218–228: MySQL Real-Time Log Table (with Reading ID Header)

```html
<thead>
  <tr>
    <th>Reading ID</th>
    <th>Washer ID</th>
    <th>Branch</th>
    <th>Cycle Temp (°C)</th>
    <th>Txn Timestamp</th>
    <th>Operational Status</th>
    <th>Breakdown Label</th>
    <th>Payload</th>
  </tr>
</thead>
```

- **Line 220**: `Reading ID` — Same UUID column in the real-time stream view tab. Shows truncated UUID with full value on hover (tooltip).

### Lines 655–677: JavaScript — MySQL Live Table Renderer

```javascript
function renderMySQLLiveTable(rows) {
  const tbody = document.getElementById('mysql-live-tbody');
  mysqlRecordsMap = rows.slice(0, 30);
  tbody.innerHTML = mysqlRecordsMap.map((r, idx) => {
    const readingId = r.reading_id || '--';
    const shortId = readingId.length > 12 ? readingId.substring(0, 12) + '...' : readingId;
    return `
      <tr>
        <td title="${readingId}">${shortId}</td>
        <td>🧺 ${r.machine_id}</td>
        <td>${r.branch}</td>
        <td>${temp}°C</td>
        <td>${r.txn_timestamp || '--'}</td>
        <td><span class="status-badge">${r.status}</span></td>
        <td>${r.breakdown_soon === 1 ? '⚠️ 1 (Risk)' : '0 (Normal)'}</td>
        <td><button onclick="inspectMySQLRecord(${idx})">Payload</button></td>
      </tr>`;
  }).join('');
}
```

- **Line 659**: `r.reading_id || '--'` — Reads the full UUID from the API response. Falls back to `'--'` if missing.
- **Line 660**: Truncates to 12 characters + `'...'` for display. Full UUID is shown on hover via the `title` attribute.

### Lines 858–871: Auto-Refresh Intervals

```javascript
initCharts();
loadHDFSDates();
setInterval(fetchStatus, 1000);
setInterval(fetchLiveStream, 1000);
setInterval(fetchMySQLLiveLog, 1500);
setInterval(fetchMySQLChart, 2000);
setInterval(fetchHDFSLiveLog, 1500);
setInterval(fetchPredictions, 3000);
```

- **Line 858**: Initializes Chart.js instances for the temperature stream chart and branch bar chart.
- **Line 859**: Loads HDFS date partitions for the inspector dropdown.
- **Lines 860–865**: Sets up auto-refresh intervals:
  - `fetchStatus` every 1 second — refreshes machine_status cards/table.
  - `fetchLiveStream` every 1 second — refreshes Kafka live stream tab.
  - `fetchMySQLLiveLog` every 1.5 seconds — refreshes MySQL real-time log tab.
  - `fetchMySQLChart` every 2 seconds — refreshes the branch temperature bar chart.
  - `fetchHDFSLiveLog` every 1.5 seconds — refreshes HDFS live stream tab.
  - `fetchPredictions` every 3 seconds — refreshes MLlib predictions.

---

## 10. Django REST Framework API Browser (`api/`)

### `api/spinwatch_api/settings.py` — Django Configuration

```python
SECRET_KEY = "spinwatch-drf-dev-secret-key-not-for-production-use"
DEBUG = True
ALLOWED_HOSTS = ["*", "localhost", "127.0.0.1"]

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.staticfiles",
    "rest_framework",
    "corsheaders",
    "endpoints",
]
```

- **Line 1**: `SECRET_KEY` — Required by Django. Uses a dev-only key (not for production deployment).
- **Line 2**: `DEBUG = True` — Enables detailed error pages and auto-reloading in development.
- **Line 3**: `ALLOWED_HOSTS = ["*"]` — Accepts requests from any hostname (appropriate for local dev).
- **Lines 5–10**: `INSTALLED_APPS` — The Django apps loaded:
  - `django.contrib.contenttypes` — Required by Django internals.
  - `django.contrib.staticfiles` — Serves DRF's CSS/JS for the browsable HTML API.
  - `rest_framework` — Django REST Framework core — provides `APIView`, `Response`, renderers.
  - `corsheaders` — Adds CORS headers so the dashboard on Port 8050 can query this API.
  - `endpoints` — Our custom app containing all SpinWatch API views.

```python
REST_FRAMEWORK = {
    "DEFAULT_RENDERER_CLASSES": [
        "rest_framework.renderers.BrowsableAPIRenderer",
        "rest_framework.renderers.JSONRenderer",
    ],
    "DEFAULT_AUTHENTICATION_CLASSES": [],
    "DEFAULT_PERMISSION_CLASSES": [],
    "UNAUTHENTICATED_USER": None,
    "UNAUTHENTICATED_TOKEN": None,
}

CORS_ALLOW_ALL_ORIGINS = True
```

- **Lines 1–4**: `BrowsableAPIRenderer` is listed first — when opened in a browser, DRF renders a beautiful HTML page with syntax-highlighted JSON. When accessed via Postman/curl with `Accept: application/json`, returns raw JSON.
- **Lines 5–6**: Empty authentication/permission classes — no login required (SpinWatch is an internal tool).
- **Lines 7–8**: `UNAUTHENTICATED_USER = None` — Prevents DRF from trying to import `django.contrib.auth.models.AnonymousUser`, keeping the app lightweight.

### `api/spinwatch_api/urls.py` — Root URL Routing

```python
from django.urls import path, include

urlpatterns = [
    path("api/", include("endpoints.urls")),
]
```

- **Line 1**: Imports Django's `path()` and `include()` URL helpers.
- **Line 4**: `path("api/", include("endpoints.urls"))` — All URLs under `/api/` are delegated to the `endpoints` app's URL configuration. This is why endpoints are accessed at `http://localhost:8001/api/readings/`, etc.

### `api/endpoints/urls.py` — Endpoint URL Mapping

```python
from django.urls import path
from . import views

urlpatterns = [
    path("",                    views.APIRootView.as_view(),        name="api-root"),
    path("readings/",           views.ReadingsView.as_view(),       name="readings"),
    path("generator/status/",   views.GeneratorStatusView.as_view(), name="generator-status"),
    path("kafka/status/",       views.KafkaStatusView.as_view(),    name="kafka-status"),
    path("connect/status/",     views.ConnectStatusView.as_view(),  name="connect-status"),
    path("hdfs/raw/",           views.HDFSRawView.as_view(),        name="hdfs-raw"),
    path("hdfs/dates/",         views.HDFSDatesView.as_view(),      name="hdfs-dates"),
    path("hdfs/history/",       views.HDFSHistoryView.as_view(),    name="hdfs-history"),
    path("sql/readings/",       views.SQLReadingsView.as_view(),    name="sql-readings"),
    path("predictions/",        views.PredictionsView.as_view(),    name="predictions"),
    path("insights/",           views.InsightsView.as_view(),       name="insights"),
    path("consumer/live/",      views.ConsumerLiveView.as_view(),   name="consumer-live"),
]
```

- **Line 5**: `path("")` — The empty path `/api/` maps to `APIRootView`, which renders the DRF API root browser with all endpoint links.
- **Lines 6–16**: Each URL pattern maps to a DRF `APIView` class. The `.as_view()` method converts the class into a callable view function. The `name=` parameter enables URL reversing with `reverse("api-root")`.

### `api/endpoints/views.py` — DRF APIView Classes (Key Endpoint)

```python
class SQLReadingsView(APIView):
    def get(self, request, format=None):
        # 50 newest readings — ORDER BY txn_timestamp DESC
        readings = query_mysql(
            "SELECT reading_id, machine_id, branch, cycle_temperature, "
            "txn_timestamp, status, breakdown_soon "
            "FROM readings_log ORDER BY txn_timestamp DESC LIMIT 50"
        )
        # All machine states — ORDER BY last_updated DESC
        machine_states = query_mysql(
            "SELECT machine_id, branch, status, cycle_temperature, last_updated "
            "FROM machine_status ORDER BY last_updated DESC"
        )
        return Response({
            "stage": "Point 3c: MySQL Operational Storage (laundry_ops)",
            "ordering": "All results ordered by timestamp DESC — newest records first",
            "machine_status_newest_first": machine_states or [],
            "latest_50_readings_newest_first": readings or [],
        })
```

- **Lines 3–7**: Same query as `producer/app.py` — returns 50 newest readings with `reading_id` included, ordered by `txn_timestamp DESC`.
- **Lines 9–12**: Returns all machine live states ordered by `last_updated DESC`.
- **Lines 13–18**: Returns a structured DRF `Response` object. DRF automatically serializes this to JSON or renders it in the browsable HTML API, depending on the client's `Accept` header.

---

## 11. Master Runner Script (`run_project.py`)

```python
def main():
    # 1. Seed HDFS & MySQL
    subprocess.run([sys.executable, "scripts/seed_hdfs_data.py"])
    subprocess.run([sys.executable, "scripts/upload_to_hdfs.py"])

    # 2. Start Ingress REST API (Port 8000)
    producer_proc = subprocess.Popen([sys.executable, "producer/app.py"])

    # 3. Start Kafka Consumer
    consumer_proc = subprocess.Popen([sys.executable, "consumer/consumer.py"])

    # 4. Start Telemetry Generator (3 TPS)
    generator_proc = subprocess.Popen([sys.executable, "generator/generator.py", "--tps", "3"])

    # 5. Start Web Dashboard (Port 8050)
    dashboard_proc = subprocess.Popen([sys.executable, "dashboard/app.py"])

    # 6. Start Django DRF API Browser (Port 8001)
    drf_proc = subprocess.Popen([sys.executable, "api/manage.py", "runserver", "8001", "--noreload"])
```

- **Lines 3–4**: `subprocess.run()` — Runs synchronously. Seeds initial data before starting services.
- **Lines 7–19**: `subprocess.Popen()` — Starts each service as a background process. All processes run concurrently.
- **`Ctrl+C`** in the terminal calls `.terminate()` on all processes to shut down cleanly.

---

## 12. Dependencies (`requirements.txt`)

```
requests>=2.28.0                  # Generator HTTP POST delivery
kafka-python>=2.0.2               # Kafka Producer & Consumer
mysql-connector-python>=8.0.30    # MySQL connectivity
pyspark>=3.3.0                    # Batch analytics & MLlib
django>=4.2.0                     # DRF API Browser framework
djangorestframework>=3.14.0       # Browsable HTML API
django-cors-headers>=4.0.0        # CORS for dashboard integration
```

- **`requests`**: Used by the generator to POST readings to the REST API.
- **`kafka-python`**: Provides `KafkaProducer` and `KafkaConsumer` classes for Apache Kafka integration.
- **`mysql-connector-python`**: Pure Python MySQL driver for connecting to `laundry_ops` database.
- **`pyspark`**: Apache Spark Python API for batch analytics and MLlib model training.
- **`django`**: Web framework powering the DRF API Browser server.
- **`djangorestframework`**: DRF extension providing `APIView`, `Response`, `BrowsableAPIRenderer`.
- **`django-cors-headers`**: Middleware adding CORS headers to DRF responses.
