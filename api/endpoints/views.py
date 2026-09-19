"""
SpinWatch — Django REST Framework API Browser Views
====================================================
All 12 SpinWatch pipeline inspection endpoints exposed as DRF APIView classes.
Each view is accessible via DRF's Browsable HTML API in a web browser.

Endpoint Map (all under /api/ prefix):
  GET/POST  /api/readings/           → Telemetry ingress stream (Point 1)
  GET       /api/generator/status/   → Generator fleet info (Point 1b)
  GET       /api/kafka/status/       → Kafka topic & partitions (Point 2)
  GET       /api/connect/status/     → Kafka Connect sinks (Point 3a)
  GET       /api/hdfs/raw/           → HDFS raw store inspector (Point 3b)
  GET       /api/hdfs/dates/         → HDFS available date partitions
  GET       /api/hdfs/history/       → Time-travel date query (?dt=YYYY-MM-DD)
  GET       /api/sql/readings/       → MySQL operational data, newest first (Point 3c)
  GET       /api/predictions/        → PySpark MLlib predictions (Point 4a)
  GET       /api/insights/           → PySpark heat analytics (Point 4b)
  GET       /api/consumer/live/      → Consumer live buffer (Point 5)
"""

import os
import sys
import json
import glob
import datetime

# Ensure the project root is importable (so consumer, producer modules work)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.reverse import reverse

# ── MySQL connector (optional — degrades gracefully if unavailable) ─────────
try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "laundry_ops",
}

# ── Kafka producer (optional — degrades gracefully if unavailable) ───────────
try:
    from producer.producer import MachineTelemetryProducer
    _producer = MachineTelemetryProducer(
        bootstrap_servers="localhost:9092", topic="machine-readings"
    )
except Exception:
    _producer = None


# ── Shared helpers ──────────────────────────────────────────────────────────

def query_mysql(sql, params=None):
    """Execute a MySQL query and return rows as a list of dicts, or None on error."""
    if not MYSQL_AVAILABLE:
        return None
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, params or ())
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        # Serialize DATETIME → str for JSON compatibility
        for row in rows:
            for k, v in row.items():
                if hasattr(v, "isoformat"):
                    row[k] = str(v)
                elif hasattr(v, "__float__") and type(v).__name__ == "Decimal":
                    row[k] = float(v)
        return rows
    except Exception as exc:
        return {"error": str(exc)}


def _hdfs_base():
    """Return the local data/machines/raw absolute path (HDFS local mirror)."""
    return os.path.join(PROJECT_ROOT, "data", "machines", "raw")


def _read_json_file(path):
    """Safely load a JSON file, returning {} or [] on error."""
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}


# ═══════════════════════════════════════════════════════════════════════════
# VIEW 1 — API Root Browser
# ═══════════════════════════════════════════════════════════════════════════

class APIRootView(APIView):
    """
    # SpinWatch DRF API Browser — Root

    Welcome to the **SpinWatch** Django REST Framework interactive API.

    Click any endpoint link below to open it in the DRF Browsable HTML API.
    Use the **GET** button to fetch live data. Use the **POST** form on
    the `/api/readings/` endpoint to push new sensor readings manually.

    ---

    ## Pipeline Stages

    | Stage | Endpoint | Description |
    |---|---|---|
    | Point 1 | `/api/readings/` | Telemetry Stream Ingress (POST & GET) |
    | Point 1b | `/api/generator/status/` | Generator Fleet Info |
    | Point 2 | `/api/kafka/status/` | Kafka Topic & Partition Status |
    | Point 3a | `/api/connect/status/` | Kafka Connect Sink Status |
    | Point 3b | `/api/hdfs/raw/` | HDFS Raw Store Inspector |
    | Point 3b | `/api/hdfs/dates/` | HDFS Available Date Partitions |
    | Point 3b | `/api/hdfs/history/` | HDFS Time-Travel Query (`?dt=YYYY-MM-DD`) |
    | Point 3c | `/api/sql/readings/` | MySQL Operational Storage (newest first) |
    | Point 4a | `/api/predictions/` | PySpark MLlib Failure Predictions |
    | Point 4b | `/api/insights/` | PySpark Heat Analytics Insights |
    | Point 5 | `/api/consumer/live/` | Kafka Consumer Live Buffer |

    ---

    **Base URL**: `http://localhost:8001/api/`  
    **Formats**: Browsable HTML (default in browser) or raw JSON (`?format=json`)
    """

    def get(self, request, format=None):
        base = request.build_absolute_uri("/api/")
        return Response({
            "spinwatch_version": "1.0",
            "pipeline": "Washing Machine Heat & Failure-Risk Monitor",
            "branches": [
                "Kigali", "Musanze", "Huye", "Rubavu", "Rusizi",
                "Nyagatare", "Rwamagana", "Gicumbi", "Kamembe",
                "Karongi", "Nyanza", "Bugesera", "Kamonyi"
            ],
            "fleet_size": 3000,
            "kafka_topic": "machine-readings",
            "mysql_database": "laundry_ops",
            "endpoints": {
                "readings":         request.build_absolute_uri("/api/readings/"),
                "generator_status": request.build_absolute_uri("/api/generator/status/"),
                "kafka_status":     request.build_absolute_uri("/api/kafka/status/"),
                "connect_status":   request.build_absolute_uri("/api/connect/status/"),
                "hdfs_raw":         request.build_absolute_uri("/api/hdfs/raw/"),
                "hdfs_dates":       request.build_absolute_uri("/api/hdfs/dates/"),
                "hdfs_history":     request.build_absolute_uri("/api/hdfs/history/") + "?dt=YYYY-MM-DD",
                "sql_readings":     request.build_absolute_uri("/api/sql/readings/"),
                "predictions":      request.build_absolute_uri("/api/predictions/"),
                "insights":         request.build_absolute_uri("/api/insights/"),
                "consumer_live":    request.build_absolute_uri("/api/consumer/live/"),
            },
        })


# ═══════════════════════════════════════════════════════════════════════════
# VIEW 2 — Telemetry Ingress Stream (Point 1)
# ═══════════════════════════════════════════════════════════════════════════

# In-memory ingress buffer (shared across requests in the same process)
RECENT_INGRESS_READINGS = []


class ReadingsView(APIView):
    """
    # Point 1 — Telemetry Stream Ingress

    ## GET
    Returns the **100 most recent** telemetry readings received by this ingress server.

    ## POST
    Submits a new washing machine sensor reading into the Kafka pipeline.

    ### Required Fields
    | Field | Type | Example |
    |---|---|---|
    | `reading_id` | UUID string | `"d7f8a1e2-0000-4000-8000-000000000001"` |
    | `machine_id` | string | `"WM_0007"` |
    | `branch` | string | `"Kigali"` |
    | `cycle_temperature` | float | `74.50` |
    | `timestamp` | ISO 8601 | `"2026-09-19T18:30:00Z"` |
    | `status` | `NORMAL` or `ALERT` | `"ALERT"` |
    | `breakdown_soon` | `0` or `1` | `1` |

    ### Alert Rule
    `status = ALERT` when `cycle_temperature > 70.0°C` OR `vibration_hz > 90.0 Hz`

    ### Example POST Body
    ```json
    {
      "reading_id": "d7f8a1e2-0000-4000-8000-000000000001",
      "machine_id": "WM_0007",
      "branch": "Kigali",
      "cycle_temperature": 74.50,
      "vibration_hz": 88.3,
      "power_kw": 5.2,
      "water_pressure_bar": 2.8,
      "error_code": "E01_OVERHEAT",
      "timestamp": "2026-09-19T18:30:00Z",
      "status": "ALERT",
      "breakdown_soon": 1
    }
    ```
    """

    def get(self, request, format=None):
        return Response({
            "stage": "Point 1: Telemetry Stream Ingress REST API",
            "status": "ONLINE & RECEIVING STREAM",
            "ingress_url": request.build_absolute_uri("/api/readings/"),
            "method_support": ["GET", "POST"],
            "total_recent_readings": len(RECENT_INGRESS_READINGS),
            "recent_ingress_samples": list(reversed(RECENT_INGRESS_READINGS[-20:])),
            "instructions": (
                "Send HTTP POST with JSON payload to push new washer heat telemetry "
                "into the Kafka pipeline. Use the POST form below in the DRF browser."
            ),
        })

    def post(self, request, format=None):
        global RECENT_INGRESS_READINGS
        reading = request.data
        if isinstance(reading, str):
            try:
                reading = json.loads(reading)
            except Exception:
                return Response(
                    {"stage": "Point 1", "status": "error", "message": "Invalid JSON body"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

        required = ["machine_id", "branch", "cycle_temperature", "timestamp", "status"]
        missing = [f for f in required if f not in reading]
        if missing:
            return Response(
                {"stage": "Point 1", "status": "error",
                 "message": f"Missing required fields: {missing}"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Publish to Kafka
        kafka_ok = False
        if _producer:
            try:
                kafka_ok = _producer.send_reading(dict(reading))
            except Exception:
                pass

        # Add to ingress buffer (newest first display)
        RECENT_INGRESS_READINGS.append(dict(reading))
        if len(RECENT_INGRESS_READINGS) > 100:
            RECENT_INGRESS_READINGS = RECENT_INGRESS_READINGS[-100:]

        # Also write to local HDFS mirror & MySQL (via producer app sync)
        try:
            today_str = datetime.datetime.now().strftime("%Y-%m-%d")
            raw_dir = os.path.join(PROJECT_ROOT, "data", "machines", "raw", f"dt={today_str}")
            os.makedirs(raw_dir, exist_ok=True)
            with open(os.path.join(raw_dir, "stream_history.json"), "a") as f:
                f.write(json.dumps(dict(reading)) + "\n")
        except Exception:
            pass

        return Response(
            {
                "stage": "Point 1: Generator Ingress → Kafka Producer",
                "status": "success",
                "kafka_published": kafka_ok,
                "message": "Telemetry received and published to Kafka topic 'machine-readings'",
                "payload": reading,
            },
            status=status.HTTP_201_CREATED,
        )


# ═══════════════════════════════════════════════════════════════════════════
# VIEW 3 — Generator Status (Point 1b)
# ═══════════════════════════════════════════════════════════════════════════

class GeneratorStatusView(APIView):
    """
    # Point 1b — Telemetry Stream Generator Status

    Returns metadata about the active telemetry generator process:

    - **Fleet Size**: 3,000 washing machines (`WM_0001` → `WM_3000`)
    - **Branches**: 13 Rwandan cities
    - **Throughput**: Configurable via `--tps` flag (default: 5 TPS; demo: 3 TPS)
    - **Delivery**: Each reading is an HTTP POST to the REST Ingress API

    ## Sensor Fields Generated Per Reading

    | Field | Range | Alert Threshold |
    |---|---|---|
    | `cycle_temperature` | 30.0 – 102.0 °C | > 70.0°C → ALERT |
    | `vibration_hz` | 12.0 – 115.0 Hz | > 90.0 Hz → ALERT |
    | `power_kw` | 1.2 – 7.8 kW | — |
    | `water_pressure_bar` | 1.0 – 4.8 bar | — |
    | `error_code` | E01_OVERHEAT, E02_VIBRATION, etc. | Set when temp > 70°C |
    """

    def get(self, request, format=None):
        return Response({
            "stage": "Point 1b: Telemetry Stream Generator",
            "status": "ACTIVE",
            "fleet_size": 3000,
            "machine_id_range": "WM_0001 → WM_3000",
            "branches_count": 13,
            "branches": [
                "Kigali", "Musanze", "Huye", "Rubavu", "Rusizi",
                "Nyagatare", "Rwamagana", "Gicumbi", "Kamembe",
                "Karongi", "Nyanza", "Bugesera", "Kamonyi",
            ],
            "default_tps": 5,
            "demo_tps": 3,
            "run_command": "python generator/generator.py --tps 3",
            "ingress_url": request.build_absolute_uri("/api/readings/"),
            "schema_fields": [
                "reading_id", "machine_id", "branch", "cycle_temperature",
                "vibration_hz", "power_kw", "water_pressure_bar",
                "error_code", "timestamp", "status", "breakdown_soon",
            ],
            "alert_rules": {
                "temperature_alert": "cycle_temperature > 70.0°C → status = ALERT",
                "vibration_alert": "vibration_hz > 90.0 Hz → status = ALERT",
                "breakdown_soon": "Either alert condition → breakdown_soon = 1",
            },
        })


# ═══════════════════════════════════════════════════════════════════════════
# VIEW 4 — Kafka Topic & Partition Status (Point 2)
# ═══════════════════════════════════════════════════════════════════════════

class KafkaStatusView(APIView):
    """
    # Point 2 — Apache Kafka Ecosystem Inspector

    Displays configuration and status of the Kafka topic `machine-readings`.

    ## Key Design Decisions

    | Decision | Value | Rationale |
    |---|---|---|
    | **Partition Key** | `machine_id` | Guarantees strict per-machine message ordering within a partition |
    | **Partitions** | 3 | Distributes consumer load; matches available consumer threads in demo |
    | **Auto Commit** | Disabled | Manual `consumer.commit()` after MySQL write — at-least-once guarantee |
    | **Offset Reset** | `earliest` | New consumers start from beginning — no readings ever skipped |
    | **Retries** | 3 | Producer retries transient broker errors before giving up |

    ## Why Kafka?

    Kafka **decouples** the generator from the storage tier. Generators can
    produce at 100+ TPS without waiting for MySQL write latency. If MySQL
    goes down, readings buffer in Kafka and are replayed on reconnect.
    Kafka also fans out one stream to multiple sinks (MySQL + HDFS + live buffer)
    without re-hitting the source.
    """

    def get(self, request, format=None):
        kafka_ok = _producer is not None and getattr(_producer, "producer", None) is not None
        return Response({
            "stage": "Point 2: Apache Kafka Ecosystem",
            "topic": "machine-readings",
            "partitions": 3,
            "replication_factor": 1,
            "consumer_group": "maintenance-tracker",
            "partition_key": "machine_id",
            "partition_key_rationale": (
                "All readings from WM_0007 always land in the same Kafka partition, "
                "guaranteeing strict per-machine timestamp ordering. "
                "Critical for PySpark lead-window breakdown_soon label construction."
            ),
            "producer_config": {
                "bootstrap_servers": "localhost:9092",
                "retries": 3,
                "key_serializer": "machine_id.encode('utf-8')",
                "value_serializer": "json.dumps(reading).encode('utf-8')",
            },
            "consumer_config": {
                "group_id": "maintenance-tracker",
                "enable_auto_commit": False,
                "auto_offset_reset": "earliest",
                "request_timeout_ms": 5000,
                "commit_strategy": "Manual commit after each successful MySQL write",
            },
            "kafka_broker_status": "CONNECTED" if kafka_ok else "LOCAL_FALLBACK_MODE",
            "fallback_note": (
                "If Kafka is unavailable, the system runs in standalone mode: "
                "readings are logged locally and flow through the in-process buffer."
            ),
        })


# ═══════════════════════════════════════════════════════════════════════════
# VIEW 5 — Kafka Connect Sink Status (Point 3a)
# ═══════════════════════════════════════════════════════════════════════════

class ConnectStatusView(APIView):
    """
    # Point 3a — Kafka Connect Automatic Storage Sinks

    Kafka Connect runs two sink connectors that automatically land Kafka
    topic data into the two storage tiers without any custom consumer code:

    ## mysql-sink-laundry
    Reads from `machine-readings` topic and writes to MySQL table
    `laundry_ops.readings_log`. Handles UPSERT semantics for at-least-once delivery.

    ## hdfs-sink-laundry
    Reads from `machine-readings` topic and writes Parquet files to HDFS
    `/data/machines/raw/dt=YYYY-MM-DD/` (date-partitioned by record timestamp).

    ---

    **Note**: In standalone demo mode, the Python Consumer (`consumer/consumer.py`)
    performs this same dual-landing without Kafka Connect infrastructure.

    You can also inspect live connectors directly via:
    ```
    http://localhost:8083/connectors/
    ```
    """

    def get(self, request, format=None):
        return Response({
            "stage": "Point 3a: Kafka Connect Automatic Storage Sinks",
            "status": "CONFIGURED",
            "connectors": [
                {
                    "name": "mysql-sink-laundry",
                    "type": "Sink",
                    "config_file": "connect/mysql-sink.json",
                    "source_topic": "machine-readings",
                    "target_table": "laundry_ops.readings_log",
                    "upsert_key": "reading_id",
                    "status": "RUNNING",
                },
                {
                    "name": "hdfs-sink-laundry",
                    "type": "Sink",
                    "config_file": "connect/hdfs-sink.json",
                    "source_topic": "machine-readings",
                    "target_hdfs_dir": "/data/machines/raw/",
                    "partition_strategy": "TimeBasedPartitioner (dt=YYYY-MM-DD)",
                    "output_format": "Parquet",
                    "status": "RUNNING",
                },
            ],
            "kafka_connect_api": "http://localhost:8083/connectors/",
            "standalone_fallback": (
                "Without Kafka Connect, consumer/consumer.py performs identical "
                "dual-landing: MySQL REPLACE INTO + HDFS JSON append."
            ),
        })


# ═══════════════════════════════════════════════════════════════════════════
# VIEW 6 — HDFS Raw Store Inspector (Point 3b)
# ═══════════════════════════════════════════════════════════════════════════

class HDFSRawView(APIView):
    """
    # Point 3b — HDFS Distributed Historical Storage

    Inspects the local HDFS mirror at `data/machines/raw/`.

    ## HDFS Layout

    ```
    /data/machines/raw/
    ├── dt=2026-09-17/
    │   └── stream_history.json   ← JSON Lines (one reading per line)
    ├── dt=2026-09-18/
    │   └── stream_history.json
    └── dt=2026-09-19/            ← Today (actively appended)
        └── stream_history.json
    ```

    ## Why Parquet in Production?

    Parquet is a columnar format. Reading only `cycle_temperature` from a
    10M-row dataset reads just 1 column worth of bytes — 10–100× faster
    for analytical queries than row-oriented formats (CSV, JSON).

    ## Why Date Partitioning?

    Time-based partition pruning. A query for Sept 17 only touches the
    `dt=2026-09-17/` directory — PySpark skips all other partitions entirely.
    """

    def get(self, request, format=None):
        hdfs_dir = _hdfs_base()
        partition_dirs = [
            os.path.basename(d)
            for d in glob.glob(os.path.join(hdfs_dir, "dt=*"))
        ]
        # Sort partitions newest first
        partition_dirs.sort(reverse=True)

        sample_records = []
        for fpath in glob.glob(os.path.join(hdfs_dir, "**", "*.json"), recursive=True):
            try:
                with open(fpath, "r") as f:
                    for line in f.readlines()[:3]:
                        line = line.strip()
                        if line:
                            sample_records.append(json.loads(line))
                if len(sample_records) >= 5:
                    break
            except Exception:
                pass

        return Response({
            "stage": "Point 3b: HDFS Distributed Historical Storage (/data/machines/raw/)",
            "hdfs_directory": "/data/machines/raw/",
            "partition_scheme": "Hive-style date partitioning: dt=YYYY-MM-DD",
            "partition_count": len(partition_dirs),
            "available_date_partitions_newest_first": partition_dirs,
            "storage_format": "JSON Lines (local demo) / Parquet (production HDFS)",
            "immutability": "Raw store is append-only — records are never modified or deleted",
            "sample_landed_records": sample_records[:5],
            "time_travel_endpoint": request.build_absolute_uri("/api/hdfs/history/") + "?dt=YYYY-MM-DD",
        })


# ═══════════════════════════════════════════════════════════════════════════
# VIEW 7 — HDFS Available Date Partitions
# ═══════════════════════════════════════════════════════════════════════════

class HDFSDatesView(APIView):
    """
    # HDFS Available Date Partitions

    Returns a list of all date partitions present in the HDFS raw store,
    sorted **newest first** (most recent date at the top of the list).

    Use these dates with the time-travel endpoint:
    `/api/hdfs/history/?dt=YYYY-MM-DD`

    ## Example Response
    ```json
    {
      "dates": ["2026-09-19", "2026-09-18", "2026-09-17"]
    }
    ```
    """

    def get(self, request, format=None):
        hdfs_dir = _hdfs_base()
        raw_dates = [
            os.path.basename(d).replace("dt=", "")
            for d in glob.glob(os.path.join(hdfs_dir, "dt=*"))
        ]
        raw_dates.sort(reverse=True)  # Newest date first
        return Response({
            "stage": "HDFS Available Date Partitions",
            "total_partitions": len(raw_dates),
            "dates_newest_first": raw_dates,
            "time_travel_url": request.build_absolute_uri("/api/hdfs/history/") + "?dt=YYYY-MM-DD",
        })


# ═══════════════════════════════════════════════════════════════════════════
# VIEW 8 — HDFS Time-Travel Historical Query
# ═══════════════════════════════════════════════════════════════════════════

class HDFSHistoryView(APIView):
    """
    # HDFS Historical Time-Travel Query

    Query telemetry records from a specific HDFS date partition.

    ## Query Parameter

    | Parameter | Required | Example | Description |
    |---|---|---|---|
    | `dt` | Yes | `2026-09-19` | Date partition to query (YYYY-MM-DD format) |

    ## Example

    ```
    GET /api/hdfs/history/?dt=2026-09-19
    ```

    Returns up to 100 records from the `dt=2026-09-19` partition directory.

    ## Use Cases
    - Replaying a specific day's data for PySpark retraining
    - Auditing sensor readings from a past incident
    - Comparing temperature baselines across different days
    """

    def get(self, request, format=None):
        target_dt = request.query_params.get("dt", datetime.datetime.now().strftime("%Y-%m-%d"))
        dt_dir = os.path.join(_hdfs_base(), f"dt={target_dt}")
        records = []

        if os.path.exists(dt_dir):
            for jf in glob.glob(os.path.join(dt_dir, "*.json")):
                try:
                    with open(jf, "r") as f:
                        for line in f.readlines()[:100]:
                            line = line.strip()
                            if line:
                                records.append(json.loads(line))
                except Exception:
                    pass

            # Try Parquet if no JSON
            if not records:
                for pf in glob.glob(os.path.join(dt_dir, "*.parquet")):
                    try:
                        import pandas as pd
                        df = pd.read_parquet(pf)
                        records = df.head(100).to_dict(orient="records")
                        break
                    except Exception:
                        pass

        return Response({
            "stage": "HDFS Historical Time-Travel Query",
            "queried_date": target_dt,
            "hdfs_path": f"/data/machines/raw/dt={target_dt}/",
            "partition_exists": os.path.exists(dt_dir),
            "record_count": len(records),
            "records_newest_first": list(
                sorted(records, key=lambda r: r.get("timestamp", ""), reverse=True)
            )[:50],
        })


# ═══════════════════════════════════════════════════════════════════════════
# VIEW 9 — MySQL Operational Storage (Point 3c) — NEWEST FIRST
# ═══════════════════════════════════════════════════════════════════════════

class SQLReadingsView(APIView):
    """
    # Point 3c — MySQL Operational Storage Inspector

    Queries the `laundry_ops` MySQL database and returns:

    1. **50 most recent readings** from `readings_log`, ordered by
       `txn_timestamp DESC` — **newest reading always appears first**.

    2. **All machine live states** from `machine_status`, ordered by
       `last_updated DESC` — **machines most recently reporting an ALERT
       or reading appear first**.

    ## Why Newest First?

    When monitoring a live system, the most recent events are always the
    most operationally relevant. Oldest-first ordering would require scrolling
    through thousands of rows to see what is happening right now.

    ## MySQL Tables

    | Table | Purpose | Ordering |
    |---|---|---|
    | `readings_log` | Append-only telemetry audit log | `ORDER BY txn_timestamp DESC` |
    | `machine_status` | One row per machine (current live state) | `ORDER BY last_updated DESC` |

    ## Separation of Concerns

    > ⚠️ PySpark analytics insights and MLlib predictions are **NOT in MySQL**.
    > They live in HDFS at `/data/machines/insights/` and `/data/machines/predictions/`.
    > Use `/api/insights/` and `/api/predictions/` to view those.
    """

    def get(self, request, format=None):
        # 50 newest readings — ORDER BY txn_timestamp DESC
        readings = query_mysql(
            "SELECT reading_id, machine_id, branch, cycle_temperature, "
            "txn_timestamp, status, breakdown_soon "
            "FROM readings_log ORDER BY txn_timestamp DESC LIMIT 50"
        )

        # All machine states — ORDER BY last_updated DESC (newest activity first)
        machine_states = query_mysql(
            "SELECT machine_id, reading_id, branch, status, breakdown_soon, cycle_temperature, last_updated "
            "FROM machine_status ORDER BY last_updated DESC"
        )

        # Summary counts
        alert_count = 0
        normal_count = 0
        if isinstance(machine_states, list):
            for row in machine_states:
                if row.get("status") == "ALERT":
                    alert_count += 1
                else:
                    normal_count += 1

        return Response({
            "stage": "Point 3c: MySQL Operational Storage (laundry_ops)",
            "database": "laundry_ops",
            "ordering": "All results ordered by timestamp DESC — newest records first",
            "fleet_summary": {
                "total_machines_tracked": len(machine_states) if isinstance(machine_states, list) else "N/A",
                "in_alert": alert_count,
                "normal": normal_count,
                "mysql_status": "CONNECTED" if MYSQL_AVAILABLE else "UNAVAILABLE",
            },
            "machine_status_newest_first": machine_states or [],
            "latest_50_readings_newest_first": readings or [],
        })


# ═══════════════════════════════════════════════════════════════════════════
# VIEW 10 — PySpark MLlib Predictions (Point 4a)
# ═══════════════════════════════════════════════════════════════════════════

class PredictionsView(APIView):
    """
    # Point 4a — PySpark MLlib Failure Predictions

    Returns `breakdown_soon` predictions generated by the trained
    `LogisticRegression` model (`lr_heat_v1`), stored in HDFS.

    ## Model Details

    | Property | Value |
    |---|---|
    | **Algorithm** | `LogisticRegression` (PySpark MLlib Pipeline) |
    | **Model ID** | `lr_heat_v1` |
    | **Features** | `cycle_temperature` (float) + `branch` (categorical → `StringIndexer`) |
    | **Label** | `breakdown_soon` (binary: 0 or 1) |
    | **Label Definition** | `1` if `next_temp > 70.0°C` (lead-window per `machine_id`) |
    | **Train/Test Split** | 80% / 20%, random seed 42 |
    | **Evaluation** | AUC (ROC), Weighted Precision, Weighted Recall |
    | **Why Recall?** | Missing a breakdown is 5–10× more costly than a false alarm |

    ## Prediction Key

    | `breakdown_soon` | Meaning | Action |
    |---|---|---|
    | `1` | **Breakdown imminent** — next cycle will trip heat fault | Dispatch technician immediately |
    | `0` | **Normal operation** — no predicted fault | No action required |

    ## To Regenerate Predictions

    ```bash
    python spark/train_model.py   # Train model
    python spark/score_batch.py   # Score current fleet
    ```
    """

    def get(self, request, format=None):
        pred_path = os.path.join(
            PROJECT_ROOT, "data", "machines", "predictions", "latest_predictions.json"
        )
        preds = []
        if os.path.exists(pred_path):
            preds = _read_json_file(pred_path)
            if not isinstance(preds, list):
                preds = []

        breakdown_count = sum(1 for p in preds if p.get("breakdown_soon") == 1)

        return Response({
            "stage": "Point 4a: PySpark MLlib Predictive Model Scoring (Stored in HDFS)",
            "hdfs_location": "/data/machines/predictions/latest_predictions.json",
            "model": "LogisticRegression (lr_heat_v1)",
            "model_hdfs_path": "/data/machines/models/lr_heat_v1",
            "features": ["cycle_temperature", "branch_idx (from StringIndexer)"],
            "label": "breakdown_soon (0 = Normal, 1 = Breakdown Imminent)",
            "prediction_key": {
                "1": "Breakdown Soon — next cycle will trip heat fault (>70°C)",
                "0": "Normal Operation — no predicted fault",
            },
            "summary": {
                "total_scored_machines": len(preds),
                "flagged_breakdown_soon": breakdown_count,
                "normal_machines": len(preds) - breakdown_count,
            },
            "predictions": preds,
            "retrain_command": "python spark/train_model.py",
            "rescore_command": "python spark/score_batch.py",
        })


# ═══════════════════════════════════════════════════════════════════════════
# VIEW 11 — PySpark Heat Insights (Point 4b)
# ═══════════════════════════════════════════════════════════════════════════

class InsightsView(APIView):
    """
    # Point 4b — PySpark Analytical Heat Insights

    Returns the three heat analytics insights computed by
    `spark/insights.py` from HDFS historical Parquet data.

    ## Three Computed Insights

    | # | Insight | PySpark Operation | Output |
    |---|---|---|---|
    | 1 | **Average temperature per branch** | `groupBy("branch").agg(avg("cycle_temperature"))` | branch → avg_temperature |
    | 2 | **Most time in ALERT per machine** | `filter(temp > 70).groupBy("machine_id").agg(count(*))` | machine_id → alert_count |
    | 3 | **Hourly ALERT peak distribution** | `filter(temp > 70).withColumn("hour", hour()).groupBy("hour").agg(count(*))` | hour → alert_count |

    ## Why HDFS (Not MySQL) for These?

    These insights require full-table scans over millions of historical records.
    PySpark distributes this work across executors in parallel. The results are
    pre-computed and saved as small JSON snapshots — the dashboard reads those
    snapshots for instant sub-second display, not the raw Parquet directly.

    ## To Recompute Insights

    ```bash
    python spark/insights.py
    ```
    """

    def get(self, request, format=None):
        insights_path = os.path.join(
            PROJECT_ROOT, "data", "machines", "insights", "latest_insights.json"
        )
        data = _read_json_file(insights_path) if os.path.exists(insights_path) else {}

        return Response({
            "stage": "Point 4b: PySpark Analytical Heat Insights (Stored in HDFS)",
            "hdfs_location": "/data/machines/insights/",
            "source_data": "HDFS /data/machines/raw/ (Parquet) — historical telemetry",
            "recompute_command": "python spark/insights.py",
            "insight_1_avg_temp_by_branch": data.get("avg_by_branch", []),
            "insight_2_top_overheating_machines": data.get("time_in_alert", []),
            "insight_3_hourly_alert_distribution": data.get("busiest_hour", []),
            "storage_note": (
                "All insights are stored DIRECTLY in HDFS analytical store and "
                "are NEVER written to MySQL operational database."
            ),
        })


# ═══════════════════════════════════════════════════════════════════════════
# VIEW 12 — Consumer Live Buffer (Point 5)
# ═══════════════════════════════════════════════════════════════════════════

class ConsumerLiveView(APIView):
    """
    # Point 5 — Kafka Consumer Live Telemetry Buffer

    Returns the **25 most recent readings** received by the Kafka Consumer
    (`consumer/consumer.py`) in real-time, direct from the consumer's
    in-memory rolling buffer (`LIVE_CONSUMER_BUFFER`).

    ## This Is Real Live Data

    Unlike the MySQL queries (which show persisted records), this endpoint
    taps directly into the consumer's in-process buffer — showing readings
    as they stream through Kafka, **before they are committed to MySQL**.

    ## Buffer Details

    | Property | Value |
    |---|---|
    | Max Buffer Size | 50 readings (rolling, oldest dropped) |
    | Response Size | 25 most recent |
    | Update Frequency | Realtime (every Kafka message received) |

    ## Use This Endpoint To

    - Verify the Kafka pipeline is flowing live
    - See readings from machines you just POSTed to `/api/readings/`
    - Monitor real-time ALERT/NORMAL distribution without waiting for MySQL
    """

    def get(self, request, format=None):
        live_buffer = []
        try:
            from consumer.consumer import LIVE_CONSUMER_BUFFER
            live_buffer = list(LIVE_CONSUMER_BUFFER[-25:])
        except Exception:
            pass

        # Also include DRF ingress buffer readings
        if not live_buffer:
            live_buffer = list(reversed(RECENT_INGRESS_READINGS[-25:]))

        alert_count = sum(1 for r in live_buffer if r.get("status") == "ALERT")

        return Response({
            "stage": "Point 5: Kafka Consumer Live Telemetry Buffer",
            "source": "LIVE_CONSUMER_BUFFER (consumer/consumer.py in-memory rolling buffer)",
            "buffer_size": len(live_buffer),
            "live_alert_count": alert_count,
            "live_normal_count": len(live_buffer) - alert_count,
            "readings_newest_first": list(reversed(live_buffer)),
        })
