# SpinWatch — Nationwide Washing Machine Heat & Failure-Risk Monitor

> **Big Data Essentials Group Final Exam Project**
> **Submitted By**: SpinWatch Engineering Team
> **Core Focus**: Machine Heat / Temperature Monitoring (`cycle_temperature`), Performing Time (`timestamp`), Failure Prediction (`breakdown_soon`), and Branch Analytics across Rwanda.

---

## Table of Contents

1. [Executive Summary & Case Framing](#1-executive-summary--case-framing)
2. [Refined Schema — The Source Table](#2-refined-schema--the-source-table)
3. [MySQL Operational Tables — Full Structure](#3-mysql-operational-tables--full-structure)
4. [End-to-End System Architecture](#4-end-to-end-system-architecture)
5. [Component-by-Component Breakdown](#5-component-by-component-breakdown)
6. [Kafka Configuration Deep-Dive](#6-kafka-configuration-deep-dive)
7. [HDFS Storage Layout & Partitioning](#7-hdfs-storage-layout--partitioning)
8. [PySpark Analytics & MLlib Machine Learning Pipeline](#8-pyspark-analytics--mllib-machine-learning-pipeline)
9. [REST API Endpoint Reference (All 11 Endpoints)](#9-rest-api-endpoint-reference-all-11-endpoints)
10. [Django REST Framework API Browser](#10-django-rest-framework-api-browser)
11. [Setup & Running Instructions](#11-setup--running-instructions)
12. [Web Dashboard Features](#12-web-dashboard-features)
13. [Colleague Q&A Cheat Sheet](#13-colleague-qa-cheat-sheet)

---

## 1. Executive Summary & Case Framing

**SpinWatch** is a production-grade IoT Big Data monitoring and predictive maintenance platform built for a nationwide coin-operated and app-operated laundry business. The operator runs **3,000 washing machines** distributed across **13 branch cities** throughout Rwanda:

> **Kigali, Musanze, Huye, Rubavu, Rusizi, Nyagatare, Rwamagana, Gicumbi, Kamembe, Karongi, Nyanza, Bugesera, Kamonyi**

### The Business Problem

Commercial washing machines run 14–18 hours per day in high-traffic locations. Motor and heating element failures are the #1 cause of service disruptions. These failures share a common early symptom: **motor/water temperature begins rising above normal operating range** several cycles before complete breakdown. Without monitoring:

- Technicians only discover faults after customers complain (reactive maintenance)
- Repair costs are 3–5× higher on fully failed machines vs. early intervention
- Downtown branches lose revenue for hours awaiting technician dispatch

### How SpinWatch Solves This

1. **Streams Heat Telemetry Continuously**: Every washing machine streams 7 live sensor readings (`cycle_temperature`, `vibration_hz`, `power_kw`, `water_pressure_bar`, `error_code`, `status`, `breakdown_soon`) to a central REST API in real time.
2. **Evaluates Heat Faults Instantly**: Automatically assigns `status = ALERT` whenever `cycle_temperature > 70.0°C` — the safe operating threshold for commercial washer motors.
3. **Predicts Impending Breakdown Proactively**: Uses a trained PySpark MLlib `LogisticRegression` model to predict whether a machine will trip the heat fault threshold in the **next reading cycle** (`breakdown_soon = 1`), enabling pre-emptive technician dispatch.
4. **Enforces Strict Separation of Operational vs Analytical Storage**:
   - **Local MySQL (`laundry_ops`)**: Operational storage ONLY — holds live machine states (`machine_status`) and a recent readings log (`readings_log`). Queried by the dashboard in under 5ms.
   - **Local HDFS (`/data/machines/raw/dt=YYYY-MM-DD/`)**: Historical analytical storage — holds the complete, immutable record of every sensor reading ever received, partitioned by date, in Parquet/JSON format.
   - **PySpark HDFS Analytical Store (`/data/machines/insights/` & `/data/machines/predictions/`)**: PySpark analytics results and MLlib model predictions are saved **DIRECTLY to HDFS** and are **NEVER stored in MySQL**, maintaining strict separation of concerns.

---

## 2. Refined Schema — The Source Table

Every telemetry reading that flows through the Generator → REST API → Kafka → Consumer → MySQL/HDFS follows this exact unified schema. Each field is explained in detail below:

| Column | SQL Type | Description |
| :--- | :--- | :--- |
| `reading_id` | `CHAR(36)` (UUID v4) | **Globally unique identifier** for each individual telemetry reading. Generated using Python `uuid.uuid4()` at the generator. Format: `d7f8a1e2-0000-4000-8000-000000000001`. Used as the PRIMARY KEY in `readings_log` to guarantee idempotent inserts (`ON DUPLICATE KEY UPDATE`). |
| `machine_id` | `VARCHAR(20)` | **Washer unit identifier**. Formatted as `WM_XXXX` (zero-padded 4 digits, e.g., `WM_0007`, `WM_2999`). 3,000 machines exist fleet-wide (`WM_0001` through `WM_3000`). Also used as the **Kafka partition key** to guarantee strict per-machine message ordering within a Kafka topic partition. |
| `branch` | `VARCHAR(50)` | **City / geographic branch location** where the washing machine is installed. One of 13 Rwandan cities: `Kigali`, `Musanze`, `Huye`, `Rubavu`, `Rusizi`, `Nyagatare`, `Rwamagana`, `Gicumbi`, `Kamembe`, `Karongi`, `Nyanza`, `Bugesera`, `Kamonyi`. Used for branch-level heat analytics and dashboard branch filtering. |
| `cycle_temperature` | `DECIMAL(5,2)` | **Water / motor temperature during active wash cycle** (°C). The single most critical operational metric. Range: `30.0°C` (cold rinse) to `102.0°C` (severe overheat, motor burnout risk). **Threshold Rule**: `cycle_temperature > 70.0°C` → `status = ALERT` and `breakdown_soon = 1`. Stored as `DECIMAL(5,2)` for precise two-decimal precision (e.g., `74.35`). |
| `vibration_hz` | `FLOAT` | **Motor vibration frequency in Hertz**. Measures drum bearing and motor balance. Normal range: `12.0–90.0 Hz`. `vibration_hz > 90.0 Hz` indicates bearing wear or unbalanced load and **also triggers `status = ALERT`**. A machine showing combined high temperature AND high vibration has dramatically elevated breakdown risk. |
| `power_kw` | `FLOAT` | **Electrical power draw in kilowatts**. Normal operating range: `1.2–7.8 kW`. Sudden spikes in power consumption alongside high temperature indicate motor strain. Monitored for variety analytics but not used as a primary fault threshold in v1. |
| `water_pressure_bar` | `FLOAT` | **Water intake pressure in bar**. Normal range: `1.0–4.8 bar`. Sustained low pressure indicates blocked inlet filters or supply issues, which can cause water-starved overheating cycles. Range `1.0–4.8 bar` is typical for commercial installations fed from a main supply. |
| `error_code` | `VARCHAR(30)` | **Machine-reported fault code** from the washer's internal diagnostics module. Values: `NORMAL` (no fault), `E01_OVERHEAT`, `E02_VIBRATION`, `E03_PRESSURE_DROP`, `E04_POWER_SURGE`, `LEAKAGE`, `NO WATER`, `DEMAGED`, `UNKNOWN`. Only populated when `cycle_temperature > 70.0°C` or vibration is high. |
| `timestamp` | `DATETIME` / ISO 8601 | **UTC timestamp** of when the sensor reading was recorded on the machine. Stored as ISO 8601 string (`2026-09-19T18:30:00Z`) in Kafka/HDFS and converted to `DATETIME` format (`2026-09-19 18:30:00`) for MySQL insertion. Used for time-travel queries on HDFS date partitions. |
| `status` | `VARCHAR(10)` | **Computed operational status** of the machine at the moment of the reading. **Two possible values only**: `NORMAL` (all sensors within safe range) or `ALERT` (`cycle_temperature > 70.0°C` OR `vibration_hz > 90.0 Hz`). Computed by the generator and re-validated by the consumer before MySQL write. |
| `breakdown_soon` | `TINYINT(1)` (0 or 1) | **Predictive failure label.** `1` = This machine is predicted to trip the heat fault threshold (`cycle_temperature > 70.0°C`) in its **next reading cycle**, indicating imminent breakdown risk. `0` = Normal predicted operation. Computed by the generator (heuristic) and refined by the PySpark MLlib `LogisticRegression` model (ML-derived). Used by the dashboard to display red warning banners. |

---

## 3. MySQL Operational Tables — Full Structure

MySQL database `laundry_ops` holds **two operational tables only**. These tables store live operational state and a rolling recent-readings log. They are deliberately kept small and fast for sub-second dashboard queries.

### Table 1: `machine_status` — Live Operational State

```sql
CREATE TABLE IF NOT EXISTS machine_status (
    machine_id    VARCHAR(20)   PRIMARY KEY,         -- Washer unit ID (e.g., WM_0007)
    branch        VARCHAR(50)   NOT NULL,             -- City/branch location
    cycle_temperature DECIMAL(5,2) NOT NULL,          -- Latest temperature reading (°C)
    status        VARCHAR(10)   NOT NULL,             -- 'NORMAL' or 'ALERT'
    last_updated  DATETIME      NOT NULL,             -- Timestamp of last update (ORDER BY this DESC)
    INDEX idx_last_updated (last_updated),            -- Fast descending sort for dashboard
    INDEX idx_status (status)                        -- Fast filtering by ALERT/NORMAL
);
```

- **One row per machine** (`machine_id` is the PRIMARY KEY). There are up to 3,000 rows maximum.
- Updated via `REPLACE INTO` (upsert) on every incoming reading — no duplicates ever accumulate.
- **Ordered by `last_updated DESC`** in all dashboard and API queries — the machine most recently reporting an ALERT always appears first.
- The dashboard's "MySQL Operational Storage View" reads exclusively from this table.

### Table 2: `readings_log` — Rolling Telemetry Audit Log

```sql
CREATE TABLE IF NOT EXISTS readings_log (
    reading_id        CHAR(36)     PRIMARY KEY,       -- UUID v4, globally unique reading identifier
    machine_id        VARCHAR(20)  NOT NULL,          -- Foreign reference to machine_status.machine_id
    branch            VARCHAR(50)  NOT NULL,          -- City/branch (denormalized for query speed)
    cycle_temperature DECIMAL(5,2) NOT NULL,          -- Temperature at time of reading (°C)
    txn_timestamp     DATETIME     NOT NULL,          -- Exact UTC timestamp of the reading
    status            VARCHAR(10)  NOT NULL,          -- 'NORMAL' or 'ALERT'
    breakdown_soon    TINYINT      DEFAULT 0,         -- 1 = Predicted imminent failure
    INDEX idx_txn_timestamp (txn_timestamp),          -- Fast descending sort (newest first)
    INDEX idx_machine_id (machine_id),               -- Fast per-machine filtering
    INDEX idx_status (status)                        -- Fast ALERT filtering
);
```

- **One row per individual reading.** Grows continuously as the pipeline runs.
- Inserted via `ON DUPLICATE KEY UPDATE` (idempotent) to handle at-least-once Kafka delivery without creating duplicate rows.
- **Always queried `ORDER BY txn_timestamp DESC`** so the most recent readings appear at the top of the API response and dashboard table.
- The API endpoint `/api/sql/readings` returns the 50 most recent rows from this table.

> **Important**: PySpark analytics insights (`/data/machines/insights/`) and MLlib model predictions (`/data/machines/predictions/`) are stored **directly in HDFS** and are **never written to MySQL**. This maintains strict separation between operational and analytical storage tiers.

---

## 4. End-to-End System Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                           SPINWATCH BIG DATA PIPELINE                                │
│                                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐                    │
│  │          TIER 1: DATA GENERATION & INGRESS (Port 8000)      │                    │
│  │                                                             │                    │
│  │   [Python Telemetry Generator]   →   [REST Ingress API]     │                    │
│  │   3,000 Washers × 13 Branches        POST /api/readings/    │                    │
│  │   7 Sensor Fields per Reading        Port 8000              │                    │
│  └────────────────────────────┬────────────────────────────────┘                    │
│                               │ JSON over HTTP POST                                  │
│  ┌────────────────────────────▼────────────────────────────────┐                    │
│  │          TIER 2: KAFKA MESSAGE BUS                          │                    │
│  │                                                             │                    │
│  │   Topic: "machine-readings"                                 │                    │
│  │   Partitions: 3   Replication: 1                           │                    │
│  │   Partition Key: machine_id (strict per-washer ordering)    │                    │
│  │   Consumer Group: "maintenance-tracker"                     │                    │
│  └─────────────┬──────────────────────────────────┬───────────┘                    │
│                │ Kafka Python Consumer              │ Kafka Connect / Stream Writer  │
│  ┌─────────────▼──────────────┐   ┌────────────────▼──────────────────────────┐    │
│  │  TIER 3a: OPERATIONAL DB   │   │  TIER 3b: HDFS HISTORICAL STORAGE         │    │
│  │                            │   │                                            │    │
│  │  MySQL: laundry_ops        │   │  /data/machines/raw/                       │    │
│  │  ├── machine_status        │   │  └── dt=YYYY-MM-DD/                        │    │
│  │  │   (1 row/machine,       │   │      └── stream_history.json               │    │
│  │  │    REPLACE INTO upsert, │   │          (Parquet / JSON, date-partitioned) │    │
│  │  │    newest first)        │   │                                            │    │
│  │  └── readings_log          │   │  Immutable raw record of ALL readings      │    │
│  │      (append-only log,     │   │  ever received. Used for PySpark batch.    │    │
│  │       txn_timestamp DESC)  │   │                                            │    │
│  └────────────────────────────┘   └──────────────────┬─────────────────────────┘    │
│                                                       │ PySpark Batch Jobs           │
│                                   ┌───────────────────▼──────────────────────────┐  │
│                                   │  TIER 4: PYSPARK ANALYTICAL STORE (HDFS)     │  │
│                                   │                                              │  │
│                                   │  /data/machines/insights/                    │  │
│                                   │  └── latest_insights.json                   │  │
│                                   │      (avg_by_branch, time_in_alert,         │  │
│                                   │       busiest_hour)                         │  │
│                                   │                                              │  │
│                                   │  /data/machines/predictions/                 │  │
│                                   │  └── latest_predictions.json                │  │
│                                   │      (breakdown_soon per machine_id)        │  │
│                                   │                                              │  │
│                                   │  /data/machines/models/lr_heat_v1/          │  │
│                                   │      (Trained LogisticRegression artifact)   │  │
│                                   └──────────────────────────────────────────────┘  │
│                                                                                      │
│  ┌───────────────────────────────────────────────────────────────────────────────┐  │
│  │                    TIER 5: ACCESS & VISUALIZATION                             │  │
│  │                                                                               │  │
│  │  [Web Dashboard]  Port 8050    ←→  MySQL + HDFS Analytics                    │  │
│  │  [Plain REST API] Port 8000    ←→  All pipeline inspection endpoints          │  │
│  │  [DRF API Browser] Port 8001   ←→  All endpoints + Browsable HTML API        │  │
│  └───────────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Component-by-Component Breakdown

### Component 1: Telemetry Generator (`generator/generator.py`)

The generator simulates the continuous IoT sensor stream from the entire washer fleet.

- **Fleet Size**: 3,000 machines (`WM_0001` → `WM_3000`), each randomly assigned to one of 13 branches at startup.
- **Throughput**: Configurable via `--tps` flag (default: 5 readings/second). At 3 TPS used in demo mode: `python generator/generator.py --tps 3`
- **Sensor Fields Generated per Reading**:
  - `cycle_temperature`: `random.uniform(30.0, 102.0)` — realistic thermal drift simulation
  - `vibration_hz`: `random.uniform(12.0, 115.0)` — motor bearing vibration simulation
  - `power_kw`: `random.uniform(1.2, 7.8)` — electrical load simulation
  - `water_pressure_bar`: `random.uniform(1.0, 4.8)` — water supply simulation
  - `error_code`: Assigned from fault code list when temp > 70°C, else `"NORMAL"`
- **Alert Logic**: `status = ALERT` if `cycle_temperature > 70.0` OR `vibration_hz > 90.0`
- **Delivery**: HTTP POST to `http://localhost:8000/api/readings/` — each reading is a complete JSON object

### Component 2: REST Ingress & Inspection API (`producer/app.py`)

A Python `http.server`-based REST API running on **Port 8000**. Serves two roles:

**Role A — Ingress Server**: Receives telemetry from the Generator via `POST /api/readings/`, validates required fields, and immediately publishes the reading to the Kafka topic `machine-readings` via the `MachineTelemetryProducer`. Also auto-syncs readings into the Consumer buffer, HDFS local file, and MySQL for single-process demo mode.

**Role B — Pipeline Inspector**: Exposes 11 `GET` endpoints so developers can inspect data at every stage of the pipeline (Generator → Kafka → MySQL → HDFS → PySpark) without needing Postman.

### Component 3: Kafka Producer (`producer/producer.py`)

A thin wrapper around `kafka-python`'s `KafkaProducer`:

- **Key serializer**: `machine_id.encode("utf-8")` — messages are partitioned by machine so all readings from `WM_0007` always land in the same Kafka partition in strict timestamp order.
- **Value serializer**: `json.dumps(reading).encode("utf-8")` — full reading dict serialized as JSON bytes.
- **Retries**: 3 — handles transient broker connectivity issues gracefully.
- **Fallback**: If Kafka is unavailable, `send_reading()` logs the reading locally and returns `True` (standalone demo mode continues without Kafka).

### Component 4: Kafka Consumer (`consumer/consumer.py`)

Reads from Kafka topic `machine-readings` (Consumer Group: `maintenance-tracker`) and performs three actions per message:

1. **HDFS Append**: Writes raw JSON reading to `data/machines/raw/dt=YYYY-MM-DD/stream_history.json` (date-partitioned, append-only).
2. **MySQL Write**: Upserts live state into `machine_status` via `REPLACE INTO`, then appends to `readings_log` via `INSERT ... ON DUPLICATE KEY UPDATE`.
3. **Dashboard Buffer**: Appends reading to `LIVE_CONSUMER_BUFFER` (in-memory list, max 50 entries) for the live stream dashboard tab.

**At-Least-Once Delivery Guarantee**: `enable_auto_commit=False` + explicit `consumer.commit()` after every successful MySQL write. If the process crashes mid-write, the offset is not committed and the message will be reprocessed on restart — zero data loss.

### Component 5: PySpark Batch Jobs (`spark/`)

Two batch PySpark jobs run independently on demand (or scheduled via cron):

**`spark/insights.py`** — Reads all HDFS raw Parquet/JSON records and computes:
- **Insight 1**: `avg(cycle_temperature)` per branch — which city runs hottest?
- **Insight 2**: Count of readings where `cycle_temperature > 70.0` per machine — which machines spend the most time overheating?
- **Insight 3**: Distribution of ALERT readings by hour of day — at what time do overheats peak nationally?

**`spark/train_model.py`** — Trains an MLlib `LogisticRegression` pipeline:
- **Features**: `cycle_temperature` (numeric) + `branch` (categorical, indexed via `StringIndexer`)
- **Label**: `breakdown_soon` (binary: 0/1), constructed from a lead-window (`next_temp > 70.0`)
- **Split**: 80% train / 20% test, fixed seed 42 for reproducibility
- **Evaluation Metrics**: AUC (Area Under ROC), Weighted Precision, Weighted Recall
- **Why Recall over Precision?** Missing an impending breakdown (False Negative) burns motors and angers customers. A false alarm (False Positive) just sends a technician unnecessarily — far cheaper.
- **Output**: Model artifact saved to HDFS `/data/machines/models/lr_heat_v1` (or local `./lr_heat_v1` fallback)

### Component 6: Web Dashboard (`dashboard/app.py`)

A web server running on **Port 8050** providing the human-facing operational interface:

- Queries **MySQL only** for sub-second page loads
- Queries **HDFS analytical JSON files** for PySpark insights and predictions
- Features: Live heat gauges, status badges, branch filter buttons, red ALERT banner for `breakdown_soon = 1` machines, light/dark mode toggle

---

## 6. Kafka Configuration Deep-Dive

| Parameter | Value | Explanation |
| :--- | :--- | :--- |
| **Topic Name** | `machine-readings` | Single topic carrying all washer telemetry fleet-wide |
| **Partitions** | `3` | Distributes load across 3 parallel consumers. 3 chosen to match the number of available consumer threads in demo mode. |
| **Replication Factor** | `1` | Single broker in local development. In production, would be 3 for fault tolerance. |
| **Partition Key** | `machine_id` | All readings from `WM_0007` always land in the same partition, guaranteeing strict per-machine time ordering. |
| **Consumer Group** | `maintenance-tracker` | Enables horizontal scaling — multiple consumer instances share partition load automatically. |
| **Auto Offset Commit** | `False` (disabled) | Manual commit via `consumer.commit()` after each successful MySQL write. Prevents data loss on crash. |
| **Offset Reset Policy** | `earliest` | On first connection or lost offset, starts reading from the very beginning of the topic — no readings skipped. |
| **Value Serializer** | `json.dumps().encode("utf-8")` | Full reading JSON serialized to UTF-8 bytes on the producer side. |
| **Value Deserializer** | `json.loads(v.decode("utf-8"))` | Deserialized back to Python dict on the consumer side. |
| **Kafka Bootstrap** | `localhost:9092` | Local single-broker Kafka instance. Change to comma-separated list for multi-broker cluster. |
| **Producer Retries** | `3` | Retries transient publish failures up to 3 times before logging an error. |
| **Request Timeout** | `5,000 ms` | Consumer waits up to 5 seconds for broker response before timing out. |

---

## 7. HDFS Storage Layout & Partitioning

HDFS is used for two distinct storage roles in SpinWatch:

### 7.1 Raw Historical Store (`/data/machines/raw/`)

```
/data/machines/raw/
├── dt=2026-09-17/
│   └── stream_history.json       ← All readings received on Sept 17 (JSON Lines format)
├── dt=2026-09-18/
│   └── stream_history.json       ← All readings received on Sept 18
├── dt=2026-09-19/
│   └── stream_history.json       ← Today's live readings (appended in real-time)
└── ...                           ← One directory per calendar day
```

- **Date Partitioning**: Each day's readings are isolated in their own `dt=YYYY-MM-DD` directory. This is the Hive-style partition convention used by Spark, Hive, and Presto.
- **Format**: JSON Lines (one complete reading JSON object per line) in demo/local mode. Parquet in production HDFS (via Kafka Connect HDFS Sink).
- **Why Parquet in Production?** Parquet is a columnar format — reading only `cycle_temperature` from a 10M-row file reads only 1 column worth of bytes from disk, not all 10 columns. 10–100× faster for analytical queries.
- **Time-Travel**: The API endpoint `/api/hdfs/history?dt=YYYY-MM-DD` lets you query any historical date partition directly.
- **Immutability**: Records in HDFS are never modified or deleted — the raw store is an immutable audit log of every reading ever received.

### 7.2 Analytical Store (`/data/machines/insights/` & `/data/machines/predictions/`)

```
/data/machines/insights/
├── avg_temp_by_branch/           ← Parquet: branch, avg_temperature
├── time_in_alert/                ← Parquet: machine_id, branch, alert_count
├── busiest_alert_hour/           ← Parquet: hour, alert_count
└── latest_insights.json          ← Combined JSON snapshot for dashboard

/data/machines/predictions/
└── latest_predictions.json       ← ML prediction per machine_id (breakdown_soon 0/1)

/data/machines/models/
└── lr_heat_v1/                   ← Trained PySpark MLlib LogisticRegression model artifact
    ├── metadata/
    └── stages/
```

---

## 8. PySpark Analytics & MLlib Machine Learning Pipeline

### 8.1 Heat Analytics Insights (`spark/insights.py`)

**Input**: HDFS `/data/machines/raw/` (Parquet) or local JSON fallback
**PySpark Session**: `SparkSession.builder.appName("SpinWatch-Heat-Insights")`
**Three Computed Insights**:

| # | Insight Name | PySpark Operation | Output Columns |
|---|---|---|---|
| 1 | Average temperature per branch | `groupBy("branch").agg(round(avg("cycle_temperature"), 2))` | `branch`, `avg_temperature` |
| 2 | Machines most in ALERT state | `filter(col > 70.0).groupBy("machine_id", "branch").agg(count("*"))` | `machine_id`, `branch`, `alert_count` |
| 3 | Hourly ALERT peak distribution | `filter(col > 70.0).withColumn("hour", hour(col)).groupBy("hour").agg(count("*"))` | `hour`, `alert_count` |

**Output**: Parquet files to HDFS + `latest_insights.json` to local analytical store (for dashboard).

### 8.2 MLlib Logistic Regression Training (`spark/train_model.py`)

**Training Data Source**: HDFS Parquet (`/data/machines/raw/`) → MySQL JDBC fallback
**Feature Engineering**:

```python
# Step 1: Construct breakdown label using lead-window per machine
w = Window.partitionBy("machine_id").orderBy("txn_timestamp")
data = df.withColumn("next_temp", lead("cycle_temperature", 1).over(w))
         .withColumn("breakdown_soon", (col("next_temp") > 70.0).cast("int"))
         .filter(col("next_temp").isNotNull())

# Step 2: Encode categorical branch → numeric index
branch_indexer = StringIndexer(inputCol="branch", outputCol="branch_idx", handleInvalid="keep")

# Step 3: Assemble feature vector
assembler = VectorAssembler(inputCols=["cycle_temperature", "branch_idx"], outputCol="features")

# Step 4: Logistic Regression classifier
lr = LogisticRegression(labelCol="breakdown_soon", featuresCol="features", maxIter=20)
```

**Pipeline Stages**: `[branch_indexer → assembler → lr]`
**Train/Test Split**: 80% / 20%, random seed `42`
**Evaluation Metrics**:

| Metric | Evaluator | Why It Matters for SpinWatch |
|---|---|---|
| **AUC** (Area Under ROC) | `BinaryClassificationEvaluator` | Overall model quality — how well it separates "about to break" from "fine" |
| **Weighted Precision** | `MulticlassClassificationEvaluator` | Minimizes false alarms (unnecessary technician dispatch costs) |
| **Weighted Recall** | `MulticlassClassificationEvaluator` | **Most critical**: ensures we catch every impending breakdown (missed breakdown = motor burnout) |

**Model Output**: Saved to HDFS `/data/machines/models/lr_heat_v1` (local `./lr_heat_v1` fallback).

---

## 9. REST API Endpoint Reference (All 11 Endpoints)

The plain Python REST API runs on **Port 8000**. The Django REST Framework version (Port 8001) mirrors all these endpoints with a browsable HTML UI.

### Base URL: `http://localhost:8000`

| # | Endpoint | Method | Description | Sample Response Key |
| :--- | :--- | :--- | :--- | :--- |
| 1 | `/api/readings/` | `POST` | Submit a new sensor reading to Kafka | `{ "status": "success", "payload": {...} }` |
| 2 | `/api/readings/` | `GET` | Inspect the 20 most recent ingress readings | `{ "recent_ingress_samples": [...] }` |
| 3 | `/api/generator/status` | `GET` | Generator fleet/branch summary | `{ "fleet_size": 3000, "branches_count": 13 }` |
| 4 | `/api/kafka/status` | `GET` | Kafka topic & partition info | `{ "topic": "machine-readings", "partitions": 3 }` |
| 5 | `/api/connect/status` | `GET` | Kafka Connect sink connector status | `{ "connectors": [{...}, {...}] }` |
| 6 | `/api/hdfs/raw` | `GET` | HDFS raw store summary + 5 sample records | `{ "partition_count": N, "sample_landed_records": [...] }` |
| 7 | `/api/hdfs/dates` | `GET` | List all available HDFS date partitions | `{ "dates": ["2026-09-19", "2026-09-18", ...] }` |
| 8 | `/api/hdfs/history?dt=YYYY-MM-DD` | `GET` | Time-travel query for a specific date partition | `{ "queried_date": "...", "records": [...] }` |
| 9 | `/api/sql/readings` | `GET` | 50 newest MySQL readings (txn_timestamp DESC) | `{ "latest_landed_readings": [...] }` |
| 10 | `/api/predictions` | `GET` | PySpark MLlib predictions from HDFS | `{ "predictions": [...] }` |
| 11 | `/api/insights` | `GET` | PySpark heat analytics from HDFS | `{ "avg_temp_by_branch": [...], "top_overheating_machines": [...] }` |
| 12 | `/api/consumer/live` | `GET` | Consumer live buffer (25 most recent) | `[{reading}, {reading}, ...]` |

### Sample `POST /api/readings/` Payload

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

### Sample `GET /api/sql/readings` Response

```json
{
  "stage": "Point 3b: MySQL Operational Storage (laundry_ops)",
  "table_target": "readings_log & machine_status",
  "status_summary": [
    { "status": "ALERT", "count": 47 },
    { "status": "NORMAL", "count": 312 }
  ],
  "latest_landed_readings": [
    {
      "reading_id": "d7f8a1e2-...",
      "machine_id": "WM_0007",
      "branch": "Kigali",
      "cycle_temperature": 74.50,
      "txn_timestamp": "2026-09-19 18:30:00",
      "status": "ALERT",
      "breakdown_soon": 1
    }
  ]
}
```

---

## 10. Django REST Framework API Browser

SpinWatch includes a **Django REST Framework (DRF)** server on **Port 8001** that exposes all pipeline inspection endpoints with a beautiful, interactive **Browsable HTML API** — no Postman required.

### Starting the DRF API Browser

```bash
# From the project root:
python api/manage.py runserver 8001
```

### Access

Open your browser at: **`http://localhost:8001/api/`**

You will see DRF's auto-generated API root listing every available endpoint as a clickable hyperlink. Each endpoint page shows:

- Full JSON response rendered with syntax highlighting
- HTTP method buttons (GET / POST)
- Input form for POST endpoints
- Content-Type negotiation (HTML browsable UI vs. raw JSON)

### DRF Endpoint Map

| URL | Methods | Description |
| :--- | :--- | :--- |
| `http://localhost:8001/api/` | GET | **API Root Browser** — all endpoints listed |
| `http://localhost:8001/api/readings/` | GET, POST | Telemetry ingress stream |
| `http://localhost:8001/api/generator/status/` | GET | Generator fleet status |
| `http://localhost:8001/api/kafka/status/` | GET | Kafka topic info |
| `http://localhost:8001/api/connect/status/` | GET | Kafka Connect sinks |
| `http://localhost:8001/api/hdfs/raw/` | GET | HDFS raw store inspector |
| `http://localhost:8001/api/hdfs/dates/` | GET | HDFS available date partitions |
| `http://localhost:8001/api/hdfs/history/` | GET | Time-travel date query (`?dt=YYYY-MM-DD`) |
| `http://localhost:8001/api/sql/readings/` | GET | MySQL newest readings (DESC) |
| `http://localhost:8001/api/predictions/` | GET | MLlib predictions |
| `http://localhost:8001/api/insights/` | GET | PySpark heat analytics |
| `http://localhost:8001/api/consumer/live/` | GET | Consumer live buffer |

### Technology Stack

- **Django 4.2+** — web framework
- **Django REST Framework 3.14+** — DRF browsable API, serializers, `APIView` classes
- **django-cors-headers 4.0+** — CORS headers for cross-origin requests from the dashboard
- **Configuration**: `api/spinwatch_api/settings.py`
- **App**: `api/endpoints/views.py` (all `APIView` classes)

---

## 11. Setup & Running Instructions

### Prerequisites

- Python 3.8+ with pip
- Apache Kafka running on `localhost:9092`
- MySQL running on `localhost:3306` with root access (no password in dev)
- Apache Hadoop / HDFS running on `localhost:9000` (optional — JSON fallback works without HDFS)

### Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

Dependencies installed:
- `requests>=2.28.0` — Generator HTTP POST delivery
- `kafka-python>=2.0.2` — Kafka Producer & Consumer
- `mysql-connector-python>=8.0.30` — MySQL operational database connectivity
- `pyspark>=3.3.0` — Batch analytics and MLlib model training
- `django>=4.2.0` — DRF API browser web framework
- `djangorestframework>=3.14.0` — Browsable HTML API
- `django-cors-headers>=4.0.0` — CORS for dashboard integration

### Step 2: Database Setup

Execute the MySQL operational schema script to create the `laundry_ops` database and both tables with performance indexes:

```bash
mysql -u root < sql/01_schema.sql
```

This creates:
- Database `laundry_ops`
- Table `machine_status` with `idx_last_updated` and `idx_status` indexes
- Table `readings_log` with `idx_txn_timestamp`, `idx_machine_id`, and `idx_status` indexes

### Step 3: HDFS Raw Directory Setup & Historical Data Seeding

Create the HDFS directory structure and populate initial historical telemetry for PySpark to process:

```bash
# Create HDFS directory tree
hdfs dfs -mkdir -p /data/machines/raw
hdfs dfs -mkdir -p /data/machines/insights
hdfs dfs -mkdir -p /data/machines/predictions

# Seed initial historical data (creates local data/ structure too)
python scripts/seed_hdfs_data.py

# Optionally upload seeded local data to HDFS cluster
python scripts/upload_to_hdfs.py
```

### Step 4: Run PySpark Batch Jobs (Optional — Pre-populates Analytics)

```bash
# Compute heat insights (branch averages, alert frequency, hourly peaks)
python spark/insights.py

# Train the LogisticRegression failure prediction model
python spark/train_model.py
```

### Step 5: Launch Master Pipeline (Single Command)

Start all services simultaneously with one command:

```bash
python run_project.py
```

This sequentially starts:
1. Seeds HDFS & MySQL with initial data
2. Starts REST Ingress API on Port 8000
3. Starts Kafka Consumer (group: `maintenance-tracker`)
4. Starts Telemetry Generator at 3 TPS
5. Starts Web Dashboard on Port 8050
6. Starts Django DRF API Browser on Port 8001

### Step 6: Running Services Individually (Multi-Terminal Mode)

For development, run each service in its own terminal for independent log visibility:

```bash
# Terminal 1 — REST Ingress & Inspection API (Port 8000)
python producer/app.py

# Terminal 2 — Kafka Telemetry Consumer
python consumer/consumer.py

# Terminal 3 — Telemetry Generator (3 readings/second)
python generator/generator.py --tps 3

# Terminal 4 — Web Dashboard (Port 8050)
python dashboard/app.py

# Terminal 5 — Django DRF API Browser (Port 8001)
python api/manage.py runserver 8001
```

### Service URL Summary

| Service | URL | Description |
| :--- | :--- | :--- |
| Web Dashboard | `http://localhost:8050/` | Light/Dark mode operational dashboard |
| REST Ingress API | `http://localhost:8000/api/readings/` | Telemetry POST endpoint |
| DRF API Browser | `http://localhost:8001/api/` | Interactive browsable API |
| Kafka Connect | `http://localhost:8083/connectors/` | Kafka Connect REST API |

---

## 12. Web Dashboard Features

The dashboard at `http://localhost:8050/` provides four main views:

### Tab 1 — MySQL Operational Storage View
Displays the live `machine_status` table from `laundry_ops`. Columns: **Washer ID**, **Branch**, **Temperature (°C)**, **Operational Status** (`NORMAL` / `ALERT` badge), **Last Updated**, **Actions**. Rows are ordered by `last_updated DESC` — machines that most recently reported an ALERT or reading appear at the top.

### Tab 2 — PySpark ML Failure Prediction Overview
Visualizes the `breakdown_soon` predictions computed by the PySpark MLlib `LogisticRegression` model and stored in HDFS `/data/machines/predictions/latest_predictions.json`. Shows which machines are flagged `breakdown_soon = 1` with red warning badges. Summary cards show: total fleet size, machines in ALERT, average fleet temperature, and ML-predicted failures.

### Tab 3 — 📡 Kafka Live Telemetry Stream (Real Coming Data)
A dedicated real-time table that displays incoming telemetry packets as they travel through Kafka topic `machine-readings`. Powered by the Consumer's `LIVE_CONSUMER_BUFFER` (50-entry rolling buffer). Refreshed automatically to show the pulse of the live data stream.

### Tab 4 — 📦 HDFS Raw Data Detailed Inspector
Record-by-record browser for the HDFS historical store. Allows filtering by date partition (`dt=YYYY-MM-DD`) and inspecting full JSON payloads for any historical reading. Uses the `/api/hdfs/history?dt=YYYY-MM-DD` API endpoint under the hood.

---

## 13. Colleague Q&A Cheat Sheet

| Question | Expert Answer |
| :--- | :--- |
| **"Why do we monitor only `cycle_temperature` as the primary threshold?"** | Temperature is the single most reliable predictor of commercial washer motor failure. High vibration and pressure issues eventually manifest as heat. By focusing on `cycle_temperature > 70.0°C` as the primary threshold, we keep the alert logic simple, fast, and actionable without false alarms from secondary metrics. |
| **"Why Kafka? Why not just write directly to MySQL?"** | Kafka decouples the generator from the storage tier. The generator can produce at 100+ TPS without waiting for MySQL write latency. If MySQL goes down for maintenance, readings buffer safely in Kafka and are replayed on reconnect. Kafka also enables multiple consumers (MySQL sink + HDFS sink + live buffer) from a single stream without re-hitting the source. |
| **"Why do we commit Kafka offsets manually?"** | `enable_auto_commit=False` + `consumer.commit()` after every successful MySQL write guarantees at-least-once delivery. If the consumer crashes after the Kafka read but before the MySQL commit, the reading is re-delivered from Kafka on restart — zero data loss, even under hardware failures. |
| **"Why separate MySQL from HDFS? Why not store everything in MySQL?"** | MySQL is optimized for low-latency single-row lookups (< 5ms). It would slow to seconds querying millions of historical rows. HDFS + Parquet is optimized for columnar analytical scans over billions of records. Each storage tier serves its purpose: MySQL for live dashboard speed, HDFS for deep historical analytics. |
| **"Why did we choose Recall over Precision as the most important ML metric?"** | A False Negative (missing a real impending breakdown) means a washer motor burns out, the customer finds the machine broken, a repair team is dispatched urgently, and the motor replacement costs 5–10× more than a preventive check. A False Positive (sending a technician unnecessarily) costs one check trip. Recall must be maximized — we cannot afford to miss failures. |
| **"Can the dashboard query HDFS directly?"** | No. Direct PySpark HDFS queries take 10–60 seconds to compute over large datasets. The dashboard queries pre-computed MySQL and JSON summary files for sub-second page loads. PySpark runs as a separate scheduled batch job, not inline with dashboard requests. |
| **"Why is `machine_id` used as the Kafka partition key?"** | Kafka guarantees ordering only within a single partition. By keying on `machine_id`, all readings for `WM_0007` always land in the same partition in strict timestamp order. This means the Consumer processes each machine's readings in perfect sequence — critical for the lead-window `breakdown_soon` label computation in PySpark. |
| **"What does `breakdown_soon = 1` actually mean?"** | It means the MLlib model predicts that this machine's **next** telemetry reading will have `cycle_temperature > 70.0°C` (the heat fault threshold). This gives the operations team a one-cycle early warning to dispatch a technician before the fault actually occurs. |
| **"Why do we use `REPLACE INTO` for `machine_status` and `ON DUPLICATE KEY UPDATE` for `readings_log`?"** | `machine_status` maintains exactly one row per machine (its current live state) — `REPLACE INTO` atomically deletes the old row and inserts the updated one. `readings_log` is an append log where each `reading_id` UUID must be unique — `ON DUPLICATE KEY UPDATE` handles Kafka at-least-once redeliveries idempotently without creating duplicate rows. |
| **"What is the DRF API browser and why was it added?"** | The Django REST Framework browsable API (Port 8001) provides a web-based interactive UI for all 11 SpinWatch API endpoints. Instead of memorizing curl commands or configuring Postman, developers can click through endpoints in a browser, see live JSON responses with syntax highlighting, and submit POST requests via HTML forms. It's the developer-experience layer on top of the existing plain-Python API. |
