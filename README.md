# SpinWatch — Nationwide Washing Machine Heat & Failure-Risk Monitor

> **Big Data Essentials Group Final Exam Project**  
> **Core Focus**: Machine Heat/Temperature Monitoring (`cycle_temperature`), Performing Time (`timestamp`), Failure Prediction (`breakdown_soon`), and Branch Analytics.

---

## 1. Executive Summary & Case Framing

**SpinWatch** is built for a laundry-machine operator running coin- and app-operated washing machines inside branches across multiple Rwandan cities (**Kigali, Musanze, Huye, Rubavu, Rusizi, Nyagatare**).

Each machine continuously streams heat telemetry (`cycle_temperature`) while in operation. The core operational problem is simple: machines that suffer motor or heating element failure often overheat mid-cycle, disrupting customer operations and incurring repair expenses.

This system:
1. **Streams Heat Telemetry**: Captures continuous water and motor temperatures per machine in real-time.
2. **Evaluates Heat Faults**: Automatically triggers `ALERT` status whenever `cycle_temperature > 70.0°C`.
3. **Predicts Impending Breakdown**: Uses a PySpark MLlib `LogisticRegression` classification model to predict machine failure risk (`breakdown_soon = 1`).
4. **Separates Operational vs Analytical Storage**:
   - **Local MySQL (`laundry_ops`)**: Operational storage for live dashboard reads and current washer state.
   - **Local HDFS (`/data/machines/raw`)**: Historical analytical storage partitioned by date (`dt=YYYY-MM-DD`) in Parquet format.

---

## 2. Refined Schema — The Source Table

Every telemetry reading flowing through Kafka, HDFS, and MySQL follows this exact schema:

| Column | Type | Description / Threshold Rule |
| :--- | :--- | :--- |
| `reading_id` | `CHAR(36)` (UUID) | Unique identifier for each telemetry reading |
| `machine_id` | `VARCHAR(20)` | Washer unit ID (e.g., `WM_0007`) |
| `branch` | `VARCHAR(50)` | City / branch location (e.g., `Kigali`, `Musanze`, `Huye`) |
| `cycle_temperature` | `DECIMAL(5,2)` | Water / motor temperature during cycle (°C) |
| `timestamp` | `DATETIME` | Timestamp when reading was recorded |
| `status` | `VARCHAR(10)` | `NORMAL` or `ALERT` (`ALERT` if `cycle_temperature > 70.0°C`) |
| `breakdown_soon` | `TINYINT` (0/1) | Label — `1` if machine trips heat fault threshold |

---

## 3. End-to-End System Architecture

```
                                [ Python Telemetry Generator ]
                                              │
                                              ▼ (HTTP POST JSON)
                                [ Ingress REST API (Port 8000) ]
                                              │
                                              ▼
                             [ Kafka Topic: "machine-readings" ]
                              (3 Partitions, Keyed by machine_id)
                                              │
                    ┌─────────────────────────┴─────────────────────────┐
                    ▼                                                   ▼
       [ Kafka Python Consumer ]                              [ Kafka HDFS Sink ]
       (Group: "maintenance-tracker")                      (/data/machines/raw Parquet)
                    │                                                   │
                    ▼                                                   ▼
          [ Local MySQL Database ] ◄────────────────────── [ PySpark Batch Engine ]
           (laundry_ops tables)                             (3 Heat Insights + MLlib Model)
                    ▲
                    │ (Queries MySQL Only)
          [ Django / Web Dashboard ]
            (Port 8050, Glassmorphic UI)
```

---

## 4. Setup & Running Instructions

### Prerequisites
- Python 3.8+
- MySQL Server (listening on `127.0.0.1:3306`, user `root`, no password)
- Apache Kafka & Zookeeper / KRaft (listening on `localhost:9092`)
- Apache Hadoop HDFS & PySpark (optional for live cluster mode; fallback mode built-in)

### Step 1: Database Setup
Execute the MySQL initialization script to create `laundry_ops` database and schema:
```bash
mysql -u root < sql/01_schema.sql
```

### Step 2: HDFS Raw Directory Creation
Create the raw telemetry target directory in HDFS:
```cmd
C:\hadoop> hdfs dfs -mkdir -p /data/machines/raw
```

### Step 3: Apache Kafka Topic Creation & Verification
Create the multi-partition Kafka topic for telemetry streaming:
```cmd
C:\kafka> bin\windows\kafka-topics.bat --create --topic machine-readings --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

C:\kafka> bin\windows\kafka-topics.bat --describe --topic machine-readings --bootstrap-server localhost:9092
```

### Step 4: Launch the System Components

Open 4 separate terminal windows to launch the full pipeline:

1. **Terminal 1 - Launch Ingress REST API**:
   ```bash
   python producer/app.py
   ```

2. **Terminal 2 - Launch Kafka Telemetry Consumer**:
   ```bash
   python consumer/consumer.py
   ```

3. **Terminal 3 - Start Telemetry Stream Generator**:
   ```bash
   python generator/generator.py --tps 3
   ```

4. **Terminal 4 - Launch Web Dashboard**:
   ```bash
   python dashboard/app.py
   ```
   *Access the web dashboard in your browser at:* **`http://localhost:8050/`**

---

## 5. PySpark Analytics & Machine Learning

### PySpark Batch Insights (`spark/insights.py`)
Reads historical Parquet telemetry from HDFS and computes 3 heat-focused insights:
1. **Average Heat per Branch**: Ranks branches by fleet thermal operational stress (`insight_avg_temp_by_branch`).
2. **Time Spent in ALERT**: Identifies machines spending the most performing time above 70°C (`insight_time_in_alert`).
3. **Hourly Alert Peaks**: Identifies peak operational hours for heat spikes (`insight_busiest_alert_hour`).

Run batch insights:
```bash
python spark/insights.py
```

### PySpark MLlib Predictive Model (`spark/train_model.py`)
Trains a `LogisticRegression` pipeline using indexed branch and `cycle_temperature` features to predict machine failure (`breakdown_soon`).

**Defense Metrics Explanation**:
- **AUC (Area under ROC)**: Evaluates how effectively the model separates normal heat cycles from impending failure cycles across all confidence thresholds.
- **Precision**: High precision prevents sending technicians on unnecessary, costly maintenance trips to remote branches.
- **Recall (Most Critical)**: High recall ensures the system **never misses a true overheating breakdown**, avoiding mid-cycle machine failure.

Run model training & batch scoring:
```bash
python spark/train_model.py
python spark/score_batch.py
```

---

## 6. Optional MapReduce Bonus (`mapreduce/`)

For Hadoop Streaming environments, `mapper.py` filters high-heat readings (`> 70.0°C`) per branch and `reducer.py` computes total alert frequency.

```bash
cat sample_data.json | python mapreduce/mapper.py | sort | python mapreduce/reducer.py
```

---

## 7. Presentation & Live Demo Script (5-6 Minutes)

| Time | Action | Presentation Narrative |
| :--- | :--- | :--- |
| **0:00** | Open Dashboard (`http://localhost:8050`) | *"SpinWatch monitors washing machine fleet heat and failure risk across Rwanda in real time."* |
| **0:30** | Start `generator.py --tps 3` | *"Telemetry stream is live. Each washer sends water and motor heat readings every few seconds."* |
| **1:00** | Check Kafka Consumer Group | *"Kafka messages are partitioned by `machine_id` to guarantee strict ordering per machine."* |
| **1:30** | Demonstrate Branch Filter & Alert Banner | *"The red banner triggers instantly when a machine exceeds 70°C or is flagged by ML predictions."* |
| **2:30** | Show HDFS & MySQL Data Separation | *"MySQL holds live operational state for fast dashboard queries; HDFS stores raw Parquet history."* |
| **3:30** | Run PySpark Insights & ML Scoring | *"PySpark analyzes historical trends to calculate branch averages and predict breakdown risks."* |
| **5:00** | Q&A Defense Wrap-up | *"The operational-vs-analytical split keeps our real-time dashboard responsive under heavy load."* |
