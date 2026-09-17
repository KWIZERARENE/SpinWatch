# SpinWatch — Postman & REST API Stage Verification Guide

> **Pipeline Inspection & Postman Testing Guide**  
> Instructions for verifying data flow at every stage point in the **SpinWatch Big Data Pipeline** using Postman, `curl`, or browser HTTP requests.

---

## 1. Quick Import into Postman

1. Open **Postman**.
2. Click **Import** (top left).
3. Select the collection file located in your workspace:  
   👉 [`SpinWatch_Postman_Collection.json`](file:///c:/Users/user/Desktop/Machine%20sensors/SpinWatch_Postman_Collection.json)
4. You will see 9 pre-configured requests under **SpinWatch Big Data Pipeline Verification Collection**.

---

## 2. Stage-by-Stage Verification Endpoints

### Point 1: Generator Stream Ingress (`POST` & `GET`)
- **Action**: Test submitting raw heat telemetry from generator to REST Ingress, or view recent ingress stream samples.
- **Method**: `POST` / `GET`
- **URL**: `http://localhost:8000/api/readings/`
- **Postman Body (`POST` JSON)**:
  ```json
  {
    "reading_id": "d7f8a1e2-0000-4000-8000-000000000001",
    "machine_id": "WM_0007",
    "branch": "Kigali",
    "cycle_temperature": 74.50,
    "timestamp": "2026-09-17T10:00:00Z",
    "status": "ALERT",
    "breakdown_soon": 1
  }
  ```

---

### Point 2: Kafka Ecosystem & Partitions Inspection (`GET`)
- **Action**: Inspect topic structure, 3 partitions, consumer group ID, and key partitioning strategy.
- **Method**: `GET`
- **URL**: `http://localhost:8000/api/kafka/status`

---

### Point 3a: Kafka Connect Storage Sinks Inspection (`GET`)
- **Action**: Inspect status of Kafka Connect sinks (`mysql-sink-laundry` and `hdfs-sink-laundry`).
- **Method**: `GET`
- **URL**: `http://localhost:8000/api/connect/status`
- **Expected Response (`200 OK`)**:
  ```json
  {
    "stage": "Point 3: Kafka Connect Automatic Storage Sinks",
    "status": "CONFIGURED",
    "connectors": [
      { "name": "mysql-sink-laundry", "target_table": "laundry_ops.readings_log", "status": "RUNNING" },
      { "name": "hdfs-sink-laundry", "target_hdfs_dir": "/data/machines/raw/ (Parquet)", "status": "RUNNING" }
    ]
  }
  ```

---

### Point 3b: HDFS Landed Raw Telemetry Records (`GET`)
- **Action**: Inspect records landed in HDFS historical storage directory (`/data/machines/raw/`).
- **Method**: `GET`
- **URL**: `http://localhost:8000/api/hdfs/raw`

---

### Point 3c: HDFS Historical Time-Travel Query (`GET`)
- **Action**: Query telemetry stored in a specific HDFS historical date partition (`dt=YYYY-MM-DD`).
- **Method**: `GET`
- **URL**: `http://localhost:8000/api/hdfs/history?dt=2026-09-17`

---

### Point 3d: MySQL Operational Storage Inspection (`GET`)
- **Action**: Inspect live landed records in MySQL `laundry_ops` (`readings_log` & `machine_status`).
- **Method**: `GET`
- **URL**: `http://localhost:8000/api/sql/readings`

---

### Point 4a: PySpark MLlib Predictive Model Scoring (`GET`)
- **Action**: Fetch ML model predictions stored in HDFS (`prediction = 1` for breakdown risk, `0` for normal).
- **Method**: `GET`
- **URL**: `http://localhost:8000/api/predictions`

---

### Point 4b: PySpark Analytical Heat Insights (`GET`)
- **Action**: Fetch PySpark calculated branch temperature averages and top overheating machine alert counts stored in HDFS.
- **Method**: `GET`
- **URL**: `http://localhost:8000/api/insights`
