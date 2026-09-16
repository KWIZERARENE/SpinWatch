# SpinWatch — Postman & REST API Stage Verification Guide

> **Pipeline Inspection & Postman Testing Guide**  
> Instructions for verifying data flow at every stage point in the **SpinWatch Big Data Pipeline** using Postman, `curl`, or browser HTTP requests.

---

## 1. Quick Import into Postman

1. Open **Postman**.
2. Click **Import** (top left).
3. Select the collection file located in your workspace:  
   👉 [`SpinWatch_Postman_Collection.json`](file:///c:/Users/user/Desktop/Machine%20sensors/SpinWatch_Postman_Collection.json)
4. You will see 6 pre-configured requests under **SpinWatch Big Data Pipeline Verification Collection**.

---

## 2. Stage-by-Stage Verification Endpoints

### Point 1: Generator Stream Ingress (`POST` & `GET`)
- **Action**: Test submitting raw heat telemetry from generator to REST Ingress.
- **Method**: `POST`
- **URL**: `http://localhost:8000/api/readings/`
- **Postman Body (JSON)**:
  ```json
  {
    "reading_id": "d7f8a1e2-0000-4000-8000-000000000001",
    "machine_id": "WM_0007",
    "branch": "Kigali",
    "cycle_temperature": 74.50,
    "timestamp": "2026-09-16T18:00:00Z",
    "status": "ALERT",
    "breakdown_soon": 1
  }
  ```
- **Expected Response (`201 Created`)**:
  ```json
  {
    "stage": "Point 1: Generator Ingress -> Kafka Producer",
    "status": "success",
    "message": "Telemetry received and forwarded to Kafka topic 'machine-readings'",
    "payload": { ... }
  }
  ```

---

### Point 2: Kafka Ecosystem & Partitions Inspection (`GET`)
- **Action**: Inspect topic structure, partition count, consumer group ID, and key partitioning strategy.
- **Method**: `GET`
- **URL**: `http://localhost:8000/api/kafka/status`
- **Expected Response (`200 OK`)**:
  ```json
  {
    "stage": "Point 2: Apache Kafka Topic & Partitions",
    "topic": "machine-readings",
    "partitions": 3,
    "replication_factor": 1,
    "consumer_group": "maintenance-tracker",
    "partitioning_key": "machine_id",
    "kafka_status": "AVAILABLE"
  }
  ```

---

### Point 3a: HDFS Landed Raw Telemetry Records (`GET`)
- **Action**: Inspect records landed in HDFS historical storage directory (`/data/machines/raw/`).
- **Method**: `GET`
- **URL**: `http://localhost:8000/api/hdfs/raw`
- **Expected Response (`200 OK`)**:
  ```json
  {
    "stage": "Point 3a: HDFS Historical Storage (/data/machines/raw/)",
    "hdfs_directory": "/data/machines/raw/",
    "storage_format": "Parquet / JSON",
    "sample_landed_records": [
      {
        "reading_id": "d7f8a1e2-...",
        "machine_id": "WM_0004",
        "branch": "Kigali",
        "cycle_temperature": 74.71,
        "status": "ALERT"
      }
    ]
  }
  ```

---

### Point 3b: MySQL Operational Storage Inspection (`GET`)
- **Action**: Inspect live landed records in MySQL `laundry_ops` (`readings_log` & `machine_status`).
- **Method**: `GET`
- **URL**: `http://localhost:8000/api/sql/readings`
- **Expected Response (`200 OK`)**:
  ```json
  {
    "stage": "Point 3b: MySQL Operational Storage (laundry_ops)",
    "table_target": "readings_log & machine_status",
    "latest_landed_readings": [ ... ]
  }
  ```

---

### Point 4a: PySpark MLlib Predictive Failure Model Scoring (`GET`)
- **Action**: Fetch ML model predictions (`prediction = 1` for breakdown risk, `0` for normal).
- **Method**: `GET`
- **URL**: `http://localhost:8000/api/predictions`
- **Expected Response (`200 OK`)**:
  ```json
  {
    "stage": "Point 4a: PySpark MLlib Predictive Model Scoring",
    "model": "LogisticRegression (lr_heat_v1)",
    "prediction_key": "1 = Breakdown Soon (Impending Fault), 0 = Normal Operation",
    "total_scored_machines": 30,
    "predictions": [
      {
        "machine_id": "WM_0004",
        "branch": "Kigali",
        "prediction": 1,
        "scored_at": "2026-09-16 18:00:00"
      }
    ]
  }
  ```

---

### Point 4b: PySpark Analytical Heat Insights (`GET`)
- **Action**: Fetch PySpark calculated branch averages and top overheating machine alert counts.
- **Method**: `GET`
- **URL**: `http://localhost:8000/api/insights`
- **Expected Response (`200 OK`)**:
  ```json
  {
    "stage": "Point 4b: PySpark Analytical Insights",
    "avg_temp_by_branch": [
      { "branch": "Kigali", "avg_temperature": 66.5 },
      { "branch": "Rubavu", "avg_temperature": 61.1 }
    ],
    "top_overheating_machines": [
      { "machine_id": "WM_0004", "branch": "Kigali", "alert_count": 18 }
    ]
  }
  ```
