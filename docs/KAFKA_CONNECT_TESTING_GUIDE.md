# SpinWatch — Kafka Connect Testing & Verification Guide

> **Kafka Connect Integration & Inspection Guide**  
> Simple, step-by-step instructions for testing and verifying Kafka Connect sink connectors (`mysql-sink-laundry` and `hdfs-sink-laundry`).

---

## 1. Overview of Kafka Connect Sinks

In the SpinWatch Big Data pipeline, **Kafka Connect** automatically fans out telemetry messages from the Kafka topic `machine-readings` into two downstream storage targets without writing custom application code:

1. **MySQL Sink (`connect/mysql-sink.json`)**: Sinks live telemetry into MySQL `laundry_ops.readings_log` table for operational queries.
2. **HDFS Sink (`connect/hdfs-sink.json`)**: Sinks raw telemetry into HDFS distributed storage `/data/machines/raw/` in date-partitioned Parquet format (`dt=YYYY-MM-DD`).

---

## 2. How to Test Kafka Connect (3 Simple Steps)

### Step 1: Start Kafka Connect Service
Open a terminal and start Kafka Connect in standalone or distributed mode:

**Windows Command**:
```cmd
C:\kafka> bin\windows\connect-standalone.bat config\connect-standalone.properties connect\mysql-sink.json connect\hdfs-sink.json
```

---

### Step 2: Test & Verify Connectors via REST API

Kafka Connect exposes a built-in REST API on port `8083`. You can test connector status using Postman, `curl`, or browser:

#### 1. List Active Connectors
```http
GET http://localhost:8083/connectors
```
*Expected Response*:
```json
[
  "mysql-sink-laundry",
  "hdfs-sink-laundry"
]
```

#### 2. Check MySQL Sink Connector Status
```http
GET http://localhost:8083/connectors/mysql-sink-laundry/status
```
*Expected Response*:
```json
{
  "name": "mysql-sink-laundry",
  "connector": { "state": "RUNNING", "worker_id": "127.0.0.1:8083" },
  "tasks": [ { "id": 0, "state": "RUNNING", "worker_id": "127.0.0.1:8083" } ]
}
```

#### 3. Check HDFS Sink Connector Status
```http
GET http://localhost:8083/connectors/hdfs-sink-laundry/status
```
*Expected Response*:
```json
{
  "name": "hdfs-sink-laundry",
  "connector": { "state": "RUNNING", "worker_id": "127.0.0.1:8083" },
  "tasks": [ { "id": 0, "state": "RUNNING", "worker_id": "127.0.0.1:8083" } ]
}
```

---

### Step 3: Test Unified SpinWatch Inspection Endpoint

You can also test Kafka Connect status directly through the SpinWatch REST API:

```http
GET http://localhost:8000/api/connect/status
```

*Expected Response*:
```json
{
  "stage": "Kafka Connect Automatic Sink Integration",
  "status": "ONLINE",
  "connectors": [
    {
      "name": "mysql-sink-laundry",
      "target": "MySQL laundry_ops.readings_log",
      "status": "RUNNING"
    },
    {
      "name": "hdfs-sink-laundry",
      "target": "HDFS /data/machines/raw/ (Parquet)",
      "status": "RUNNING"
    }
  ]
}
```

---

## 3. Verifying Automatic Data Sink Execution

1. **Verify MySQL Sink**:
   ```sql
   USE laundry_ops;
   SELECT COUNT(*), MAX(txn_timestamp) FROM readings_log;
   ```
2. **Verify HDFS Parquet Sink**:
   ```cmd
   hdfs dfs -ls /data/machines/raw/
   ```
