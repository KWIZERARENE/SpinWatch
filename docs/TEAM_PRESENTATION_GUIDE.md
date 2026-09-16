# SpinWatch — Colleague & Team Presentation Guide

> **Colleague Presentation & Implementation Walkthrough Guide**  
> Use this document to explain the **SpinWatch Big Data Pipeline & Heat Monitor** project step-by-step to your team, colleagues, and supervisor.

---

## 1. Executive Presentation Elevator Pitch (1 Minute)

> *"SpinWatch is an IoT Big Data monitoring system for a nationwide laundry business operating washing machines across Rwanda (Kigali, Musanze, Huye, Rubavu, Rusizi, Nyagatare).*
> 
> *Instead of sending technicians to inspect every machine daily, our system continuously monitors machine temperature (`cycle_temperature`). If a washer starts overheating (> 70°C), it triggers real-time alerts. Furthermore, our PySpark Machine Learning model predicts machine failures (`breakdown_soon`) before they happen so technicians can intervene proactively."*

---

## 2. System Architecture & Component Explanation

### Visual Architecture Flow
```
 Generator (Python) ──POST──► REST API (Port 8000) ──► Kafka Topic "machine-readings"
                                                                │
                               ┌────────────────────────────────┴────────────────────────────────┐
                               ▼                                                                 ▼
                    Consumer / MySQL Sink                                               HDFS Sink Connector
                 (Operational Storage: MySQL)                                      (Historical Storage: HDFS)
                               │                                                                 │
                               ▼                                                                 ▼
                   MySQL: laundry_ops DB ◄────────────────────────────────────────────── PySpark Batch Jobs
                   - machine_status (Live)                                             - 3 Heat Analytics Insights
                   - readings_log (Raw)                                                - MLlib Logistic Regression
                   - predictions (ML output)                                             (Saves model to HDFS)
                               ▲
                               │ (Queries MySQL Only)
                   Web Dashboard UI (Port 8050)
                   (Dark & Light Mode Switcher)
```

---

## 3. How Data Flows Step-by-Step

### Phase 1: Data Generation & Ingress
- **Generator (`generator/generator.py`)**: Simulates 30 nationwide washing machines. Sends JSON readings every few seconds with realistic thermal drift.
- **REST Ingress (`producer/app.py` & `producer/producer.py`)**: Receives JSON telemetry and publishes messages to Apache Kafka topic `machine-readings`.
- **Partitioning Strategy**: Messages are keyed by `machine_id` (`key=mid`) to guarantee that all readings from a given washer land in the exact same Kafka partition in strict time order.

### Phase 2: Dual Landing — Operational vs Historical Storage
- **Operational Storage (MySQL `laundry_ops`)**:
  - `consumer/consumer.py` reads Kafka messages using `at-least-once` manual offset commit strategy (`enable_auto_commit=False`).
  - Writes live status to `machine_status` and appends raw telemetry to `readings_log`.
- **Historical Storage (Apache HDFS `/data/machines/raw/`)**:
  - Kafka Connect / Stream writer lands full raw readings in date-partitioned Parquet files (`/data/machines/raw/dt=YYYY-MM-DD/part-0000.parquet`).
  - **Why split?**: MySQL stays super fast for live dashboard queries (< 5ms), while HDFS safely stores millions of raw records for PySpark model training without slowing down the dashboard.

### Phase 3: PySpark Insights & Predictive ML
- **Insights (`spark/insights.py`)**: Reads HDFS Parquet files to calculate average temperature per branch, machines spending the most time in ALERT status, and hourly alert peaks.
- **MLlib Predictive Model (`spark/train_model.py`)**:
  - Trains a `LogisticRegression` pipeline model to predict `breakdown_soon` (1 = next cycle trips heat fault > 70°C).
  - Evaluates model using **AUC**, **Precision**, and **Recall**.
- **Batch Scoring (`spark/score_batch.py`)**: Scores current fleet state and writes predictions into MySQL `predictions` table.

### Phase 4: Interactive Web Dashboard
- **Dashboard (`dashboard/app.py` & `index.html`)**:
  - Runs on `http://localhost:8050/`.
  - Queries local MySQL `laundry_ops` database only.
  - **Features**: Live heat gauges, status badges, red warning alert banner, branch filters, and a **Light Mode / Dark Mode Theme Toggle**.

---

## 4. Live Demonstration Walkthrough Script (5-6 Minutes)

When presenting live to colleagues, follow this exact sequence:

1. **Show Web Dashboard**:
   - Open `http://localhost:8050/`.
   - Toggle **Light Mode / Dark Mode** using the top navigation button to demonstrate UI polish.
   - Point out the summary cards (Fleet Washers, Heat Alerts > 70°C, Average Fleet Temp, ML Predictions).

2. **Show Continuous Generator & Ingress**:
   - Run `python generator/generator.py --tps 3`.
   - Explain how telemetry flows into Kafka `machine-readings` topic keyed by `machine_id`.

3. **Demonstrate Branch Filtering & Red Banner**:
   - Click branch filter buttons (**Kigali**, **Musanze**, **Huye**).
   - Show how the table updates instantly. Point out the red warning banner highlighting machines flagged for impending failure (`breakdown_soon = 1`).

4. **Show HDFS Parquet Data Landing**:
   - Navigate to `data/machines/raw/dt=2026-09-16/`.
   - Explain: *"This is where historical telemetry is stored in Parquet format, separated from MySQL operational storage."*

5. **Run PySpark ML Scoring**:
   - Run `python spark/score_batch.py`.
   - Show how MySQL `predictions` table updates and reflects on the web dashboard.

---

## 5. Colleague Q&A Cheat Sheet

| Question from Teammate | Expert Answer |
| :--- | :--- |
| **"Why do we monitor only Heat (`cycle_temperature`)?"** | Focuses our system on the single most critical failure mode in commercial washers (motor/element overheating), eliminating unnecessary telemetry noise while delivering high prediction accuracy. |
| **"Why do we commit Kafka offsets manually?"** | We disable auto-commit (`enable_auto_commit=False`) and issue `consumer.commit()` strictly after MySQL write succeeds. If the server crashes during write, no reading is lost. |
| **"Why did we choose Recall over Precision?"** | Missing an impending machine breakdown (a False Negative) causes customer dissatisfaction and expensive motor burnouts. Routine check trips (low precision) are far cheaper. |
| **"Can the dashboard query HDFS directly?"** | No. Direct HDFS queries take seconds to compute over large datasets. The dashboard queries MySQL operational tables for sub-second page loads. |
