# SpinWatch — Project Report & Complete Technical Documentation

> **Big Data Essentials — Group Final Exam Project Report**  
> **Title**: SpinWatch: Real-Time Machine Heat Monitoring & Predictive Failure Pipeline  
> **Course**: Big Data Essentials & Distributed Systems Architecture  

---

## 1. Case Study & Business Problem Framing

### 1.1 Commercial Problem Context
**SpinWatch** is designed for a nationwide laundry-machine operator managing coin- and app-operated commercial washing machines installed across multiple branches (**Kigali, Musanze, Huye, Rubavu, Rusizi, Nyagatare**). 

With dozens of machines distributed geographically across different cities, manual daily physical inspection by staff is impossible. When a washer's heating element or drive motor malfunctions while running a cycle, the water/motor temperature (`cycle_temperature`) spikes rapidly. If undetected, the machine overheats mid-cycle, damaging customer laundry, tripping facility breakers, causing costly repair bills, and losing customer trust.

### 1.2 The Technical Solution
SpinWatch solves this by establishing a real-time big data telemetry pipeline:
1. **Continuous Telemetry Stream**: Each washing machine streams cycle temperature (`cycle_temperature`) and timestamp (`timestamp`) continuously.
2. **Automated Status Evaluation**: Evaluates thermal threshold rules in real-time (`ALERT` if `cycle_temperature > 70.0°C`, otherwise `NORMAL`).
3. **Machine Learning Predictive Failure**: Trains a PySpark MLlib `LogisticRegression` classifier to predict impending machine breakdown (`breakdown_soon = 1`).
4. **Separation of Operational and Historical Storage**:
   - **Operational Storage (MySQL `laundry_ops`)**: Low-latency storage driving the web dashboard for real-time fleet state and ML predictions.
   - **Historical Storage (Apache HDFS `/data/machines/raw`)**: Scalable, block-replicated Parquet storage (`dt=YYYY-MM-DD`) holding raw historical stream data for PySpark batch processing.

---

## 2. Refined Schema Definition (The Source Table)

Every record produced by the generator, streamed through Kafka, stored in HDFS, and displayed in MySQL follows this refined schema:

| Field Name | Datatype | Example | Business & System Description |
| :--- | :--- | :--- | :--- |
| `reading_id` | `CHAR(36)` | `d7f8a1e2-...` | Unique UUID for every telemetry reading |
| `machine_id` | `VARCHAR(20)` | `WM_0007` | Washer identifier string |
| `branch` | `VARCHAR(50)` | `Kigali` | City branch location of the machine |
| `cycle_temperature` | `DECIMAL(5,2)` | `74.50` | Water/motor temperature in °C during wash cycle |
| `timestamp` | `DATETIME` | `2026-09-16 17:40:00` | Timestamp when telemetry was recorded |
| `status` | `VARCHAR(10)` | `ALERT` | Operational status: `NORMAL` or `ALERT` (`>70.0°C`) |
| `breakdown_soon` | `TINYINT` | `1` | Machine failure label (1 = Next cycle trips heat fault) |

---

## 3. End-to-End System Architecture

```
 Generator (Python) ──POST──► Ingress REST API (Port 8000)
                                      │
                                      ▼
                        Kafka Topic "machine-readings" (3 Partitions, RF=1)
                                      │
           ┌──────────────────────────┴──────────────────────────┐
           ▼                                                     ▼
 MySQL Sink / Python Consumer                         HDFS Sink Connector
 (Group: maintenance-tracker)                     (/data/machines/raw/ dt=YYYY-MM-DD)
           │                                                     │
           ▼                                                     ▼
 Local MySQL: laundry_ops ◄────────────────────────────── PySpark Engine
 - machine_status                                   (3 Insights & MLlib Model)
 - readings_log                                                  │
 - predictions                                                   ▼
 - insight_*                                         HDFS Model Store
           ▲                                  (/data/machines/models/lr_heat_v1)
           │ (Queries MySQL Only)
 Django/Web Dashboard
 (Port 8050, Glassmorphic Dark UI)
```

---

## 4. Implementation Guidelines & Rubric Crosswalk

This implementation maps 1-to-1 against all course rubric requirements:

| Rubric Requirement | Implementation Component | Technical Verification |
| :--- | :--- | :--- |
| **1. Continuous Data Generation** | [`generator/generator.py`](file:///c:/Users/user/Desktop/Machine%20sensors/generator/generator.py) | Configurable rate via `--tps` argument. Simulates 30 washers across 6 branches with thermal drift. |
| **2. Kafka Ecosystem & Ordering** | [`producer/producer.py`](file:///c:/Users/user/Desktop/Machine%20sensors/producer/producer.py) | Messages keyed by `machine_id` to guarantee partition ordering. 3 partitions on `machine-readings` topic. |
| **3. At-Least-Once Delivery** | [`consumer/consumer.py`](file:///c:/Users/user/Desktop/Machine%20sensors/consumer/consumer.py) | `enable_auto_commit=False`. Manual `consumer.commit()` issued strictly AFTER MySQL write succeeds. |
| **4. Operational vs Historical Storage Split** | [`sql/01_schema.sql`](file:///c:/Users/user/Desktop/Machine%20sensors/sql/01_schema.sql) & `hdfs-sink.json` | MySQL stores live current state (`machine_status`); HDFS stores full raw Parquet history (`/data/machines/raw`). |
| **5. PySpark Analytical Insights** | [`spark/insights.py`](file:///c:/Users/user/Desktop/Machine%20sensors/spark/insights.py) | Computes 3 heat insights: Branch average temp, Top alert machines, Hourly alert distribution. |
| **6. PySpark MLlib Predictive Model** | [`spark/train_model.py`](file:///c:/Users/user/Desktop/Machine%20sensors/spark/train_model.py) | `LogisticRegression` model predicting `breakdown_soon`. Evaluates AUC, Precision, and Recall. |
| **7. ML Batch Deployment** | [`spark/score_batch.py`](file:///c:/Users/user/Desktop/Machine%20sensors/spark/score_batch.py) | Loads saved model from HDFS and writes predictions to MySQL `predictions` table. |
| **8. Interactive Web Dashboard** | [`dashboard/app.py`](file:///c:/Users/user/Desktop/Machine%20sensors/dashboard/app.py) & `index.html` | Real-time status table, branch filters, temperature progress gauges, and red alert banner. Queries MySQL only. |
| **9. MapReduce Optional Bonus** | [`mapreduce/mapper.py`](file:///c:/Users/user/Desktop/Machine%20sensors/mapreduce/mapper.py) & `reducer.py` | Python Hadoop Streaming mapper and reducer filtering high-heat readings (`> 70.0°C`) per branch. |

---

## 5. Defense Q&A Preparation & Metric Justifications

During presentation and defense, be prepared to answer:

1. **Why split MySQL and HDFS?**  
   *Answer*: MySQL is optimized for fast, indexed operational queries by the dashboard (e.g., fetching current status of 30 washers in < 5ms). HDFS is scalable, distributed storage optimized for large-scale PySpark batch reads over months of raw historical data without impacting operational dashboard speed.

2. **Why does Recall matter more than Precision for failure prediction?**  
   *Answer*: In commercial washer monitoring, a **false negative** (missing a machine that is about to break down) results in mid-cycle failure, customer compensation, and catastrophic motor burnouts. A **false positive** (low precision) merely results in a routine maintenance check. Thus, maximizing Recall is critical.

3. **How is Kafka partition ordering preserved?**  
   *Answer*: Kafka producers key messages by `machine_id`. Kafka routes all messages with the same key to the exact same partition, guaranteeing chronological sequence for each washer's temperature readings.
