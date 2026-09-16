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
4. **Strict Separation of Operational vs Analytical Storage**:
   - **Local MySQL (`laundry_ops`)**: Operational storage ONLY (`machine_status`, `readings_log`).
   - **Local HDFS (`/data/machines/raw`)**: Historical analytical storage holding raw stream history.
   - **PySpark HDFS Storage (`/data/machines/insights/` & `/data/machines/predictions/`)**: PySpark analytics insights and MLlib predictions are saved **DIRECTLY in HDFS Analytical Storage** and **NOT stored in MySQL**.

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
           - machine_status                                             │
           - readings_log                                               ▼
                    ▲                                      [ HDFS Analytical Store ]
                    │                                      (/data/machines/insights/
                    │ (Queries MySQL for Live Status)       /data/machines/predictions/)
                    └─────────────────┬─────────────────────────────────┘
                                      │ (Queries HDFS Analytics & Predictions)
                            [ Django / Web Dashboard ]
                              (Port 8050, Light & Dark Theme)
```

---

## 4. Setup & Running Instructions

### Step 1: Database Setup
Execute the MySQL operational schema script:
```bash
mysql -u root < sql/01_schema.sql
```

### Step 2: HDFS Raw Directory Creation & Seeding
Create target directories in HDFS and populate initial historical data:
```cmd
hdfs dfs -mkdir -p /data/machines/raw
python scripts/seed_hdfs_data.py
```

### Step 3: Launch System Services

1. **Terminal 1 - Ingress & Stage Inspection REST API**:
   ```bash
   python producer/app.py
   ```

2. **Terminal 2 - Kafka Telemetry Consumer**:
   ```bash
   python consumer/consumer.py
   ```

3. **Terminal 3 - Telemetry Stream Generator**:
   ```bash
   python generator/generator.py --tps 3
   ```

4. **Terminal 4 - Web Dashboard**:
   ```bash
   python dashboard/app.py
   ```
   *Access dashboard at:* **`http://localhost:8050/`** (Includes Theme Toggle switch for Light/Dark mode).

---

## 5. PySpark Analytics & Machine Learning

- **PySpark Batch Insights (`spark/insights.py`)**: Computes branch temperature averages and overheating frequency, writing results directly to HDFS (`/data/machines/insights/`).
- **PySpark MLlib Model (`spark/train_model.py` & `score_batch.py`)**: Trains `LogisticRegression` on `cycle_temperature` trends and saves predicted breakdown flags (`breakdown_soon`) directly to HDFS (`/data/machines/predictions/`).
