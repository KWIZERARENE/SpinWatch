# SpinWatch — Line-by-Line Code Documentation

This document provides a line-by-line / block-by-block technical breakdown of every source file in the **SpinWatch** project repository.

---

## 1. Database Schema (`sql/01_schema.sql`)

```sql
-- SpinWatch Database Schema (MySQL)
CREATE DATABASE IF NOT EXISTS laundry_ops;
USE laundry_ops;
```
- **Lines 1–3**: Creates database `laundry_ops` if it doesn't already exist, and selects it for operational query execution.

```sql
CREATE TABLE IF NOT EXISTS machine_status (
    machine_id VARCHAR(20) PRIMARY KEY,
    branch VARCHAR(50) NOT NULL,
    cycle_temperature DECIMAL(5,2) NOT NULL,
    status VARCHAR(10) NOT NULL, -- 'NORMAL' or 'ALERT' (>70.0°C)
    last_updated DATETIME NOT NULL
);
```
- **Lines 5–11**: Defines `machine_status` table storing the single current operational state of each machine.
  - `machine_id`: Primary key identifier for the washer.
  - `branch`: City location.
  - `cycle_temperature`: Current cycle water/motor temperature.
  - `status`: String state (`NORMAL` or `ALERT`).
  - `last_updated`: Timestamp of last consumer write.

```sql
CREATE TABLE IF NOT EXISTS readings_log (
    reading_id CHAR(36) PRIMARY KEY,
    machine_id VARCHAR(20) NOT NULL,
    branch VARCHAR(50) NOT NULL,
    cycle_temperature DECIMAL(5,2) NOT NULL,
    txn_timestamp DATETIME NOT NULL,
    status VARCHAR(10) NOT NULL,
    breakdown_soon TINYINT DEFAULT 0
);
```
- **Lines 13–21**: Defines `readings_log` table storing raw telemetry stream rows landed into MySQL. Includes `breakdown_soon` label (0 or 1).

```sql
CREATE TABLE IF NOT EXISTS predictions (
    machine_id VARCHAR(20) PRIMARY KEY,
    branch VARCHAR(50) NOT NULL,
    prediction TINYINT NOT NULL, -- 1 = Breakdown Risk, 0 = Normal Operation
    scored_at DATETIME NOT NULL
);
```
- **Lines 23–28**: Defines `predictions` table populated by PySpark batch scoring job containing ML predictive failure flags.

```sql
CREATE TABLE IF NOT EXISTS insight_avg_temp_by_branch (
    branch VARCHAR(50) PRIMARY KEY,
    avg_temperature DECIMAL(5,2) NOT NULL
);
CREATE TABLE IF NOT EXISTS insight_time_in_alert (
    machine_id VARCHAR(20) PRIMARY KEY,
    branch VARCHAR(50) NOT NULL,
    alert_count INT NOT NULL
);
CREATE TABLE IF NOT EXISTS insight_busiest_alert_hour (
    hour INT PRIMARY KEY,
    alert_count INT NOT NULL
);
```
- **Lines 30–44**: Defines 3 insight tables holding aggregated metrics produced by PySpark analytical batch jobs.

---

## 2. Telemetry Stream Generator (`generator/generator.py`)

```python
import time, uuid, random, argparse, datetime, requests

BRANCHES = ["Kigali", "Musanze", "Huye", "Rubavu", "Rusizi", "Nyagatare"]
MACHINES = [{"machine_id": f"WM_{i:04d}", "branch": random.choice(BRANCHES)} for i in range(1, 31)]
base_temperature = {m["machine_id"]: random.uniform(40.0, 55.0) for m in MACHINES}
```
- **Lines 1–16**: Imports required standard libraries. Initializes 30 nationwide washing machines spread across 6 Rwandan branch cities, assigning a baseline temperature between 40°C and 55°C per machine.

```python
def generate_telemetry():
    parser = argparse.ArgumentParser(description="SpinWatch Heat Stream Telemetry Generator")
    parser.add_argument("--tps", type=float, default=3.0, help="Readings per second")
    parser.add_argument("--target-url", type=str, default="http://localhost:8000/api/readings/")
    args = parser.parse_args()
```
- **Lines 18–22**: Sets up CLI argument parsing for configurable reading generation velocity (`--tps`) and ingress REST endpoint target URL.

```python
    while True:
        machine = random.choice(MACHINES)
        mid = machine["machine_id"]
        drift = random.uniform(-0.5, 0.9)
        base_temperature[mid] += drift
        base_temperature[mid] = max(30.0, min(base_temperature[mid], 85.0))
        current_temp = round(base_temperature[mid], 2)
        status = "ALERT" if current_temp > 70.0 else "NORMAL"
```
- **Lines 27–34**: Infinite simulation loop. Selects a machine at random, applies thermal drift (-0.5°C to +0.9°C per cycle tick), clamps temperature between 30°C and 85°C, and evaluates the `status` rule (`ALERT` if `cycle_temperature > 70.0°C`).

```python
        reading = {
            "reading_id": str(uuid.uuid4()),
            "machine_id": mid,
            "branch": machine["branch"],
            "cycle_temperature": current_temp,
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "status": status,
            "breakdown_soon": 1 if current_temp > 68.0 else 0
        }
        requests.post(args.target_url, json=reading, timeout=2.0)
        time.sleep(1.0 / args.tps)
```
- **Lines 36–47**: Constructs JSON telemetry reading payload conforming to the exact target schema and posts it to the REST Ingress API at the specified `--tps` rate.

---

## 3. Kafka Producer (`producer/producer.py`)

```python
class MachineTelemetryProducer:
    def __init__(self, bootstrap_servers="localhost:9092", topic="machine-readings"):
        self.topic = topic
        if KAFKA_AVAILABLE:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers.split(","),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                enable_idempotence=True,
                retries=3
            )
```
- **Lines 12–25**: Initializes `KafkaProducer` instance with JSON value serialization, UTF-8 key serialization, idempotence enabled, and 3 automatic retry attempts.

```python
    def send_reading(self, reading: dict) -> bool:
        mid = reading.get("machine_id")
        if self.producer:
            future = self.producer.send(self.topic, key=mid, value=reading)
            future.get(timeout=2.0)
            return True
```
- **Lines 27–34**: Sends reading payload to Kafka topic `machine-readings`. **Crucially passes `key=mid` (`machine_id`) to ensure strict partition ordering per washer.**

---

## 4. Kafka Telemetry Consumer (`consumer/consumer.py`)

```python
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers.split(","),
    group_id="maintenance-tracker",
    enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest"
)
```
- **Lines 31–39**: Connects to Kafka as consumer group `maintenance-tracker`. **Sets `enable_auto_commit=False` to enable manual offset management for strictly guaranteed at-least-once delivery.**

```python
for msg in consumer:
    reading = msg.value
    # Process message and execute MySQL DB writes
    cursor.execute(upsert_status_sql, (mid, branch, temp, status, now_str))
    cursor.execute(insert_log_sql, (...))
    db_conn.commit()
    
    # Manual commit issued AFTER successful DB write
    consumer.commit()
```
- **Lines 49–82**: Consumes messages from Kafka, updates MySQL operational tables `machine_status` and `readings_log`, commits database transaction, and calls `consumer.commit()` **only after the write succeeds**.

---

## 5. PySpark Analytical Insights (`spark/insights.py`)

```python
spark = SparkSession.builder \
    .appName("SpinWatch-Heat-Insights") \
    .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
    .getOrCreate()
df = spark.read.parquet("hdfs://localhost:9000/data/machines/raw")
```
- **Lines 23–35**: Initializes PySpark session configured with MySQL JDBC driver. Reads historical Parquet files from HDFS directory `/data/machines/raw`.

```python
# Insight 1: Average temperature by branch
avg_temp_by_branch = df.groupBy("branch") \
    .agg(F.round(F.avg("cycle_temperature"), 2).alias("avg_temperature"))
avg_temp_by_branch.write.jdbc(url=JDBC_URL, table="insight_avg_temp_by_branch", mode="overwrite", properties=JDBC_PROPERTIES)
```
- **Lines 45–54**: Groups Parquet telemetry by `branch`, computes average cycle temperature, and writes result table to MySQL via JDBC.

---

## 6. PySpark MLlib Failure Model Trainer (`spark/train_model.py`)

```python
w = Window.partitionBy("machine_id").orderBy("txn_timestamp")
data = df.withColumn("next_temp", F.lead("cycle_temperature", 1).over(w)) \
    .withColumn("breakdown_soon", (F.col("next_temp") > 70.0).cast("int"))
```
- **Lines 45–48**: Uses PySpark Window function to examine the subsequent reading's temperature (`F.lead`), creating ground truth binary label `breakdown_soon = 1` if next temperature exceeds 70.0°C.

```python
branch_indexer = StringIndexer(inputCol="branch", outputCol="branch_idx")
assembler = VectorAssembler(inputCols=["cycle_temperature", "branch_idx"], outputCol="features")
lr = LogisticRegression(labelCol="breakdown_soon", featuresCol="features")
pipeline = Pipeline(stages=[branch_indexer, assembler, lr])
model = pipeline.fit(train_df)
```
- **Lines 50–57**: Encodes categorical branch strings into numbers, vectorizes features (`cycle_temperature` + `branch_idx`), fits `LogisticRegression` pipeline model, and saves trained model artifact to HDFS `/data/machines/models/lr_heat_v1`.

---

## 7. Web Dashboard Server (`dashboard/app.py`)

```python
class DashboardHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self.serve_template("templates/index.html")
        elif self.path == "/api/status":
            self.handle_api_status()
```
- **Lines 44–58**: HTTP request router. Serves static dashboard HTML template on root path and JSON API endpoints (`/api/status`, `/api/predictions`, `/api/insights`) querying local MySQL `laundry_ops`.

```python
def handle_api_status(self):
    rows = query_mysql("SELECT machine_id, branch, cycle_temperature, status, last_updated FROM machine_status ORDER BY cycle_temperature DESC")
    self.send_json(rows)
```
- **Lines 82–92**: Queries MySQL `machine_status` table, orders washers worst-first by temperature, formats datatypes, and returns JSON payload to the web dashboard frontend.
