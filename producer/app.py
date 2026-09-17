"""
SpinWatch - Unified REST API Ingress & Pipeline Inspection Server
Exposes REST endpoints to test, query, and verify data at every stage of the Big Data Pipeline:
- Point 1: POST /api/readings/ (Generator Ingress) & GET /api/generator/status
- Point 2: GET /api/kafka/status (Kafka Topic & Partition Inspector)
- Point 3a: GET /api/hdfs/raw (HDFS Historical Raw Data Inspector)
            GET /api/hdfs/dates (HDFS Available Partition Dates List)
            GET /api/hdfs/history?dt=YYYY-MM-DD (HDFS Historical Time-Travel Query)
- Point 3b: GET /api/sql/readings (MySQL Operational Storage Inspector)
- Point 4a: GET /api/predictions (PySpark MLlib Breakdown Failure Predictions from HDFS)
- Point 4b: GET /api/insights (PySpark Analytics & Heat Insights from HDFS)
- Point 5: GET /api/consumer/live (Python Consumer Application Live Stream Broadcast)
"""

import os
import json
import glob
import datetime
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from producer import MachineTelemetryProducer

producer = MachineTelemetryProducer(bootstrap_servers="localhost:9092", topic="machine-readings")

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
    "database": "laundry_ops"
}

def query_mysql(sql, params=None):
    if not MYSQL_AVAILABLE:
        return None
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, params or ())
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    except Exception as e:
        print(f"[!] API MySQL query exception: {e}")
        return None

class UnifiedPipelineAPIHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path in ["/api/readings/", "/api/readings"]:
            content_length = int(self.headers.get("Content-Length", 0))
            post_data = self.rfile.read(content_length)
            
            try:
                reading = json.loads(post_data.decode("utf-8"))
                required_fields = ["machine_id", "branch", "cycle_temperature", "timestamp", "status"]
                for f in required_fields:
                    if f not in reading:
                        raise ValueError(f"Missing required field: {f}")
                
                success = producer.send_reading(reading)
                
                self.send_json({
                    "stage": "Point 1: Generator Ingress -> Kafka Producer",
                    "status": "success",
                    "message": "Telemetry received and published to Kafka topic 'machine-readings'",
                    "payload": reading
                }, status_code=201 if success else 200)
            except Exception as e:
                self.send_json({"stage": "Point 1", "status": "error", "message": str(e)}, status_code=400)
        else:
            self.send_error(404, "Endpoint not found")

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query_params = urllib.parse.parse_qs(parsed.query)

        # 1. Generator Ingress Check
        if path in ["/api/generator/status", "/api/stage1"]:
            self.send_json({
                "stage": "Point 1: Telemetry Stream Generator",
                "status": "ACTIVE",
                "branches_count": 13,
                "fleet_size": 1000,
                "ingress_url": "http://localhost:8000/api/readings/",
                "schema": ["reading_id", "machine_id", "branch", "cycle_temperature", "timestamp", "status", "breakdown_soon"]
            })

        # 2. Kafka Topic & Partition Status Check
        elif path in ["/api/kafka/status", "/api/stage2"]:
            self.send_json({
                "stage": "Point 2: Apache Kafka Ecosystem",
                "topic": "machine-readings",
                "partitions": 3,
                "replication_factor": 1,
                "consumer_group": "maintenance-tracker",
                "partitioning_key": "machine_id (Guarantees strict washer message sequence)",
                "kafka_status": "AVAILABLE" if producer.producer else "LOCAL_FALLBACK"
            })

        # 3a. HDFS Historical Storage & Date Partition Inspector
        elif path in ["/api/hdfs/raw", "/api/stage3/hdfs"]:
            hdfs_dir = os.path.join(os.getcwd(), "data", "machines", "raw")
            partition_dirs = [os.path.basename(d) for d in glob.glob(os.path.join(hdfs_dir, "dt=*"))]
            
            sample_records = []
            files = glob.glob(os.path.join(hdfs_dir, "**", "*.*"), recursive=True)
            for fpath in files:
                if fpath.endswith(".json"):
                    try:
                        with open(fpath, "r") as f:
                            for line in f.readlines()[:5]:
                                sample_records.append(json.loads(line.strip()))
                    except Exception:
                        pass

            self.send_json({
                "stage": "Point 3a: HDFS Distributed Historical Storage (/data/machines/raw/)",
                "hdfs_directory": "/data/machines/raw/",
                "partition_count": len(partition_dirs),
                "available_date_partitions": partition_dirs,
                "storage_format": "Parquet / JSON",
                "sample_landed_records": sample_records[:5]
            })

        # HDFS Available Date Partitions API
        elif path == "/api/hdfs/dates":
            hdfs_dir = os.path.join(os.getcwd(), "data", "machines", "raw")
            partition_dirs = [os.path.basename(d).replace("dt=", "") for d in glob.glob(os.path.join(hdfs_dir, "dt=*"))]
            self.send_json({
                "stage": "HDFS Date Partitions",
                "dates": sorted(partition_dirs, reverse=True)
            })

        # HDFS Historical Time-Travel Query API
        elif path == "/api/hdfs/history":
            target_dt = query_params.get("dt", [datetime.datetime.now().strftime("%Y-%m-%d")])[0]
            dt_dir = os.path.join(os.getcwd(), "data", "machines", "raw", f"dt={target_dt}")
            records = []

            if os.path.exists(dt_dir):
                json_files = glob.glob(os.path.join(dt_dir, "*.json"))
                for jf in json_files:
                    try:
                        with open(jf, "r") as f:
                            records.extend([json.loads(line.strip()) for line in f.readlines()[:100]])
                    except Exception:
                        pass
                
                # If Parquet file exists, try pandas/pyarrow read
                parquet_files = glob.glob(os.path.join(dt_dir, "*.parquet"))
                if parquet_files and not records:
                    try:
                        import pandas as pd
                        df = pd.read_parquet(parquet_files[0])
                        records = df.head(100).to_dict(orient="records")
                    except Exception:
                        pass

            self.send_json({
                "stage": "HDFS Historical Time-Travel Query",
                "queried_date": target_dt,
                "record_count": len(records),
                "hdfs_path": f"/data/machines/raw/dt={target_dt}/",
                "records": records[:50]
            })

        # 3b. MySQL Operational Storage Check
        elif path in ["/api/sql/readings", "/api/stage3/sql"]:
            readings = query_mysql("SELECT reading_id, machine_id, branch, cycle_temperature, txn_timestamp, status, breakdown_soon FROM readings_log ORDER BY txn_timestamp DESC LIMIT 15")
            status_summary = query_mysql("SELECT status, count(*) as count FROM machine_status GROUP BY status")
            
            if readings:
                for r in readings:
                    if "txn_timestamp" in r and r["txn_timestamp"]:
                        r["txn_timestamp"] = str(r["txn_timestamp"])
                    if "cycle_temperature" in r:
                        r["cycle_temperature"] = float(r["cycle_temperature"])

            self.send_json({
                "stage": "Point 3b: MySQL Operational Storage (laundry_ops)",
                "table_target": "readings_log & machine_status",
                "status_summary": status_summary or [],
                "latest_landed_readings": readings or []
            })

        # 4a. PySpark MLlib Predictive Model Scoring Check (Read from HDFS)
        elif path in ["/api/predictions", "/api/stage4/predictions"]:
            pred_json_path = os.path.join(os.getcwd(), "data", "machines", "predictions", "latest_predictions.json")
            preds = []
            if os.path.exists(pred_json_path):
                try:
                    with open(pred_json_path, "r") as f:
                        preds = json.load(f)
                except Exception:
                    pass

            self.send_json({
                "stage": "Point 4a: PySpark MLlib Predictive Model Scoring (Stored in HDFS)",
                "hdfs_location": "/data/machines/predictions/",
                "model": "LogisticRegression (lr_heat_v1)",
                "prediction_key": "1 = Breakdown Soon (Impending Fault), 0 = Normal Operation",
                "total_scored_machines": len(preds),
                "predictions": preds
            })

        # 4b. PySpark Analytical Heat Insights Check (Read from HDFS)
        elif path in ["/api/insights", "/api/stage4/insights"]:
            insights_json_path = os.path.join(os.getcwd(), "data", "machines", "insights", "latest_insights.json")
            insights_data = {"avg_by_branch": [], "time_in_alert": []}
            if os.path.exists(insights_json_path):
                try:
                    with open(insights_json_path, "r") as f:
                        insights_data = json.load(f)
                except Exception:
                    pass

            self.send_json({
                "stage": "Point 4b: PySpark Analytical Heat Insights (Stored in HDFS)",
                "hdfs_location": "/data/machines/insights/",
                "avg_temp_by_branch": insights_data.get("avg_by_branch", []),
                "top_overheating_machines": insights_data.get("time_in_alert", [])
            })

        # 5. Python Consumer Live Broadcast Endpoint
        elif path == "/api/consumer/live":
            from consumer.consumer import LIVE_CONSUMER_BUFFER
            self.send_json({
                "stage": "Python Consumer Live Stream Broadcast",
                "buffer_size": len(LIVE_CONSUMER_BUFFER),
                "recent_consumer_events": LIVE_CONSUMER_BUFFER[-10:]
            })
        else:
            self.send_error(404, "Endpoint not found")

    def send_json(self, data, status_code=200):
        content = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, format, *args):
        return

def run_server(port=8000):
    server_address = ("", port)
    httpd = HTTPServer(server_address, UnifiedPipelineAPIHandler)
    print(f"[*] SpinWatch Stage Inspection REST API listening on http://localhost:{port}/api/")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[*] REST API server stopped.")

if __name__ == "__main__":
    run_server()
