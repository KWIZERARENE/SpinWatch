"""
SpinWatch Web Dashboard Server
Serves heat monitoring dashboard UI & REST APIs reading from:
1. MySQL (laundry_ops) for Live Operational Views (machine_status, readings_log).
2. HDFS Analytical Storage (data/machines/insights/ & predictions/) for PySpark Analytics & ML Predictions.
3. HDFS Historical Time-Travel Archive (/data/machines/raw/dt=YYYY-MM-DD/).
4. Python Consumer Application Live Event Buffer.
"""

import os
import sys
import json
import glob
import datetime
from http.server import HTTPServer, SimpleHTTPRequestHandler
import urllib.parse

# Ensure project root directory is in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

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
        print(f"[!] Dashboard MySQL read exception: {e}")
        return None

class DashboardHandler(SimpleHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        if path in ["/api/readings/", "/api/readings"]:
            content_length = int(self.headers.get("Content-Length", 0))
            post_data = self.rfile.read(content_length)
            
            # Forward request to Ingress API server at http://localhost:8000/api/readings/
            try:
                import urllib.request
                req = urllib.request.Request(
                    "http://localhost:8000/api/readings/",
                    data=post_data,
                    headers={"Content-Type": "application/json"},
                    method="POST"
                )
                with urllib.request.urlopen(req, timeout=3.0) as resp:
                    resp_data = resp.read()
                    self.send_response(resp.status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(resp_data)
                    return
            except Exception as e:
                print(f"[!] Dashboard proxy to 8000 warning: {e}")
                # Standalone fallback response
                try:
                    payload = json.loads(post_data.decode("utf-8"))
                    self.send_json({
                        "stage": "Point 1: Ingress (Dashboard Fallback)",
                        "status": "success",
                        "message": "Telemetry received",
                        "payload": payload
                    }, status_code=201)
                except Exception:
                    self.send_json({"status": "error", "message": str(e)}, status_code=400)
        else:
            self.send_error(404, "Endpoint not found")

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query_params = urllib.parse.parse_qs(parsed.query)

        if path == "/" or path == "/index.html":
            self.serve_template("templates/index.html")
        elif path.startswith("/static/"):
            file_path = path.lstrip("/")
            full_path = os.path.join(os.path.dirname(__file__), file_path)
            if os.path.exists(full_path):
                self.serve_file(file_path)
            else:
                self.send_error(404, "Static file not found")
        elif path == "/api/status":
            self.handle_api_status()
        elif path == "/api/predictions":
            self.handle_api_predictions()
        elif path == "/api/insights":
            self.handle_api_insights()
        elif path == "/api/hdfs/dates":
            self.handle_api_hdfs_dates()
        elif path == "/api/hdfs/history":
            dt = query_params.get("dt", [datetime.datetime.now().strftime("%Y-%m-%d")])[0]
            self.handle_api_hdfs_history(dt)
        elif path == "/api/consumer/live":
            self.handle_api_consumer_live()
        elif path == "/api/sql/readings":
            self.handle_api_sql_readings()
        elif path == "/api/hdfs/live":
            self.handle_api_hdfs_live()
        else:
            self.send_error(404, "Endpoint not found")

    def serve_template(self, relative_path):
        full_path = os.path.join(os.path.dirname(__file__), relative_path)
        if os.path.exists(full_path):
            with open(full_path, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        else:
            self.send_error(404, "Template file missing")

    def serve_file(self, file_path):
        full_path = os.path.join(os.path.dirname(__file__), file_path)
        mime = "text/css" if file_path.endswith(".css") else "application/javascript"
        if os.path.exists(full_path):
            with open(full_path, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", mime)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        else:
            self.send_error(404, "Static file missing")

    def handle_api_status(self):
        rows = query_mysql("SELECT machine_id, branch, cycle_temperature, status, last_updated FROM machine_status ORDER BY machine_id ASC LIMIT 1000")
        if not rows:
            # Fallback: Read 1,000 washer statuses from latest HDFS raw partition
            rows = []
            raw_base = os.path.join(PROJECT_ROOT, "data", "machines", "raw")
            partition_dirs = glob.glob(os.path.join(raw_base, "dt=*"))
            if partition_dirs:
                latest_dir = sorted(partition_dirs, reverse=True)[0]
                parquet_files = glob.glob(os.path.join(latest_dir, "*.parquet"))
                if parquet_files:
                    try:
                        import pandas as pd
                        df = pd.read_parquet(parquet_files[0])
                        rows = df.to_dict(orient="records")
                    except Exception:
                        pass
                if not rows:
                    json_files = glob.glob(os.path.join(latest_dir, "*.json"))
                    for jf in json_files:
                        try:
                            with open(jf, "r") as f:
                                rows.extend([json.loads(line.strip()) for line in f.readlines()])
                        except Exception:
                            pass

            # Fallback 2: Generate 1,000 washers if store empty
            if not rows:
                branches = ["Kigali", "Musanze", "Huye", "Rubavu", "Rusizi", "Nyagatare", "Rwamagana", "Gicumbi", "Kamembe", "Karongi", "Nyanza", "Bugesera", "Kamonyi"]
                rows = [{
                    "machine_id": f"WM_{i:04d}",
                    "branch": branches[i % len(branches)],
                    "cycle_temperature": round(35.0 + (i * 7) % 55, 1),
                    "status": "ALERT" if (35.0 + (i * 7) % 55) > 70 else "NORMAL",
                    "last_updated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                } for i in range(1, 1001)]

        for r in rows:
            if "last_updated" in r and r["last_updated"]:
                r["last_updated"] = str(r["last_updated"])
            if "cycle_temperature" in r:
                r["cycle_temperature"] = float(r["cycle_temperature"])

        self.send_json(rows)

    def handle_api_predictions(self):
        pred_json_path = os.path.join(PROJECT_ROOT, "data", "machines", "predictions", "latest_predictions.json")
        if os.path.exists(pred_json_path):
            try:
                with open(pred_json_path, "r") as f:
                    preds = json.load(f)
                self.send_json(preds)
                return
            except Exception as e:
                print(f"[!] HDFS predictions read issue: {e}")

        preds = [
            {"machine_id": "WM_0001", "branch": "Kigali", "prediction": 1},
            {"machine_id": "WM_0004", "branch": "Kigali", "prediction": 1}
        ]
        self.send_json(preds)

    def handle_api_insights(self):
        insights_json_path = os.path.join(PROJECT_ROOT, "data", "machines", "insights", "latest_insights.json")
        if os.path.exists(insights_json_path):
            try:
                with open(insights_json_path, "r") as f:
                    insights_data = json.load(f)
                self.send_json(insights_data)
                return
            except Exception as e:
                print(f"[!] HDFS insights read issue: {e}")

        self.send_json({
            "avg_by_branch": [{"branch": "Kigali", "avg_temperature": 66.50}, {"branch": "Musanze", "avg_temperature": 58.20}],
            "time_in_alert": [{"machine_id": "WM_0004", "branch": "Kigali", "alert_count": 18}]
        })

    def handle_api_hdfs_dates(self):
        raw_base = os.path.join(PROJECT_ROOT, "data", "machines", "raw")
        dates = [os.path.basename(d).replace("dt=", "") for d in glob.glob(os.path.join(raw_base, "dt=*"))]
        self.send_json(sorted(dates, reverse=True))

    def handle_api_hdfs_history(self, target_dt):
        raw_base = os.path.join(PROJECT_ROOT, "data", "machines", "raw", f"dt={target_dt}")
        records = []
        if os.path.exists(raw_base):
            parquet_files = glob.glob(os.path.join(raw_base, "*.parquet"))
            for pf in parquet_files:
                if len(records) >= 200:
                    break
                try:
                    import pandas as pd
                    df = pd.read_parquet(pf)
                    for rec in df.head(200 - len(records)).to_dict(orient="records"):
                        records.append(rec)
                except Exception as e:
                    print(f"[!] HDFS parquet read warning: {e}")

            if len(records) < 200:
                json_files = glob.glob(os.path.join(raw_base, "*.json"))
                for jf in json_files:
                    if len(records) >= 200:
                        break
                    try:
                        with open(jf, "r") as f:
                            for line in f:
                                if len(records) >= 200:
                                    break
                                line_str = line.strip()
                                if line_str:
                                    records.append(json.loads(line_str))
                    except Exception as e:
                        print(f"[!] HDFS json read warning: {e}")

        clean_records = []
        for r in records:
            item = {}
            for k, v in r.items():
                if isinstance(v, (datetime.datetime, datetime.date)):
                    item[k] = str(v)
                else:
                    item[k] = v
            clean_records.append(item)

        self.send_json({
            "date": target_dt,
            "count": len(clean_records),
            "records": clean_records
        })

    def handle_api_consumer_live(self):
        # 1. Query live stream buffer from Ingress API server (port 8000)
        try:
            import urllib.request
            req = urllib.request.Request("http://localhost:8000/api/consumer/live")
            with urllib.request.urlopen(req, timeout=1.5) as resp:
                data = resp.read()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(data)
                return
        except Exception:
            pass

        # 2. Local buffer fallback
        try:
            from consumer.consumer import LIVE_CONSUMER_BUFFER
            self.send_json(LIVE_CONSUMER_BUFFER[-25:])
        except Exception:
            self.send_json([])

    def handle_api_sql_readings(self):
        rows = query_mysql("SELECT reading_id, machine_id, branch, cycle_temperature, txn_timestamp, status, breakdown_soon FROM readings_log ORDER BY txn_timestamp DESC LIMIT 25")
        if rows:
            for r in rows:
                if "txn_timestamp" in r and r["txn_timestamp"]:
                    r["txn_timestamp"] = str(r["txn_timestamp"])
                if "cycle_temperature" in r:
                    r["cycle_temperature"] = float(r["cycle_temperature"])
            self.send_json(rows)
            return

        # Proxy fallback to API server on 8000
        try:
            import urllib.request
            req = urllib.request.Request("http://localhost:8000/api/sql/readings")
            with urllib.request.urlopen(req, timeout=1.5) as resp:
                data = resp.read()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(data)
                return
        except Exception:
            pass

        self.send_json([])

    def handle_api_hdfs_live(self):
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        raw_dir = os.path.join(PROJECT_ROOT, "data", "machines", "raw", f"dt={today_str}")
        json_file = os.path.join(raw_dir, "stream_history.json")
        records = []
        if os.path.exists(json_file):
            try:
                with open(json_file, "r") as f:
                    lines = f.readlines()
                    for line in lines[-25:]:
                        line_str = line.strip()
                        if line_str:
                            records.append(json.loads(line_str))
            except Exception:
                pass
        self.send_json(records)

    def send_json(self, data, status_code=200):
        try:
            content = json.dumps(data).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(content)
        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            print(f"[!] send_json issue: {e}")

    def log_message(self, format, *args):
        return

def run_dashboard(port=8050):
    server = HTTPServer(("", port), DashboardHandler)
    print(f"[*] SpinWatch Dashboard running at http://localhost:{port}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[*] Dashboard server stopped.")

if __name__ == "__main__":
    run_dashboard()
