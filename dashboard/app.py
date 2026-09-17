"""
SpinWatch Web Dashboard Server
Serves heat monitoring dashboard UI & REST APIs reading from:
1. MySQL (laundry_ops) for Live Operational Views (machine_status, readings_log).
2. HDFS Analytical Storage (data/machines/insights/ & predictions/) for PySpark Analytics & ML Predictions.
3. HDFS Historical Time-Travel Archive (/data/machines/raw/dt=YYYY-MM-DD/).
4. Python Consumer Application Live Event Buffer.
"""

import os
import json
import glob
import datetime
from http.server import HTTPServer, SimpleHTTPRequestHandler
import urllib.parse

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
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query_params = urllib.parse.parse_qs(parsed.query)

        if path == "/" or path == "/index.html":
            self.serve_template("templates/index.html")
        elif path.startswith("/static/"):
            file_path = path.lstrip("/")
            if os.path.exists(file_path):
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
        else:
            self.send_error(404, "Endpoint not found")

    def serve_template(self, relative_path):
        if os.path.exists(relative_path):
            with open(relative_path, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        else:
            self.send_error(404, "Template file missing")

    def serve_file(self, file_path):
        mime = "text/css" if file_path.endswith(".css") else "application/javascript"
        with open(file_path, "rb") as f:
            content = f.read()
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def handle_api_status(self):
        # Operational View: Query MySQL machine_status table
        rows = query_mysql("SELECT machine_id, branch, cycle_temperature, status, last_updated FROM machine_status ORDER BY cycle_temperature DESC LIMIT 100")
        if not rows:
            rows = [
                {"machine_id": "WM_0001", "branch": "Kigali", "cycle_temperature": 74.5, "status": "ALERT", "last_updated": "2026-09-17 07:00:00"},
                {"machine_id": "WM_0004", "branch": "Musanze", "cycle_temperature": 52.1, "status": "NORMAL", "last_updated": "2026-09-17 07:00:00"}
            ]

        for r in rows:
            if "last_updated" in r and r["last_updated"]:
                r["last_updated"] = str(r["last_updated"])
            if "cycle_temperature" in r:
                r["cycle_temperature"] = float(r["cycle_temperature"])

        self.send_json(rows)

    def handle_api_predictions(self):
        # Predictive View: Read directly from HDFS Analytical Storage
        pred_json_path = os.path.join(os.getcwd(), "..", "data", "machines", "predictions", "latest_predictions.json")
        if not os.path.exists(pred_json_path):
            pred_json_path = os.path.join(os.getcwd(), "data", "machines", "predictions", "latest_predictions.json")

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
        # Analytical Insights View: Read directly from HDFS Analytical Storage
        insights_json_path = os.path.join(os.getcwd(), "..", "data", "machines", "insights", "latest_insights.json")
        if not os.path.exists(insights_json_path):
            insights_json_path = os.path.join(os.getcwd(), "data", "machines", "insights", "latest_insights.json")

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
        raw_base = os.path.join(os.getcwd(), "..", "data", "machines", "raw")
        if not os.path.exists(raw_base):
            raw_base = os.path.join(os.getcwd(), "data", "machines", "raw")

        dates = [os.path.basename(d).replace("dt=", "") for d in glob.glob(os.path.join(raw_base, "dt=*"))]
        self.send_json(sorted(dates, reverse=True))

    def handle_api_hdfs_history(self, target_dt):
        raw_base = os.path.join(os.getcwd(), "..", "data", "machines", "raw", f"dt={target_dt}")
        if not os.path.exists(raw_base):
            raw_base = os.path.join(os.getcwd(), "data", "machines", "raw", f"dt={target_dt}")

        records = []
        if os.path.exists(raw_base):
            parquet_files = glob.glob(os.path.join(raw_base, "*.parquet"))
            if parquet_files:
                try:
                    import pandas as pd
                    df = pd.read_parquet(parquet_files[0])
                    records = df.head(100).to_dict(orient="records")
                except Exception:
                    pass

            if not records:
                json_files = glob.glob(os.path.join(raw_base, "*.json"))
                for jf in json_files:
                    try:
                        with open(jf, "r") as f:
                            records.extend([json.loads(line.strip()) for line in f.readlines()[:100]])
                    except Exception:
                        pass

        self.send_json({
            "date": target_dt,
            "count": len(records),
            "records": records[:50]
        })

    def handle_api_consumer_live(self):
        from consumer.consumer import LIVE_CONSUMER_BUFFER
        self.send_json(LIVE_CONSUMER_BUFFER[-10:])

    def send_json(self, data):
        content = json.dumps(data).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(content)

    def log_message(self, format, *args):
        return

def run_dashboard(port=8050):
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    server = HTTPServer(("", port), DashboardHandler)
    print(f"[*] SpinWatch Dashboard running at http://localhost:{port}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[*] Dashboard server stopped.")

if __name__ == "__main__":
    run_dashboard()
