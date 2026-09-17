"""
SpinWatch Master Runner Script
Launches all SpinWatch services and processes in one single command:
1. Seeds initial HDFS & MySQL data (scripts/seed_hdfs_data.py)
2. Syncs data to HDFS cluster (scripts/upload_to_hdfs.py)
3. Starts Ingress REST API Server (producer/app.py) on Port 8000
4. Starts Telemetry Stream Generator (generator/generator.py) at 3 TPS
5. Starts Web Dashboard Server (dashboard/app.py) on Port 8050
"""

import sys
import time
import subprocess

def main():
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass

    print("==================================================================")
    print("           [+] SPINWATCH BIG DATA PIPELINE RUNNER                 ")
    print("==================================================================")

    # 1. Seed & Sync Data
    print("\n[Step 1/4] Seeding initial HDFS & MySQL database data...")
    subprocess.run([sys.executable, "scripts/seed_hdfs_data.py"])
    subprocess.run([sys.executable, "scripts/upload_to_hdfs.py"])

    # 2. Launch Ingress API Server
    print("\n[Step 2/5] Starting Ingress REST API Server on http://localhost:8000/...")
    producer_proc = subprocess.Popen([sys.executable, "producer/app.py"])
    time.sleep(3.0)

    # 3. Launch Kafka Telemetry Consumer
    print("\n[Step 3/5] Starting Kafka Telemetry Consumer (maintenance-tracker group)...")
    consumer_proc = subprocess.Popen([sys.executable, "consumer/consumer.py"])
    time.sleep(1.5)

    # 4. Launch Stream Generator
    print("\n[Step 4/5] Starting Continuous Telemetry Generator at 3 TPS...")
    generator_proc = subprocess.Popen([sys.executable, "generator/generator.py", "--tps", "3"])
    time.sleep(1.5)

    # 5. Launch Web Dashboard
    print("\n[Step 5/5] Starting Web Dashboard Server on http://localhost:8050/...")
    dashboard_proc = subprocess.Popen([sys.executable, "dashboard/app.py"])
    time.sleep(1.0)

    print("\n==================================================================")
    print("  [+] ALL SPINWATCH SERVICES ARE LIVE AND RUNNING!")
    print("  -> Web Dashboard URL : http://localhost:8050/")
    print("  -> REST Ingress API  : http://localhost:8000/api/readings/")
    print("  -> Postman APIs      : http://localhost:8000/api/kafka/status")
    print("==================================================================")
    print("Press Ctrl+C in this terminal to stop all project processes.\n")

    try:
        producer_proc.wait()
        consumer_proc.wait()
        generator_proc.wait()
        dashboard_proc.wait()
    except KeyboardInterrupt:
        print("\n[*] Stopping all SpinWatch processes...")
        producer_proc.terminate()
        consumer_proc.terminate()
        generator_proc.terminate()
        dashboard_proc.terminate()
        print("[+] All SpinWatch services stopped successfully.")

if __name__ == "__main__":
    main()
