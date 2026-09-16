"""
SpinWatch - Automatic HDFS Synchronization Script
Uploads all raw telemetry Parquet files, ML predictions, and PySpark insights
directly into HDFS cluster directories (hdfs://localhost:9000/data/machines/).
"""

import os
import glob
import subprocess

HDFS_CMD = r"C:\hadoop\bin\hdfs.cmd"

def upload_to_hdfs():
    print("[*] Synchronizing SpinWatch analytical data with HDFS cluster (hdfs://localhost:9000)...")

    # 1. Create HDFS Target Directories
    dirs = [
        "hdfs://localhost:9000/data/machines/raw",
        "hdfs://localhost:9000/data/machines/predictions",
        "hdfs://localhost:9000/data/machines/insights"
    ]
    for d in dirs:
        subprocess.run([HDFS_CMD, "dfs", "-mkdir", "-p", d], check=False)

    # 2. Upload Raw Telemetry Parquet Files
    parquet_files = [os.path.abspath(f) for f in glob.glob("data/machines/raw/*/*.parquet")]
    for pf in parquet_files:
        print(f"[*] Uploading Parquet file to HDFS: {pf}")
        subprocess.run([HDFS_CMD, "dfs", "-put", "-f", pf, "hdfs://localhost:9000/data/machines/raw/part-0000.parquet"], check=False)

    # 3. Upload ML Predictions JSON
    pred_path = os.path.abspath("data/machines/predictions/latest_predictions.json")
    if os.path.exists(pred_path):
        print(f"[*] Uploading ML predictions to HDFS: {pred_path}")
        subprocess.run([HDFS_CMD, "dfs", "-put", "-f", pred_path, "hdfs://localhost:9000/data/machines/predictions/"], check=False)

    # 4. Upload PySpark Heat Insights JSON
    insight_path = os.path.abspath("data/machines/insights/latest_insights.json")
    if os.path.exists(insight_path):
        print(f"[*] Uploading PySpark insights to HDFS: {insight_path}")
        subprocess.run([HDFS_CMD, "dfs", "-put", "-f", insight_path, "hdfs://localhost:9000/data/machines/insights/"], check=False)

    # 5. List HDFS Directory Contents for Verification
    print("\n================ HDFS DIRECTORY VERIFICATION ================")
    for d in dirs:
        print(f"\nContents of {d}:")
        subprocess.run([HDFS_CMD, "dfs", "-ls", d])
    print("=============================================================\n")

if __name__ == "__main__":
    upload_to_hdfs()
