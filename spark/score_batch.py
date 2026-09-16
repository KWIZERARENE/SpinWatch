"""
SpinWatch - PySpark ML Batch Scoring Job (Heat Focus)
Loads trained MLlib pipeline model from HDFS (/data/machines/models/lr_heat_v1).
Scores telemetry readings directly from HDFS raw historical storage (/data/machines/raw/).
Saves breakdown predictions DIRECTLY to HDFS Analytical Store (/data/machines/predictions/).
DOES NOT write predictions to MySQL database.
"""

import sys
import os
import json

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.ml import PipelineModel
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

from common.jdbc_config import JDBC_URL, JDBC_PROPERTIES

def run_batch_scoring():
    if not PYSPARK_AVAILABLE:
        print("[!] PySpark MLlib is not installed.")
        return

    print("[*] Initializing PySpark Session for Batch Machine Failure Scoring...")
    spark = SparkSession.builder \
        .appName("SpinWatch-MLlib-Score-Heat") \
        .getOrCreate()

    # Load Trained Model
    model_path = "hdfs://localhost:9000/data/machines/models/lr_heat_v1"
    try:
        model = PipelineModel.load(model_path)
        print(f"[*] Loaded MLlib PipelineModel from HDFS: {model_path}")
    except Exception:
        local_path = "./lr_heat_v1"
        model = PipelineModel.load(local_path)
        print(f"[*] Loaded MLlib PipelineModel from local path: {local_path}")

    # Read data directly from HDFS or MySQL operational view
    try:
        data = spark.read.parquet("hdfs://localhost:9000/data/machines/raw")
    except Exception:
        data = spark.read.format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "machine_status") \
            .option("user", JDBC_PROPERTIES["user"]) \
            .option("password", JDBC_PROPERTIES["password"]) \
            .load()

    # Predict breakdown risk
    scored = model.transform(data) \
        .select(
            F.col("machine_id"),
            F.col("branch"),
            F.col("prediction").cast("int").alias("prediction"),
            F.current_timestamp().alias("scored_at")
        )

    # Save predictions to HDFS Analytical Store (NOT MySQL)
    hdfs_pred_path = "hdfs://localhost:9000/data/machines/predictions/scored"
    local_pred_dir = os.path.join(os.getcwd(), "data", "machines", "predictions")
    os.makedirs(local_pred_dir, exist_ok=True)

    try:
        scored.write.mode("overwrite").parquet(hdfs_pred_path)
        print(f"[+] Saved PySpark ML predictions directly to HDFS: {hdfs_pred_path}")
    except Exception as e:
        print(f"[!] HDFS write bypassed ({e}). Saving predictions to local analytical store...")

    # Output JSON predictions file for dashboard ingestion
    pred_list = [row.asDict() for row in scored.collect()]
    for p in pred_list:
        if "scored_at" in p and p["scored_at"]:
            p["scored_at"] = str(p["scored_at"])

    json_pred_path = os.path.join(local_pred_dir, "latest_predictions.json")
    with open(json_pred_path, "w") as f:
        json.dump(pred_list, f, indent=2)

    print(f"[+] Saved ML predictions to analytical store: {json_pred_path}")
    spark.stop()

if __name__ == "__main__":
    run_batch_scoring()
