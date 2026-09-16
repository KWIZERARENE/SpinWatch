"""
SpinWatch - PySpark Batch Insights Processor
Reads raw historical readings from HDFS (/data/machines/raw/) in Parquet format.
Computes 3 heat analytics insights and writes results DIRECTLY to HDFS Analytical Storage (/data/machines/insights/).
DOES NOT write analytics to MySQL operational database.
"""

import sys
import os
import json

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

def run_pyspark_insights(hdfs_raw_path="hdfs://localhost:9000/data/machines/raw"):
    if not PYSPARK_AVAILABLE:
        print("[!] PySpark is not installed in the Python environment.")
        return

    print("[*] Initializing PySpark Session for SpinWatch Heat Analytics...")
    spark = SparkSession.builder \
        .appName("SpinWatch-Heat-Insights") \
        .getOrCreate()

    try:
        print(f"[*] Reading historical Parquet telemetry from HDFS: {hdfs_raw_path}...")
        df = spark.read.parquet(hdfs_raw_path)
    except Exception as e:
        local_raw = "./data/machines/raw"
        print(f"[!] HDFS Read bypassed ({e}). Reading from local analytical store: {local_raw}...")
        df = spark.read.option("recursiveFileLookup", "true").json(local_raw)

    df.printSchema()

    # Insight 1: Average cycle temperature per branch (°C)
    print("[*] Computing Insight 1: Average cycle_temperature per branch...")
    avg_temp_by_branch = df.groupBy("branch") \
        .agg(F.round(F.avg("cycle_temperature"), 2).alias("avg_temperature"))

    # Insight 2: Machines with highest frequency of heat ALERT readings (>70°C)
    print("[*] Computing Insight 2: Time spent in heat ALERT state per machine...")
    time_in_alert = df.filter(F.col("cycle_temperature") > 70.0) \
        .groupBy("machine_id", "branch") \
        .agg(F.count("*").alias("alert_count")) \
        .orderBy(F.col("alert_count").desc())

    # Insight 3: Hour of day with the most ALERT readings nationwide
    print("[*] Computing Insight 3: Hourly heat ALERT distribution...")
    busiest_alert_hour = df.filter(F.col("cycle_temperature") > 70.0) \
        .withColumn("hour", F.hour(F.col("txn_timestamp"))) \
        .groupBy("hour") \
        .agg(F.count("*").alias("alert_count")) \
        .orderBy(F.col("alert_count").desc())

    # Save Insights directly to HDFS Analytical Store (NOT MySQL)
    hdfs_insights_base = "hdfs://localhost:9000/data/machines/insights"
    local_insights_base = os.path.join(os.getcwd(), "data", "machines", "insights")
    os.makedirs(local_insights_base, exist_ok=True)

    try:
        avg_temp_by_branch.write.mode("overwrite").parquet(f"{hdfs_insights_base}/avg_temp_by_branch")
        time_in_alert.write.mode("overwrite").parquet(f"{hdfs_insights_base}/time_in_alert")
        busiest_alert_hour.write.mode("overwrite").parquet(f"{hdfs_insights_base}/busiest_alert_hour")
        print(f"[+] Saved insights directly to HDFS: {hdfs_insights_base}")
    except Exception as e:
        print(f"[!] HDFS cluster write bypassed ({e}). Saving insights to local analytical storage...")

    # Also output JSON files for direct dashboard reading
    avg_list = [row.asDict() for row in avg_temp_by_branch.collect()]
    alert_list = [row.asDict() for row in time_in_alert.collect()]
    hour_list = [row.asDict() for row in busiest_alert_hour.collect()]

    insights_payload = {
        "avg_by_branch": avg_list,
        "time_in_alert": alert_list,
        "busiest_hour": hour_list
    }

    insights_json_path = os.path.join(local_insights_base, "latest_insights.json")
    with open(insights_json_path, "w") as f:
        json.dump(insights_payload, f, indent=2)

    print(f"[+] Saved PySpark insights to analytical file store: {insights_json_path}")
    spark.stop()

if __name__ == "__main__":
    run_pyspark_insights()
