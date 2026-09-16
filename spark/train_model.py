"""
SpinWatch - PySpark MLlib Machine Failure Model Trainer (Heat Focus)
Trains a LogisticRegression pipeline model predicting `breakdown_soon` based on cycle_temperature and location.
Evaluates model performance using AUC, Precision, and Recall metrics.
Saves model artifact to HDFS (/data/machines/models/lr_heat_v1).
"""

import sys
import os

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import VectorAssembler, StringIndexer
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

from common.jdbc_config import JDBC_URL, JDBC_PROPERTIES

def train_heat_failure_model():
    if not PYSPARK_AVAILABLE:
        print("[!] PySpark MLlib is not installed.")
        return

    print("[*] Starting PySpark Session for Model Training...")
    spark = SparkSession.builder \
        .appName("SpinWatch-MLlib-Train-Heat") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
        .getOrCreate()

    try:
        df = spark.read.parquet("hdfs://localhost:9000/data/machines/raw")
    except Exception:
        print("[!] Reading from MySQL readings_log for model training...")
        df = spark.read.format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "readings_log") \
            .option("user", JDBC_PROPERTIES["user"]) \
            .option("password", JDBC_PROPERTIES["password"]) \
            .load()

    # Construct lead-window to accurately label `breakdown_soon` (1 if next reading temp > 70.0°C)
    w = Window.partitionBy("machine_id").orderBy("txn_timestamp")
    data = df.withColumn("next_temp", F.lead("cycle_temperature", 1).over(w)) \
        .withColumn("breakdown_soon", (F.col("next_temp") > 70.0).cast("int")) \
        .filter(F.col("next_temp").isNotNull())

    # Categorical Indexing & Feature Vector Assembly
    branch_indexer = StringIndexer(inputCol="branch", outputCol="branch_idx", handleInvalid="keep")
    assembler = VectorAssembler(
        inputCols=["cycle_temperature", "branch_idx"],
        outputCol="features"
    )

    lr = LogisticRegression(labelCol="breakdown_soon", featuresCol="features", maxIter=20)
    pipeline = Pipeline(stages=[branch_indexer, assembler, lr])

    # Split dataset into 80% Training and 20% Testing
    train_df, test_df = data.randomSplit([0.8, 0.2], seed=42)
    print(f"[*] Training on {train_df.count()} samples, testing on {test_df.count()} samples...")

    model = pipeline.fit(train_df)

    # Evaluate Model Predictions
    preds = model.transform(test_df)
    
    eval_auc = BinaryClassificationEvaluator(labelCol="breakdown_soon", rawPredictionCol="rawPrediction")
    eval_prec = MulticlassClassificationEvaluator(labelCol="breakdown_soon", metricName="weightedPrecision")
    eval_rec = MulticlassClassificationEvaluator(labelCol="breakdown_soon", metricName="weightedRecall")

    auc = eval_auc.evaluate(preds)
    precision = eval_prec.evaluate(preds)
    recall = eval_rec.evaluate(preds)

    print("\n=================== MODEL EVALUATION METRICS ===================")
    print(f" Area Under ROC (AUC) : {auc:.4f}  (Measures overall heat fault separation capability)")
    print(f" Weighted Precision   : {precision:.4f}  (Minimizes false alarms sending technicians unnecessarily)")
    print(f" Weighted Recall      : {recall:.4f}  (Crucial: Prevents missing impending machine breakdowns)")
    print("================================================================\n")

    # Export Model Artifact
    model_output_path = "hdfs://localhost:9000/data/machines/models/lr_heat_v1"
    try:
        model.write().overwrite().save(model_output_path)
        print(f"[+] Model successfully saved to HDFS: {model_output_path}")
    except Exception as e:
        local_model_path = "./lr_heat_v1"
        model.write().overwrite().save(local_model_path)
        print(f"[+] Model saved to local directory: {local_model_path} ({e})")

    spark.stop()

if __name__ == "__main__":
    train_heat_failure_model()
