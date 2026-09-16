"""
SpinWatch - Kafka Telemetry Consumer
Reads from Kafka topic 'machine-readings' (Consumer Group: 'maintenance-tracker').
Updates operational database MySQL 'laundry_ops' (tables: machine_status, readings_log).
Uses explicit manual offset commits for strictly guaranteed at-least-once delivery.
"""

import json
import logging
import datetime

try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SpinWatchConsumer")

MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "",
    "database": "laundry_ops"
}

def process_messages(bootstrap_servers="localhost:9092", topic="machine-readings", group_id="maintenance-tracker"):
    if not KAFKA_AVAILABLE:
        logger.error("kafka-python not installed. Please run `pip install kafka-python mysql-connector-python`.")
        return

    logger.info(f"Connecting to Kafka topic '{topic}' with group_id '{group_id}'...")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers.split(","),
        group_id=group_id,
        enable_auto_commit=False, # Manual offset commit -> explicit at-least-once guarantee
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest"
    )

    db_conn = None
    if MYSQL_AVAILABLE:
        try:
            db_conn = mysql.connector.connect(**MYSQL_CONFIG)
            logger.info("Connected successfully to MySQL 'laundry_ops'")
        except Exception as e:
            logger.warning(f"Could not connect to MySQL: {e}. Operational DB writes skipped.")

    try:
        for msg in consumer:
            reading = msg.value
            mid = reading.get("machine_id")
            branch = reading.get("branch")
            temp = float(reading.get("cycle_temperature", 0.0))
            txn_ts = reading.get("timestamp")
            
            # Re-evaluate status strictly based on heat: cycle_temperature > 70.0°C
            status = "ALERT" if temp > 70.0 else "NORMAL"
            breakdown_soon = 1 if temp > 68.0 else 0
            reading_id = reading.get("reading_id")

            logger.info(f"Received Telemetry | Machine: {mid} | Branch: {branch} | Heat: {temp}°C | Status: {status}")

            # Continuously append raw readings into HDFS historical directory
            try:
                today_str = datetime.datetime.now().strftime("%Y-%m-%d")
                raw_dir = os.path.join(os.getcwd(), "data", "machines", "raw", f"dt={today_str}")
                os.makedirs(raw_dir, exist_ok=True)
                json_append_path = os.path.join(raw_dir, "stream_history.json")
                with open(json_append_path, "a") as f:
                    f.write(json.dumps(reading) + "\n")
            except Exception as e:
                logger.debug(f"HDFS append log exception: {e}")

            if db_conn and db_conn.is_connected():
                cursor = db_conn.cursor()
                now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

                # 1. Update current operational state per washer
                upsert_status_sql = """
                    REPLACE INTO machine_status 
                    (machine_id, branch, cycle_temperature, status, last_updated)
                    VALUES (%s, %s, %s, %s, %s)
                """
                cursor.execute(upsert_status_sql, (mid, branch, temp, status, now_str))

                # 2. Append to full raw readings log
                insert_log_sql = """
                    INSERT INTO readings_log
                    (reading_id, machine_id, branch, cycle_temperature, txn_timestamp, status, breakdown_soon)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE cycle_temperature=VALUES(cycle_temperature), status=VALUES(status)
                """
                cursor.execute(insert_log_sql, (reading_id, mid, branch, temp, txn_ts, status, breakdown_soon))

                db_conn.commit()
                cursor.close()

            # Manual offset commit strictly AFTER successful DB write
            consumer.commit()
            logger.debug(f"Kafka offset committed for partition {msg.partition} offset {msg.offset}")

    except KeyboardInterrupt:
        logger.info("Consumer loop interrupted by user.")
    finally:
        if db_conn and db_conn.is_connected():
            db_conn.close()
        consumer.close()
        logger.info("Consumer shutdown complete.")

if __name__ == "__main__":
    process_messages()
