"""
Kafka Producer Module for SpinWatch Heat Telemetry
Keyed by machine_id to ensure order guarantee per washing machine on Kafka topic partitions.
"""

import json
import logging

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SpinWatchProducer")

class MachineTelemetryProducer:
    def __init__(self, bootstrap_servers="localhost:9092", topic="machine-readings"):
        self.topic = topic
        self.producer = None
        if KAFKA_AVAILABLE:
            # Fast socket probe so we never block or hang when Kafka is not running
            import socket
            broker_online = False
            for s in bootstrap_servers.split(","):
                try:
                    h, p = s.strip().split(":")
                    with socket.create_connection((h, int(p)), timeout=0.3):
                        broker_online = True
                        break
                except Exception:
                    pass

            if broker_online:
                try:
                    self.producer = KafkaProducer(
                        bootstrap_servers=bootstrap_servers.split(","),
                        key_serializer=lambda k: k.encode("utf-8") if k else None,
                        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                        retries=3,
                        request_timeout_ms=2000,
                        max_block_ms=2000
                    )
                    logger.info(f"KafkaProducer connected to {bootstrap_servers}, topic: {topic}")
                except Exception as e:
                    logger.warning(f"Could not connect KafkaProducer: {e}. Running in standalone fallback mode.")
            else:
                logger.info(f"Kafka broker at {bootstrap_servers} offline. Running in standalone fallback mode.")

    def send_reading(self, reading: dict) -> bool:
        mid = reading.get("machine_id")
        if self.producer:
            try:
                # Keying by machine_id ensures strict per-machine partition ordering
                future = self.producer.send(self.topic, key=mid, value=reading)
                future.get(timeout=2.0)
                logger.debug(f"Published reading for {mid} to Kafka topic {self.topic}")
                return True
            except Exception as e:
                logger.error(f"Error publishing to Kafka: {e}")
                return False
        else:
            logger.info(f"[Fallback Log Only] Reading for {mid}: temp={reading.get('cycle_temperature')}°C, status={reading.get('status')}")
            return True
