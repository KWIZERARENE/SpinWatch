-- SpinWatch Database Schema (MySQL)
-- Operational Storage ONLY (Strictly separate from HDFS Analytical Storage & PySpark Predictions)

CREATE DATABASE IF NOT EXISTS laundry_ops;
USE laundry_ops;

-- 1. Current Operational State per Washer (Updated in real-time by Python Consumer)
CREATE TABLE IF NOT EXISTS machine_status (
    machine_id VARCHAR(20) PRIMARY KEY,
    branch VARCHAR(50) NOT NULL,
    cycle_temperature DECIMAL(5,2) NOT NULL,
    status VARCHAR(10) NOT NULL, -- 'NORMAL' or 'ALERT' (>70.0°C)
    last_updated DATETIME NOT NULL
);

-- 2. Operational Telemetry Readings Log (Landed automatically by Kafka Connect or Consumer)
CREATE TABLE IF NOT EXISTS readings_log (
    reading_id CHAR(36) PRIMARY KEY,
    machine_id VARCHAR(20) NOT NULL,
    branch VARCHAR(50) NOT NULL,
    cycle_temperature DECIMAL(5,2) NOT NULL,
    txn_timestamp DATETIME NOT NULL,
    status VARCHAR(10) NOT NULL,
    breakdown_soon TINYINT DEFAULT 0 -- 1 = Next reading trips heat threshold (>70.0°C)
);

-- NOTE: PySpark Analytics Insights & MLlib Predictions are stored directly in HDFS 
-- (/data/machines/insights/ and /data/machines/predictions/) and NOT in MySQL.
