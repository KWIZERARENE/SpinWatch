-- SpinWatch Database Schema (MySQL)
-- Operational Storage ONLY (Strictly separate from HDFS Analytical Storage & PySpark Predictions)
-- All queries ordered by timestamp DESC so NEWEST records are always returned first.

CREATE DATABASE IF NOT EXISTS laundry_ops;
USE laundry_ops;

-- ============================================================
-- TABLE 1: machine_status
-- One row per washing machine — represents live operational state.
-- Updated via REPLACE INTO (upsert) on every incoming Kafka reading.
-- Ordered by last_updated DESC in all queries — newest activity first.
-- ============================================================
CREATE TABLE IF NOT EXISTS machine_status (
    machine_id        VARCHAR(20)   PRIMARY KEY,                -- Washer unit ID (e.g., WM_0007)
    reading_id        CHAR(36),                                 -- UUID v4 of the latest telemetry reading
    branch            VARCHAR(50)   NOT NULL,                   -- City/branch location (e.g., Kigali)
    cycle_temperature DECIMAL(5,2)  NOT NULL,                   -- Latest temperature reading (°C)
    status            VARCHAR(10)   NOT NULL,                   -- 'NORMAL' or 'ALERT' (>70.0°C)
    breakdown_soon    TINYINT       DEFAULT 0,                  -- 1 = Imminent failure alert, 0 = Normal
    last_updated      DATETIME      NOT NULL,                   -- Timestamp of last update (ORDER BY DESC)
    INDEX idx_last_updated (last_updated),                      -- Fast descending sort for dashboard (newest first)
    INDEX idx_status (status)                                   -- Fast filtering by ALERT / NORMAL status
);

-- ============================================================
-- TABLE 2: readings_log
-- Append-only log of every telemetry reading received from Kafka.
-- Grows continuously as the pipeline runs. Idempotent inserts via
-- ON DUPLICATE KEY UPDATE (handles Kafka at-least-once redeliveries).
-- Ordered by txn_timestamp DESC — newest readings returned first.
-- ============================================================
CREATE TABLE IF NOT EXISTS readings_log (
    reading_id        CHAR(36)      PRIMARY KEY,                -- UUID v4 — globally unique reading ID
    machine_id        VARCHAR(20)   NOT NULL,                   -- Washer unit ID (e.g., WM_0007)
    branch            VARCHAR(50)   NOT NULL,                   -- City/branch (denormalized for query speed)
    cycle_temperature DECIMAL(5,2)  NOT NULL,                   -- Temperature at time of reading (°C)
    txn_timestamp     DATETIME      NOT NULL,                   -- Exact UTC timestamp of the reading
    status            VARCHAR(10)   NOT NULL,                   -- 'NORMAL' or 'ALERT'
    breakdown_soon    TINYINT       DEFAULT 0,                  -- 1 = Predicted imminent failure, 0 = Normal
    INDEX idx_txn_timestamp (txn_timestamp),                    -- Fast ORDER BY txn_timestamp DESC (newest first)
    INDEX idx_machine_id (machine_id),                          -- Fast per-machine filtering & lookup
    INDEX idx_status (status)                                   -- Fast ALERT-only filtering for dashboards
);

-- ============================================================
-- STORAGE SEPARATION NOTE:
-- PySpark Analytics Insights  → HDFS /data/machines/insights/
-- MLlib Failure Predictions   → HDFS /data/machines/predictions/
-- Trained Model Artifact      → HDFS /data/machines/models/lr_heat_v1
--
-- None of the above are written to MySQL. MySQL holds ONLY:
--   • Live operational state   (machine_status)
--   • Rolling readings audit   (readings_log)
-- ============================================================
