CREATE DATABASE IF NOT EXISTS btc_analytics;

USE btc_analytics;

CREATE TABLE IF NOT EXISTS transactions
(
    tx_id String,
    timestamp_ms UInt64,
    ingest_time_ms UInt64,
    source LowCardinality(String),
    input_count UInt16,
    output_count UInt16,
    total_input Float64,
    total_output Float64,
    inputs_json String,
    outputs_json String,
    is_late UInt8
)
ENGINE = MergeTree
ORDER BY (timestamp_ms, tx_id);

CREATE TABLE IF NOT EXISTS alerts
(
    alert_id String,
    alert_type LowCardinality(String),
    severity LowCardinality(String),
    timestamp_ms UInt64,
    tx_id String,
    address String,
    details_json String
)
ENGINE = MergeTree
ORDER BY (timestamp_ms, alert_id);

CREATE TABLE IF NOT EXISTS balance_updates
(
    address String,
    timestamp_ms UInt64,
    source LowCardinality(String),
    balance Float64,
    utxo_count UInt32
)
ENGINE = ReplacingMergeTree(timestamp_ms)
ORDER BY (address, timestamp_ms);

CREATE TABLE IF NOT EXISTS metrics
(
    metric_type LowCardinality(String),
    timestamp_ms UInt64,
    source LowCardinality(String),
    tx_id String,
    address String,
    payload_json String
)
ENGINE = MergeTree
ORDER BY (timestamp_ms, metric_type, tx_id);
