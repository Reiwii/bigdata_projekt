-- Bronze: surowe dane z Kafki (append-only, zero zmian)

CREATE DATABASE IF NOT EXISTS bronze;

-- Tabela-silnik Kafka (czyta z topiku, nie trzyma danych)
CREATE TABLE IF NOT EXISTS bronze.raw_klines_queue (
    event_id     String,
    source       String,
    endpoint     String,
    code         String,
    kline_type   UInt8,
    fetched_at   String,
    payload      String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'vm1:9092',
    kafka_topic_list = 'raw.alltick',
    kafka_group_name = 'clickhouse-bronze',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_max_block_size = 65536;

-- Tabela docelowa (MergeTree)
CREATE TABLE IF NOT EXISTS bronze.raw_klines (
    event_id     String,
    source       String,
    endpoint     String,
    code         String,
    kline_type   UInt8,
    fetched_at   DateTime64(3, 'UTC'),
    payload      String,
    _inserted_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (code, kline_type, fetched_at)
PARTITION BY toYYYYMM(fetched_at);

-- Materialized View: automatycznie przelewa z queue do tabeli
CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.raw_klines_mv TO bronze.raw_klines AS
SELECT
    event_id,
    source,
    endpoint,
    code,
    kline_type,
    parseDateTimeBestEffort(fetched_at) AS fetched_at,
    payload
FROM bronze.raw_klines_queue;
