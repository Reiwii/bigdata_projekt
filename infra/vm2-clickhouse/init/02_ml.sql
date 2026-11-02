-- ML: wyniki modeli z serwisu na vm4 (predykcja cen + anomalie).
-- Ten sam schemat tworzy serwis ML przy starcie (ml/database.py), bo skrypty z init/
-- wykonują się tylko przy PIERWSZYM starcie ClickHouse z pustym wolumenem.
--
-- ReplacingMergeTree: ponowne przeliczenie tej samej świecy nadpisuje wiersz
-- (w zapytaniach Grafany używaj FINAL, żeby nie widzieć duplikatów przed scaleniem).
--
-- ml.predictions: predykcja ceny zamknięcia świecy target_candle_time
--   (= base_candle_time + horizon świec). Porównanie z rzeczywistością:
--   JOIN analytics.silver_prices ON symbol AND candle_time = target_candle_time.
-- ml.anomalies:   jedno zdarzenie = jeden wiersz (szczytowa świeca candle_time + początek/koniec/długość);
--                 anomaly_score w (0, 1], ~0.5 = typowa świeca. Klucz: (symbol, event_start_time).
-- ml.model_runs:  log każdego treningu / przebiegu z metrykami (JSON).

CREATE DATABASE IF NOT EXISTS ml;

CREATE TABLE IF NOT EXISTS ml.predictions (
    symbol                 LowCardinality(String),
    base_candle_time       DateTime('UTC'),
    target_candle_time     DateTime('UTC'),
    horizon                UInt8,
    base_close             Float64,
    predicted_close        Float64,
    predicted_return       Float64,
    validation_mae_return  Float64,
    confidence             Float32,
    model_version          String,
    predicted_at           DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(predicted_at)
PARTITION BY toYYYYMM(base_candle_time)
ORDER BY (symbol, base_candle_time, horizon);

CREATE TABLE IF NOT EXISTS ml.anomalies (
    symbol                   LowCardinality(String),
    event_start_time         DateTime('UTC'),
    event_end_time           DateTime('UTC'),
    event_candle_count       UInt16,
    candle_time              DateTime('UTC'),
    anomaly_score            Float64,
    anomaly_type             LowCardinality(String),
    dominant_feature         LowCardinality(String),
    dominant_feature_zscore  Float64,
    close                    Float64,
    volume                   Float64,
    rsi_14                   Float64,
    model_version            String,
    detected_at              DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(detected_at)
PARTITION BY toYYYYMM(event_start_time)
ORDER BY (symbol, event_start_time);

CREATE TABLE IF NOT EXISTS ml.model_runs (
    run_at         DateTime64(3, 'UTC'),
    task           LowCardinality(String),
    symbol         LowCardinality(String),
    training_rows  UInt32,
    model_version  String,
    metrics_json   String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(run_at)
ORDER BY (task, run_at);
