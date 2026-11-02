"""Dostęp do ClickHouse: odczyt warstwy gold, zapis wyników ML, schemat bazy ml."""

from __future__ import annotations

import logging

import clickhouse_connect
import pandas as pd

from settings import Settings

log = logging.getLogger("ml.database")


def create_client(settings: Settings):
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        connect_timeout=10,
        send_receive_timeout=300,
    )


# ---------------------------------------------------------------------------
# Schemat bazy ml — ten sam co w infra/vm2-clickhouse/init/02_ml.sql.
# Serwis tworzy go sam przy starcie, bo skrypty init ClickHouse uruchamiają się
# tylko przy pierwszym starcie z pustym wolumenem (na działającej bazie by nie zadziałały).
# ---------------------------------------------------------------------------
def ml_schema_statements(ml_database: str) -> list[str]:
    return [
        f"CREATE DATABASE IF NOT EXISTS {ml_database}",
        f"""
        CREATE TABLE IF NOT EXISTS {ml_database}.predictions (
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
        ORDER BY (symbol, base_candle_time, horizon)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {ml_database}.anomalies (
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
        ORDER BY (symbol, event_start_time)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {ml_database}.model_runs (
            run_at         DateTime64(3, 'UTC'),
            task           LowCardinality(String),
            symbol         LowCardinality(String),
            training_rows  UInt32,
            model_version  String,
            metrics_json   String
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(run_at)
        ORDER BY (task, run_at)
        """,
    ]


def ensure_ml_schema(client, settings: Settings) -> None:
    for statement in ml_schema_statements(settings.ml_database):
        client.command(statement)


# ---------------------------------------------------------------------------
# Odczyt
# ---------------------------------------------------------------------------
def _symbol_filter(symbols: list[str] | None, column_name: str = "symbol") -> tuple[str, dict]:
    if not symbols:
        return "", {}
    return f" AND {column_name} IN {{symbols:Array(String)}}", {"symbols": [s.upper() for s in symbols]}


def _to_utc_naive(column: pd.Series) -> pd.Series:
    """Ujednolicamy czasy do naiwnego UTC (bez strefy) — łatwiej je porównywać i łączyć."""
    return pd.to_datetime(column, utc=True).dt.tz_localize(None)


def load_technical_candles(
    client, settings: Settings, window_days: int, symbols: list[str] | None = None
) -> pd.DataFrame:
    """Świece z gold_technical — tylko ZAMKNIĘTE (ostatnia świeca na Binance jest jeszcze otwarta)."""
    symbol_condition, parameters = _symbol_filter(symbols)
    query = f"""
        SELECT
            symbol, candle_time, close, volume,
            rsi_14, sma_20, ema_12, bollinger_upper, bollinger_lower, bollinger_width
        FROM {settings.analytics_database}.gold_technical
        WHERE candle_time >= now() - INTERVAL {int(window_days)} DAY
          AND candle_time + INTERVAL {int(settings.candle_minutes)} MINUTE <= now()
          {symbol_condition}
        ORDER BY symbol, candle_time
    """
    candles = client.query_df(query, parameters=parameters)
    if not candles.empty:
        candles["candle_time"] = _to_utc_naive(candles["candle_time"])
    return candles


def load_hourly_context(
    client, settings: Settings, window_days: int, symbols: list[str] | None = None
) -> pd.DataFrame:
    """Godzinowe OHLC + VWAP (gold_ohlc_1h ⋈ gold_vwap) — kontekst dla predykcji."""
    symbol_condition, parameters = _symbol_filter(symbols, "hourly_ohlc.symbol")
    query = f"""
        SELECT
            hourly_ohlc.symbol  AS symbol,
            hourly_ohlc.hour    AS hour,
            hourly_ohlc.open    AS hour_open,
            hourly_ohlc.high    AS hour_high,
            hourly_ohlc.low     AS hour_low,
            hourly_ohlc.close   AS hour_close,
            hourly_vwap.vwap    AS hour_vwap
        FROM {settings.analytics_database}.gold_ohlc_1h AS hourly_ohlc
        LEFT JOIN {settings.analytics_database}.gold_vwap AS hourly_vwap
            ON hourly_ohlc.symbol = hourly_vwap.symbol AND hourly_ohlc.hour = hourly_vwap.hour
        WHERE hourly_ohlc.hour >= now() - INTERVAL {int(window_days) + 1} DAY
          {symbol_condition}
        ORDER BY symbol, hour
    """
    hourly_context = client.query_df(query, parameters=parameters)
    if not hourly_context.empty:
        hourly_context["hour"] = _to_utc_naive(hourly_context["hour"])
        # LEFT JOIN w ClickHouse wstawia 0 zamiast NULL przy braku dopasowania
        hourly_context.loc[hourly_context["hour_vwap"] <= 0, "hour_vwap"] = float("nan")
    return hourly_context


# ---------------------------------------------------------------------------
# Zapis
# ---------------------------------------------------------------------------
def insert_rows(client, settings: Settings, table_name: str, rows: pd.DataFrame) -> int:
    if rows.empty:
        return 0
    client.insert_df(table=table_name, df=rows, database=settings.ml_database)
    return len(rows)
