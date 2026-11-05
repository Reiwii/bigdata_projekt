-- Silver Prices: oczyszczone świece OHLC, zdeduplikowane.
-- Deduplikacja: dla każdego event_id zostaje wiersz z najnowszym fetched_at
-- (bo API zwraca tę samą świecę wielokrotnie, finalna wersja ma najnowszy fetch).
--
-- Incremental: delete+insert po event_id, z 2h lookback na spóźnione fetche z Binance.
-- Full refresh robimy przez `dbt run --full-refresh` przy zmianach schematu albo backfillu.

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_id'
) }}

SELECT
    event_id,
    symbol,
    kline_type,
    candle_time,
    open,
    high,
    low,
    close,
    fetched_at
FROM (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY event_id
            ORDER BY fetched_at DESC
        ) AS rn
    FROM {{ ref('stg_klines') }}
    {% if is_incremental() %}
    -- Tylko ostatnie 2h, ale pełny dedup w tym oknie:
    -- late fetch dla świecy z ostatnich 2h zostanie wychwycony i nadpisze starszą wersję.
    WHERE candle_time >= (SELECT max(candle_time) - INTERVAL 2 HOUR FROM {{ this }})
    {% endif %}
)
WHERE rn = 1
