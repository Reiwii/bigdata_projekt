-- Silver Volumes: wolumen i obrót, zdeduplikowane.
-- Osobna tabela bo wolumen i obrót mają inne wzorce użycia niż ceny
-- (VWAP, analiza płynności, anomalie wolumenowe).
--
-- Incremental: delete+insert po event_id, z 2h lookback na spóźnione fetche.

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
    volume,
    turnover,
    CASE
        WHEN volume > 0 THEN turnover / volume
        ELSE 0
    END                                          AS avg_trade_price,
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
    WHERE candle_time >= (SELECT max(candle_time) - INTERVAL 2 HOUR FROM {{ this }})
    {% endif %}
)
WHERE rn = 1
