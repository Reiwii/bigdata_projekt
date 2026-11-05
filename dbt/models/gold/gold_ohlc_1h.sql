-- Gold OHLC 1H: świece zagregowane do interwału 1-godzinnego.
-- Źródło: silver_prices (deduplikowane OHLC).
-- Agregacja: first(open), max(high), min(low), last(close) w oknie 1h.
--
-- Incremental: delete+insert po (symbol, hour). Przeliczamy BIEŻĄCĄ i POPRZEDNIĄ
-- godzinę — bieżąca jeszcze trwa, poprzednia może dostać spóźnione świece.

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['symbol', 'hour']
) }}

SELECT
    symbol,
    toStartOfHour(candle_time)                           AS hour,
    argMin(open, candle_time)                            AS open,
    max(high)                                            AS high,
    min(low)                                             AS low,
    argMax(close, candle_time)                           AS close,
    count()                                              AS candle_count,
    min(candle_time)                                     AS first_candle,
    max(candle_time)                                     AS last_candle
FROM {{ ref('silver_prices') }}
{% if is_incremental() %}
WHERE candle_time >= (SELECT max(hour) - INTERVAL 1 HOUR FROM {{ this }})
{% endif %}
GROUP BY symbol, toStartOfHour(candle_time)
