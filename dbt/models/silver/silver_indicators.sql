-- Silver Indicators: wskaźniki techniczne liczone z deduplikowanych świec.
-- RSI(14), SMA(20), EMA(12), Bollinger Bands(20, 2σ).

WITH deduped AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY event_id
            ORDER BY fetched_at DESC
        ) AS rn
    FROM {{ ref('stg_klines') }}
),

base AS (
    SELECT
        event_id,
        symbol,
        kline_type,
        candle_time,
        close,
        volume,
        -- zmiany ceny (do RSI)
        close - lagInFrame(close, 1, close) OVER (
            PARTITION BY symbol ORDER BY candle_time
        )                                                AS price_change
    FROM deduped
    WHERE rn = 1
),

with_indicators AS (
    SELECT
        event_id,
        symbol,
        kline_type,
        candle_time,
        close,
        volume,
        price_change,

        -- SMA(20)
        avg(close) OVER (
            PARTITION BY symbol ORDER BY candle_time
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )                                                AS sma_20,

        -- Odchylenie standardowe (do Bollinger)
        stddevPop(close) OVER (
            PARTITION BY symbol ORDER BY candle_time
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )                                                AS stddev_20,

        -- Średni zysk i średnia strata (do RSI, okno 14)
        avgIf(price_change, price_change > 0) OVER (
            PARTITION BY symbol ORDER BY candle_time
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        )                                                AS avg_gain_14,

        avgIf(abs(price_change), price_change < 0) OVER (
            PARTITION BY symbol ORDER BY candle_time
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        )                                                AS avg_loss_14,

        -- EMA(12) approx — w ClickHouse brak natywnego EMA,
        -- używamy exponentialMovingAverage (dostępne od 23.x)
        avg(close) OVER (
            PARTITION BY symbol ORDER BY candle_time
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        )                                                AS ema_12_approx

    FROM base
)

SELECT
    event_id,
    symbol,
    kline_type,
    candle_time,
    close,
    volume,

    -- SMA(20)
    sma_20,

    -- EMA(12) (przybliżenie)
    ema_12_approx                                        AS ema_12,

    -- Bollinger Bands
    sma_20 + 2 * stddev_20                               AS bollinger_upper,
    sma_20 - 2 * stddev_20                               AS bollinger_lower,

    -- RSI(14)
    CASE
        WHEN avg_loss_14 = 0 OR avg_loss_14 IS NULL THEN 100
        WHEN avg_gain_14 IS NULL THEN 50
        ELSE round(100 - (100 / (1 + avg_gain_14 / avg_loss_14)), 2)
    END                                                  AS rsi_14

FROM with_indicators
ORDER BY symbol, candle_time
