-- Gold VWAP: Volume-Weighted Average Price na interwale godzinowym.
-- Łączy silver_prices (ceny) z silver_volumes (wolumeny).
-- VWAP = Σ(typical_price × volume) / Σ(volume)

WITH price_volume AS (
    SELECT
        p.symbol,
        toStartOfHour(p.candle_time)                     AS hour,
        p.candle_time,
        (p.high + p.low + p.close) / 3                   AS typical_price,
        v.volume,
        v.turnover
    FROM {{ ref('silver_prices') }}  AS p
    INNER JOIN {{ ref('silver_volumes') }} AS v
        ON p.event_id = v.event_id
)

SELECT
    symbol,
    hour,
    CASE
        WHEN sum(volume) > 0
        THEN sum(typical_price * volume) / sum(volume)
        ELSE 0
    END                                                  AS vwap,
    sum(volume)                                          AS total_volume,
    sum(turnover)                                        AS total_turnover,
    count()                                              AS trade_count
FROM price_volume
GROUP BY symbol, hour
ORDER BY symbol, hour
