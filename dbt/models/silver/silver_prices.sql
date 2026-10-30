-- Silver Prices: oczyszczone świece OHLC, zdeduplikowane.
-- Deduplikacja: dla każdego event_id zostaje wiersz z najnowszym fetched_at
-- (bo API zwraca tę samą świecę wielokrotnie, finalna wersja ma najnowszy fetch).

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
)
WHERE rn = 1
ORDER BY symbol, candle_time
