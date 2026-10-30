-- Gold Technical: sygnały tradingowe generowane ze wskaźników.
-- Źródło: silver_indicators (RSI, SMA, EMA, Bollinger).
-- Sygnały: oversold/overbought (RSI), trend (SMA/EMA cross),
-- volatility squeeze/expansion (Bollinger width).

SELECT
    event_id,
    symbol,
    candle_time,
    close,
    rsi_14,
    sma_20,
    ema_12,
    bollinger_upper,
    bollinger_lower,

    -- Sygnał RSI
    CASE
        WHEN rsi_14 < 30 THEN 'oversold'
        WHEN rsi_14 > 70 THEN 'overbought'
        ELSE 'neutral'
    END                                                  AS rsi_signal,

    -- Sygnał trendu: EMA(12) vs SMA(20)
    CASE
        WHEN ema_12 > sma_20 THEN 'bullish'
        WHEN ema_12 < sma_20 THEN 'bearish'
        ELSE 'neutral'
    END                                                  AS trend_signal,

    -- Szerokość Bollingera (zmienność)
    CASE
        WHEN sma_20 > 0
        THEN (bollinger_upper - bollinger_lower) / sma_20
        ELSE 0
    END                                                  AS bollinger_width,

    -- Sygnał zmienności
    CASE
        WHEN sma_20 > 0 AND (bollinger_upper - bollinger_lower) / sma_20 < 0.02
        THEN 'squeeze'
        WHEN sma_20 > 0 AND (bollinger_upper - bollinger_lower) / sma_20 > 0.10
        THEN 'expansion'
        ELSE 'normal'
    END                                                  AS volatility_signal,

    -- Sygnał Bollinger touch
    CASE
        WHEN close >= bollinger_upper THEN 'upper_touch'
        WHEN close <= bollinger_lower THEN 'lower_touch'
        ELSE 'inside'
    END                                                  AS bollinger_position,

    -- Zagregowany sygnał
    CASE
        WHEN rsi_14 < 30 AND ema_12 > sma_20 THEN 'strong_buy'
        WHEN rsi_14 > 70 AND ema_12 < sma_20 THEN 'strong_sell'
        WHEN rsi_14 < 30 THEN 'buy'
        WHEN rsi_14 > 70 THEN 'sell'
        ELSE 'hold'
    END                                                  AS composite_signal

FROM {{ ref('silver_indicators') }}
ORDER BY symbol, candle_time
