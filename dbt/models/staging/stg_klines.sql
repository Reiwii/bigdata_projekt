-- Staging: parsuje surowy JSON z bronze, nadaje typy.
-- To jest VIEW — nie kopiuje danych, tylko udostępnia je w czytelnej formie.
--
-- Źródło to Binance klines (crypto). Pola payload:
--   timestamp   = open_time Unix (sekundy)
--   open/high/low/close_price = string z 8 miejscami po przecinku
--   volume      = base asset volume (ułamkowy, np. "0.00541000")
--   turnover    = quote asset volume (iloczyn ceny i wolumenu w USDT)
--
-- Wszystkie wartości liczbowe parsujemy jako Float64:
--   * ceny krypto mają do 8 miejsc po przecinku i mieszczą się w Float64 bez utraty precyzji,
--   * volume jest ułamkowy (toUInt64 jak przy akcjach by się wywalił),
--   * downstream (silver_volumes, silver_indicators, gold_vwap) wykonuje dzielenia
--     i mnożenia, które z Float64 działają prościej niż z Decimal (brak overflow
--     przy mnożeniu dwóch dużych Decimali, brak kłopotów ze skalą wyniku).

SELECT
    event_id,
    code                                                     AS symbol,
    kline_type,
    fetched_at,
    _inserted_at,

    -- parsowanie payload JSON → kolumny z typami
    toDateTime(
        toInt64(JSONExtractString(payload, 'timestamp'))
    )                                                        AS candle_time,
    toFloat64(JSONExtractString(payload, 'open_price'))      AS open,
    toFloat64(JSONExtractString(payload, 'high_price'))      AS high,
    toFloat64(JSONExtractString(payload, 'low_price'))       AS low,
    toFloat64(JSONExtractString(payload, 'close_price'))     AS close,
    toFloat64(JSONExtractString(payload, 'volume'))          AS volume,
    toFloat64(JSONExtractString(payload, 'turnover'))        AS turnover

FROM bronze.raw_klines
