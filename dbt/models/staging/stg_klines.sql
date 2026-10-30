-- Staging: parsuje surowy JSON z bronze, nadaje typy.
-- To jest VIEW — nie kopiuje danych, tylko udostępnia je w czytelnej formie.

SELECT
    event_id,
    code                                                    AS symbol,
    kline_type,
    fetched_at,
    _inserted_at,

    -- parsowanie payload JSON → kolumny z typami
    toDateTime(
        toInt64(JSONExtractString(payload, 'timestamp'))
    )                                                       AS candle_time,
    toDecimal64(JSONExtractString(payload, 'open_price'), 6)   AS open,
    toDecimal64(JSONExtractString(payload, 'high_price'), 6)   AS high,
    toDecimal64(JSONExtractString(payload, 'low_price'), 6)    AS low,
    toDecimal64(JSONExtractString(payload, 'close_price'), 6)  AS close,
    toUInt64(JSONExtractString(payload, 'volume'))              AS volume,
    toDecimal64(JSONExtractString(payload, 'turnover'), 2)     AS turnover

FROM bronze.raw_klines
