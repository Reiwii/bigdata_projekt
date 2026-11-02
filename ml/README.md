# Serwis ML (vm4)

FastAPI, który Airflow woła po przeliczeniu warstwy gold. Czyta dane z `analytics.gold_*` w ClickHouse,
a wyniki zapisuje do bazy `ml`.

| Endpoint | Co robi | Woła go |
|---|---|---|
| `POST /detect-anomalies` | Isolation Forest per symbol → `ml.anomalies` | DAG, po `gold_technical` |
| `POST /predict` | XGBoost (1 model na horyzont 1..6 świec) → `ml.predictions` | DAG, po wszystkich 3 gold |
| `POST /train` | wymusza trening modelu predykcji | ręcznie |
| `GET /model-info` | metryki aktualnego modelu (MAE vs naiwny, confidence) | ręcznie |
| `GET /health` | status + czy ClickHouse odpowiada | Docker healthcheck |

Body jest opcjonalne, `{}` z DAG-a wystarczy. Opcje: `{"symbols": ["BTCUSDT"], "force_retrain": true}` dla `/predict`
i `{"scoring_window_minutes": 120}` dla `/detect-anomalies`.

Kody: `200` również przy `"status": "insufficient_data"` (świeży start, mało historii), dzięki czemu DAG nie pada.
`503` oznacza, że ClickHouse jest niedostępny, a Airflow ponowi próbę.

## Pliki

| Plik | Zawartość |
|---|---|
| `app.py` | endpointy, decyzja kiedy trenować, zapis wyników |
| `settings.py` | konfiguracja ze zmiennych środowiskowych (patrz `infra/vm4-ml/docker-compose.yml`) |
| `database.py` | zapytania do gold, schemat bazy `ml` (to samo co `infra/vm2-clickhouse/init/02_ml.sql`) |
| `features.py` | cechy: zwroty, zmienność, RSI, odległość od SMA/EMA, pozycja w Bollingerze, wolumen względny, VWAP poprzedniej godziny |
| `forecasting.py` | XGBoost: trening, walidacja, predykcja, zapis `.joblib` |
| `anomaly_detection.py` | Isolation Forest, typ anomalii, grupowanie w zdarzenia |

## Predykcja cen (XGBoost)

- **Cel:** zwrot `close[t+h] / close[t] − 1` dla h = 1..6 świec, czyli 1–6 minut przy świecach 1m. Cena wynikowa
  to `predicted_close = base_close × (1 + zwrot)`. Zwroty mają podobną skalę dla wszystkich par, więc jeden model
  uczy się naraz na BTC, SOL, BNB i XRP.
- **Cechy** są bezwymiarowe. Kontekst godzinowy (`gold_ohlc_1h`, `gold_vwap`) pochodzi z poprzedniej, zamkniętej
  godziny, bo bieżąca godzina zawiera już przyszłe minuty, co dawałoby wyciek danych.
- **Trening:** ostatnie 7 dni, ponownie co 60 min. Model jest zapisywany do wolumenu `/models/price_forecast.joblib`
  i wczytywany przy starcie.
- **Walidacja:** ostatnie 20% danych w podziale czasowym, z odstępem równym horyzontowi. Model jest porównywany
  z naiwnym „cena się nie zmieni”.
  `confidence = 1 − MAE_modelu / MAE_naiwnego`, przycięte do [0, 1].
  **Dla minutowych świec krypto wartość bliska 0 jest normalna**: rynek w tak krótkim horyzoncie to niemal błądzenie
  losowe. To uczciwa metryka, a nie błąd.
- Predykcja jest liczona z ostatniej zamkniętej świecy. Otwarta świeca jest pomijana, bo jej close jeszcze się zmienia.

## Anomalie (Isolation Forest)

- Osobny model dla każdego symbolu, uczony na 3 dniach. Ocenia ostatnie 60 minut, więc okna kolejnych przebiegów
  się zakładają.
- Cechy opisują pojedynczą świecę: zwrot, wolumen względem średniej z 60 min, pozycja w Bollingerze,
  RSI − 50, odległość od SMA20.
- `anomaly_score` to miara z pracy Liu i in. (2008), przyjmująca wartości z (0, 1]. Wartość ok. 0.5 to typowa świeca,
  a im bliżej 1, tym bardziej nietypowa. Flagę daje `contamination = 1%`.
- `anomaly_type` wskazuje cechę o największym odporny z-score: `price_spike` / `price_drop`, `volume_spike` /
  `volume_drop`, `bollinger_breakout_up` / `_down`, `rsi_overbought` / `rsi_oversold`, `trend_deviation_up` / `_down`.
- **Jedno zdarzenie = jeden wiersz.** Po skoku ceny przez kolejne ~15 świec cena odstaje od SMA20, więc sąsiednie
  oznaczone świece (przerwa ≤ 2) są łączone. Zapisywana jest świeca szczytowa oraz `event_start_time`,
  `event_end_time` i `event_candle_count`.

## Tabele (baza `ml`)

Tabele są typu ReplacingMergeTree, więc w zapytaniach należy używać `FINAL`.

- `ml.predictions`: klucz `(symbol, base_candle_time, horizon)`
- `ml.anomalies`: klucz `(symbol, event_start_time)`
- `ml.model_runs`: log każdego treningu i przebiegu, z metrykami w `metrics_json`

## Zapytania do Grafany

Predykcja vs rzeczywistość:

```sql
SELECT
    prediction.target_candle_time AS time,
    prediction.predicted_close    AS predicted,
    actual.close                  AS actual
FROM ml.predictions AS prediction FINAL
LEFT JOIN analytics.silver_prices AS actual
    ON actual.symbol = prediction.symbol AND actual.candle_time = prediction.target_candle_time
WHERE prediction.symbol = 'BTCUSDT'
  AND prediction.horizon = 1
  AND $__timeFilter(prediction.target_candle_time)
ORDER BY time
```

Błąd predykcji per horyzont (tylko tam, gdzie rzeczywista cena jest już znana):

```sql
SELECT
    prediction.horizon,
    avg(abs(prediction.predicted_close - actual.close) / actual.close) AS mean_absolute_percentage_error
FROM ml.predictions AS prediction FINAL
INNER JOIN analytics.silver_prices AS actual
    ON actual.symbol = prediction.symbol AND actual.candle_time = prediction.target_candle_time
GROUP BY prediction.horizon
ORDER BY prediction.horizon
```

Lista anomalii:

```sql
SELECT event_start_time, symbol, anomaly_type, round(anomaly_score, 3) AS score,
       event_candle_count AS candles, close, volume, rsi_14
FROM ml.anomalies FINAL
WHERE $__timeFilter(event_start_time)
ORDER BY event_start_time DESC
```

## Lokalnie

```bash
cd ml
pip install -r requirements.txt
CLICKHOUSE_HOST=vm2 MODEL_DIRECTORY=./models uvicorn app:app --port 8000
```
