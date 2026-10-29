# Architektura platformy danych: Kafka → ClickHouse → dbt → AI → Grafana

Dokument opisuje, **jak przepływają dane** i **co działa na której maszynie**.
Założenia: 5 maszyn wirtualnych, każda usługa to jeden kontener (Docker Compose).
Bezpieczeństwo danych zapewniają **backupy** oraz **14-dniowa retencja w Kafce**.

Pipeline jest **nieliniowy** — po warstwie bronze dane rozgałęziają się na niezależne
ścieżki (ceny, wolumeny, wskaźniki techniczne), które łączą się dopiero w warstwie ML.
Airflow pilnuje zależności i odpala równoległe ścieżki jednocześnie.

---

## 1. Obraz ogólny

```
                           ŹRÓDŁA
                    ┌─────────────────────┐
                    │  AllTick API         │
                    │  (REST mini-batch)   │
                    └─────────┬───────────┘
                              │
  vm1  INGEST       ┌────────▼────────┐
                    │  Producer (Py)  │  co 5 min → Kafka
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  KAFKA (KRaft)  │  topik: raw.alltick
                    │  retencja 14 d  │
                    └────────┬────────┘
                             │ Kafka Engine
                             ▼
  vm2  BAZA         ┌────────────────────────────────────────┐
                    │            CLICKHOUSE                   │
                    │                                         │
                    │  ┌─────────────────────────────────┐   │
                    │  │          BRONZE                  │   │
                    │  │  bronze.raw_klines (append-only) │   │
                    │  └──────────────┬──────────────────┘   │
                    │        ┌────────┼────────┐              │
                    │        ▼        ▼        ▼              │
                    │  ┌─────────┐┌────────┐┌───────────┐    │
                    │  │ SILVER  ││SILVER  ││ SILVER    │    │
                    │  │ prices  ││volumes ││indicators │    │
                    │  └────┬────┘└───┬────┘└─────┬─────┘    │
                    │       ▼         ▼           ▼           │
                    │  ┌─────────┐┌────────┐┌───────────┐    │
                    │  │ GOLD    ││ GOLD   ││ GOLD      │    │
                    │  │ ohlc    ││ vwap   ││ technical │    │
                    │  └────┬────┘└───┬────┘└─────┬─────┘    │
                    │       │         │           │           │
                    │       └─────┬───┘           │           │
                    │             ▼               ▼           │
                    │  ┌──────────────┐  ┌──────────────┐    │
                    │  │ ml_predictions│  │ ml_anomalies │    │
                    │  └──────────────┘  └──────────────┘    │
                    └────────────────────────────────────────┘
                          ▲               ▲
          dbt (SQL)       │   AI (Python) │
                          │               │
  vm3  ORKIESTRACJA  ┌────┴───────────────┴────┐
                     │ AIRFLOW (LocalExecutor)  │
                     │ + Postgres (metadata)    │
                     │ + dbt-clickhouse         │
                     └──────────────────────────┘

  vm4  AI            ┌─────────────────────────┐
                     │ Serwis ML (Python)       │
                     │ • predykcja cen          │
                     │   (XGBoost / Prophet)    │
                     │ • anomaly detection      │
                     │   (Isolation Forest)     │
                     │ wywoływany przez Airflow │
                     └─────────────────────────┘

  vm5  WIZUALIZACJA  ┌─────────────────────────┐
                     │ Grafana                  │
                     │ • dashboardy cen         │
                     │ • predykcje vs realność  │
                     │ • alerty anomalii        │
                     │ + backup (rsync z vm2)   │
                     └─────────────────────────┘
```

---

## 2. Nieliniowy DAG — przepływ danych

Pipeline **nie jest linią** — po bronze rozgałęzia się na 3 niezależne ścieżki,
które Airflow odpala równolegle. Ścieżki schodzą się dopiero przy ML.

```
                    bronze_check
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
        silver_prices  silver_vol  silver_indicators     ← 3 równoległe
              │          │          │
              ▼          ▼          ▼
        gold_ohlc    gold_vwap   gold_technical          ← 3 równoległe
              │          │          │
              └────┬─────┘          │
                   ▼                ▼
             ml_predict ◄──── ml_anomaly                 ← cross-dependency
                   │                │
                   └────────┬───────┘
                            ▼
                    notify_grafana (odśwież)
```

### Zależności w Airflow (co na co czeka)

| Task | Czeka na | Dlaczego |
|---|---|---|
| `silver_prices` | `bronze_check` | dane muszą być świeże |
| `silver_volumes` | `bronze_check` | j.w. |
| `silver_indicators` | `bronze_check` | j.w. |
| `gold_ohlc` | `silver_prices` | agregaty z czystych cen |
| `gold_vwap` | `silver_volumes` | VWAP potrzebuje wolumenów |
| `gold_technical` | `silver_indicators` | RSI, MA, Bollinger z oczyszczonych danych |
| `ml_anomaly` | `gold_technical` | szuka anomalii we wskaźnikach |
| `ml_predict` | `gold_ohlc` + `gold_vwap` + `gold_technical` | predykcja potrzebuje wszystkiego |
| `backup_daily` | *(niezależny DAG)* | leci osobno, raz na dobę |

---

## 3. Warstwy medalionu — co jest w każdej tabeli

### Bronze (surowe, append-only)

| Tabela | Źródło | Opis |
|---|---|---|
| `bronze.raw_klines` | Kafka `raw.alltick` | Surowy JSON z API, zero zmian. Deduplikacja dopiero w silver. |

### Silver (oczyszczone, z typami)

| Tabela | Zależy od | Opis |
|---|---|---|
| `silver.prices` | `bronze.raw_klines` | OHLC sparsowane, typy Decimal, deduplikacja po `event_id` (najnowszy `fetched_at`) |
| `silver.volumes` | `bronze.raw_klines` | Wolumen i obrót (turnover), oczyszczone, z timestampem UTC |
| `silver.indicators` | `bronze.raw_klines` | Wskaźniki techniczne liczone z surowych danych: RSI(14), SMA(20), EMA(12), Bollinger Bands |

### Gold (agregaty, gotowe do użycia)

| Tabela | Zależy od | Opis |
|---|---|---|
| `gold.ohlc_1h` | `silver.prices` | Świece 1h zagregowane z 5-min świec, per symbol |
| `gold.vwap` | `silver.volumes` | Volume-Weighted Average Price w oknach 1h / 1d |
| `gold.technical` | `silver.indicators` | Sygnały: RSI > 70 (overbought), RSI < 30 (oversold), cross MA |

### ML (wyniki modeli)

| Tabela | Zależy od | Opis |
|---|---|---|
| `ml.predictions` | `gold.ohlc_1h` + `gold.vwap` + `gold.technical` | Predykcja close_price na następne 1-6 świec (XGBoost) |
| `ml.anomalies` | `gold.technical` | Anomalie wykryte Isolation Forest: nietypowy wolumen, nagły ruch ceny |

---

## 4. Co działa na której maszynie

| VM | Rola | Usługi | Zasoby |
|---|---|---|---|
| **vm1** | Ingest | Kafka (1 broker, KRaft), Kafka UI, Producer (Python) | 2-4 vCPU, 8 GB RAM, 100 GB |
| **vm2** | Baza | ClickHouse (bronze + silver + gold + ml) | 4-8 vCPU, 16 GB RAM, SSD |
| **vm3** | Orkiestracja | Airflow (LocalExecutor) + Postgres + dbt-clickhouse | 4 vCPU, 8 GB RAM |
| **vm4** | AI / ML | Serwis predykcji i anomalii (Python + scikit-learn/XGBoost) | 2-4 vCPU, 8 GB RAM |
| **vm5** | Wizualizacja | Grafana + backup ClickHouse (rsync/cron z vm2) | 2 vCPU, 4 GB RAM |

Komunikacja: TCP po nazwach / IP (`vm1:9092`, `vm2:8123`). Firewall otwiera potrzebne porty.

---

## 5. Orkiestracja — DAG-i w Airflow

### DAG `medallion_and_ml` (co 5 min)

```python
bronze_check = ClickHouseSensor(
    sql="SELECT max(fetched_at) FROM bronze.raw_klines",
    # czeka aż bronze ma dane nowsze niż 5 min temu
)

# --- SILVER: 3 taski równolegle ---
silver_prices     = BashOperator(cmd="dbt run --select silver_prices")
silver_volumes    = BashOperator(cmd="dbt run --select silver_volumes")
silver_indicators = BashOperator(cmd="dbt run --select silver_indicators")

# --- GOLD: 3 taski równolegle ---
gold_ohlc     = BashOperator(cmd="dbt run --select gold_ohlc_1h")
gold_vwap     = BashOperator(cmd="dbt run --select gold_vwap")
gold_tech     = BashOperator(cmd="dbt run --select gold_technical")

# --- ML: wywołanie serwisu na vm4 ---
ml_anomaly = SimpleHttpOperator(endpoint="http://vm4:8000/detect-anomalies")
ml_predict = SimpleHttpOperator(endpoint="http://vm4:8000/predict")

# --- Zależności (nieliniowe!) ---
bronze_check >> [silver_prices, silver_volumes, silver_indicators]

silver_prices     >> gold_ohlc
silver_volumes    >> gold_vwap
silver_indicators >> gold_tech

gold_tech                          >> ml_anomaly
[gold_ohlc, gold_vwap, gold_tech]  >> ml_predict
```

### DAG `backup_daily` (raz na dobę, niezależny)

```python
backup_ch = BashOperator(
    cmd="clickhouse-client -h vm2 -q \"BACKUP DATABASE ... TO Disk('backups', ...)\""
)
rsync_to_vm5 = BashOperator(cmd="rsync -az vm2:/backups/ vm5:/backups/")

backup_ch >> rsync_to_vm5
```

---

## 6. Serwis ML na vm4

Prosty FastAPI serwis z dwoma endpointami, wywoływany przez Airflow:

| Endpoint | Model | Input (z ClickHouse) | Output (do ClickHouse) |
|---|---|---|---|
| `POST /predict` | XGBoost (lub Prophet) | gold.ohlc_1h + gold.vwap + gold.technical (ostatnie N świec) | ml.predictions (predicted_close, horizon, confidence) |
| `POST /detect-anomalies` | Isolation Forest | gold.technical (ostatnie okno) | ml.anomalies (symbol, timestamp, anomaly_score, typ) |

Serwis czyta z ClickHouse (clickhouse-connect), liczy, zapisuje wyniki z powrotem.
Model trenowany offline na danych historycznych, plik `.joblib` ładowany przy starcie.

---

## 7. Backupy

- **Co:** `silver` i `gold` codziennie. `bronze` opcjonalnie (odtwarzalne z Kafki).
- **Jak:** `BACKUP DATABASE` w ClickHouse → lokalny dysk vm2 → `rsync` na vm5.
- **Ile trzymamy:** 7 dziennych kopii, starsze usuwa DAG.
- **Retencja Kafki (14 dni) > odstęp backupów (1 dzień)** — zawsze da się dograć brakujące dane.

---

## 8. Model awarii

| Co padnie | Skutek | Odtworzenie |
|---|---|---|
| ClickHouse (vm2) | brak odczytów i analityki | restart; baza doczytuje z Kafki (do 14 dni) |
| Dysk ClickHouse | utrata bazy | restore z backupu (vm5) + replay z Kafki |
| Kafka (vm1) | stop nowych danych | restart; dane na wolumenie; producer ma retry |
| Airflow (vm3) | stop przeliczeń, bronze nadal rośnie | restart; DAG nadrabia |
| Serwis ML (vm4) | brak nowych predykcji, reszta działa | restart; następny cykl DAG przeliczy |

---

## 9. Kolejność wdrożenia

| Krok | Co | VM | Test |
|---|---|---|---|
| 1 | Docker + porty + `/etc/hosts` na wszystkich | all | `docker ps` z każdej maszyny |
| 2 | Kafka (KRaft) + Kafka UI + topik `raw.alltick` | vm1 | produce/consume z konsoli |
| 3 | ClickHouse + tabela `bronze.raw_klines` (Kafka Engine) | vm2 | `SELECT count()` rośnie |
| 4 | Producer → Kafka | vm1 | dane w bronze po sekundach |
| 5 | Airflow + Postgres + dbt: silver i gold | vm3 | DAG zielony, `dbt test` OK |
| 6 | Serwis ML (FastAPI + XGBoost + Isolation Forest) | vm4 | `curl vm4:8000/predict` zwraca wyniki |
| 7 | Grafana: dashboardy cen + predykcje + anomalie | vm5 | wykresy się rysują |
| 8 | Backup: rsync vm2 → vm5, test restore | vm5 | odtworzenie tabeli z kopii |

---

## 10. Skalowanie w przyszłości

1. **Monitoring:** Prometheus + Grafana alerting (brak świeżych danych w bronze).
2. **Replikacja ClickHouse:** replika na vm5, Keeper, `ReplicatedMergeTree`.
3. **Kafka HA:** dodatkowe brokery, RF=3.
4. **Archiwum:** surowe dane → Parquet w MinIO.
5. **Więcej modeli ML:** sentiment z newsów, korelacje między symbolami.
6. **CeleryExecutor** w Airflow gdy LocalExecutor nie wystarczy.

---

## API

Link do API: https://alltick.co/
