# Projekt ETL Koszela — Big Data Pipeline

## Co to jest

Studencki projekt Big Data: pipeline giełdowy na 5 maszynach wirtualnych.
Dane z Binance Spot API (świece OHLC kryptowalut) płyną przez Kafkę do ClickHouse (architektura medalionowa),
są przetwarzane przez dbt, karmią modele ML (predykcja cen + anomaly detection), a wyniki
wyświetlają się na dashboardach w Grafanie.

## Stos technologiczny

| Warstwa         | Technologia                           | VM  |
|-----------------|---------------------------------------|-----|
| Ingest          | Python producer + Kafka (KRaft)       | vm1 |
| Baza danych     | ClickHouse (bronze → silver → gold)   | vm2 |
| Orkiestracja    | Airflow (LocalExecutor) + dbt         | vm3 |
| AI / ML         | FastAPI + XGBoost + Isolation Forest  | vm4 |
| Wizualizacja    | Grafana + backup (rsync)              | vm5 |

## Kluczowe zasady architektury

1. **Pipeline jest NIELINIOWY** — po bronze dane rozgałęziają się na 3 równoległe
   ścieżki silver (prices, volumes, indicators), potem 3 gold, potem ML.
   Airflow zarządza zależnościami (fan-out / fan-in).

2. **Architektura medalionowa:**
   - Bronze = surowe JSON z API, append-only, zero zmian
   - Silver = oczyszczone, z typami, zdeduplikowane (3 tabele równoległe)
   - Gold = agregaty gotowe pod dashboardy i ML (3 tabele równoległe)
   - ML = wyniki modeli (predictions + anomalies)

3. **Jedno repo, 5 VM** — całe repo klonowane na każdą VM, ale każda odpala
   tylko swój `docker compose` z katalogu `infra/vmX-*/`.

4. **Źródło danych:** Binance Spot API, endpoint `/api/v3/klines`
   na publicznym URL `https://data-api.binance.vision` (bez klucza API).
   Limit: 6000 wagi/min na IP; nasz polling (4 symbole × 1 req co 60 s) kosztuje
   ~8 wagi/min, czyli ~700× poniżej limitu.
   Symbole: BTCUSDT, SOLUSDT, BNBUSDT, XRPUSDT (rozszerzalne).
   Interwał: 1-minutowe świece; backfill 1000 świec/symbol przy starcie (~16 h historii).

## Struktura repozytorium

```
bigdata_projekt/
├── CLAUDE.md                       # TEN PLIK — kontekst dla AI
├── architektura.md                 # Pełna architektura z diagramami
├── HighLevel_Solution_Design.md    # Dokumentacja biznesowa (nietechniczna)
├── pyproject.toml                  # Zależności Python (uv)
│
├── infra/                          # Docker Compose — PER VM
│   ├── vm1-ingest/
│   │   └── docker-compose.yml      # ✅ Kafka + Kafka UI + Producer
│   ├── vm2-clickhouse/
│   │   ├── docker-compose.yml      # ✅ ClickHouse
│   │   ├── config/kafka.xml        # ✅ konfiguracja konsumenta Kafki
│   │   ├── init/01_bronze.sql      # ✅ tabele bronze (auto przy starcie)
│   │   └── init/02_ml.sql          # ✅ ml.predictions / ml.anomalies / ml.model_runs
│   ├── vm3-orchestration/
│   │   └── docker-compose.yml      # ✅ Airflow + Postgres + dbt
│   ├── vm4-ml/
│   │   └── docker-compose.yml      # ✅ FastAPI ML serwis
│   └── vm5-grafana/
│       └── docker-compose.yml      # ✅ Grafana + backup cron
│
├── producer/                       # ✅ Kod producera Binance (używany przez vm1)
│   ├── producer.py                 #   KafkaSink + FileSink (ABC pattern)
│   ├── Dockerfile
│   ├── .env                        #   (gitignored)
│   └── .env.example
│
├── dbt/                            # ✅ Modele dbt (używany przez vm3)
│   ├── dbt_project.yml             #   config: materialized per layer
│   ├── profiles.yml                #   połączenie do ClickHouse (vm2:8123)
│   └── models/
│       ├── staging/
│       │   ├── stg_klines.sql      # ✅ parsuje JSON z bronze.raw_klines
│       │   └── stg_klines.yml      # ✅ dokumentacja kolumn
│       ├── silver/
│       │   ├── silver_prices.sql   # ✅ deduplikowane OHLC
│       │   ├── silver_volumes.sql  # ✅ wolumen + avg_trade_price
│       │   ├── silver_indicators.sql # ✅ RSI(14), SMA(20), EMA(12), Bollinger
│       │   └── schema.yml          # ✅ dokumentacja kolumn
│       └── gold/
│           ├── gold_ohlc_1h.sql    # ✅ agregowane świece 1h
│           ├── gold_vwap.sql       # ✅ VWAP (łączy prices + volumes)
│           ├── gold_technical.sql  # ✅ sygnały: buy/sell/hold
│           └── schema.yml          # ✅ dokumentacja kolumn
│
├── airflow/dags/                   # ✅ DAG-i Airflow (używany przez vm3)
│   └── medallion_and_ml.py         # ✅ nieliniowy fan-out/fan-in, co 15 min
│
├── ml/                             # ✅ Serwis ML (używany przez vm4) — szczegóły: ml/README.md
│   ├── app.py                      # ✅ FastAPI: /predict, /detect-anomalies, /train, /model-info, /health
│   ├── features.py                 # ✅ cechy z gold (zwroty, RSI, SMA/EMA, Bollinger, wolumen, VWAP 1h)
│   ├── forecasting.py              # ✅ XGBoost: 1 model na horyzont 1..6 świec, .joblib w /models
│   ├── anomaly_detection.py        # ✅ Isolation Forest per symbol, zdarzenia anomalii
│   ├── database.py / settings.py   # ✅ ClickHouse I/O, schemat ml, konfiguracja z env
│   └── Dockerfile                  # ✅
│
├── grafana/                        # Dashboardy (używany przez vm5)
│   ├── provisioning/               # 🔲 datasource + dashboard provisioning
│   └── dashboards/                 # 🔲 JSON dashboards
│
└── data/                           # Lokalne dane testowe (gitignored)
```

## DAG — graf zależności (Airflow)

```
                bronze_check
                     │
          ┌──────────┼──────────┐
          ▼          ▼          ▼
    silver_prices  silver_vol  silver_indicators     ← równolegle
          │          │          │
          ▼          ▼          ▼
    gold_ohlc    gold_vwap   gold_technical          ← równolegle
          │          │          │
          └────┬─────┘          │
               ▼                ▼
         ml_predict ◄──── ml_anomaly                 ← cross-dependency
```

ml_predict czeka na WSZYSTKIE trzy gold. ml_anomaly czeka tylko na gold_technical.
gold_vwap czeka na silver_prices + silver_volumes (łączy ceny z wolumenami).

## Tabele w ClickHouse

### Bronze (auto z Kafka Engine)
| Tabela | Engine | Opis |
|---|---|---|
| `bronze.raw_klines_queue` | Kafka | czyta z topiku `raw.binance` |
| `bronze.raw_klines` | MergeTree | surowe JSON, append-only |
| `bronze.raw_klines_mv` | Materialized View | auto-przelew queue → tabela |

### Silver (dbt, materialized: table)
| Tabela | Zależy od | Opis |
|---|---|---|
| `silver_prices` | `stg_klines` | OHLC zdeduplikowane (najnowszy fetch per event_id) |
| `silver_volumes` | `stg_klines` | wolumen, obrót, avg_trade_price |
| `silver_indicators` | `stg_klines` | RSI(14), SMA(20), EMA(12), Bollinger Bands |

### Gold (dbt, materialized: table)
| Tabela | Zależy od | Opis |
|---|---|---|
| `gold_ohlc_1h` | `silver_prices` | świece 1h (first open, max high, min low, last close) |
| `gold_vwap` | `silver_prices` + `silver_volumes` | VWAP = Σ(typical_price × volume) / Σ(volume) |
| `gold_technical` | `silver_indicators` | sygnały: rsi_signal, trend_signal, volatility_signal, composite_signal |

### ML (pisane przez serwis ML z vm4)
| Tabela | Zależy od | Opis |
|---|---|---|
| `ml.predictions` | all 3 gold | predykcja close_price na 1..6 świec (XGBoost), klucz (symbol, base_candle_time, horizon) |
| `ml.anomalies` | `gold_technical` | zdarzenia anomalii (Isolation Forest, score 0-1, typ), klucz (symbol, event_start_time) |
| `ml.model_runs` | — | log treningów i przebiegów z metrykami |

## Co jest gotowe, co do zrobienia

### ✅ Gotowe
- [x] Producer (Python) z KafkaSink + FileSink + Dockerfile
- [x] Docker-compose vm1: Kafka (KRaft) + Kafka UI + Producer + auto-topik
- [x] Docker-compose vm2: ClickHouse + bronze (Kafka Engine + MergeTree + MV)
- [x] Docker-compose vm3: Airflow (LocalExecutor) + Postgres + dbt mount
- [x] Docker-compose vm4: FastAPI ML serwis + wolumen na model
- [x] Docker-compose vm5: Grafana + backup cron
- [x] dbt: staging (stg_klines) + 3× silver + 3× gold z dokumentacją
- [x] Airflow DAG: medallion_and_ml (nieliniowy, co 15 min)
- [x] Architektura (architektura.md) + dokumentacja biznesowa
- [x] **ML:** /predict (XGBoost) i /detect-anomalies (Isolation Forest) — ml/README.md
- [x] **ML:** tabele ml.predictions, ml.anomalies, ml.model_runs (init/02_ml.sql + auto przy starcie serwisu)

### 🔲 Do zrobienia
- [ ] **Grafana:** Provisioning datasource (ClickHouse)
- [ ] **Grafana:** Dashboardy: ceny live, predykcje vs real, alerty anomalii
- [ ] **Testy:** dbt test, integracja end-to-end
- [ ] **Deploy:** /etc/hosts na wszystkich VM, firewall porty

## Konwencje

- Każda usługa = kontener Docker, docker-compose per VM w `infra/vmX-*/`
- Komunikacja: TCP po nazwie (`vm1:9092`, `vm2:8123`, `vm4:8000`)
- Python: uv, 3.14+ (kontenery: 3.12)
- Nazwy tabel: `{warstwa}.{nazwa}` np. `bronze.raw_klines`, `gold.ohlc_1h`
- Kafkowe topiki: `raw.{źródło}` np. `raw.binance`
- dbt tagi: `tag:staging`, `tag:silver`, `tag:gold`
- Schedule: DAG co 15 min, backup co 24h

## Uruchamianie per VM

```bash
# Na każdej VM — klonujesz repo RAZ:
git clone <repo-url> && cd bigdata_projekt

# Potem odpalasz TYLKO swoją część:
# vm1:
cd infra/vm1-ingest && docker compose up -d

# vm2:
cd infra/vm2-clickhouse && docker compose up -d

# vm3:
cd infra/vm3-orchestration && docker compose up -d

# vm4:
cd infra/vm4-ml && docker compose up -d

# vm5:
cd infra/vm5-grafana && docker compose up -d
```

## Przydatne komendy

```bash
# Sprawdź topiki Kafki (vm1)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Podejrzyj dane w topiku
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic raw.binance --from-beginning --max-messages 5

# Producer lokalnie (bez Kafki, do pliku)
cd producer && SINK_TYPE=file uv run python producer.py

# Sprawdź bronze w ClickHouse (vm2)
docker exec clickhouse clickhouse-client -q "SELECT count() FROM bronze.raw_klines"

# dbt run silver (vm3, w kontenerze airflow)
docker exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt run --select tag:silver"

# dbt run all
docker exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt run"

# Airflow UI (vm3)
# http://vm3:8082  login: airflow / airflow

# Test ML serwis (vm4)
curl http://vm4:8000/health

# Grafana (vm5)
# http://vm5:3000  login: admin / admin
```
