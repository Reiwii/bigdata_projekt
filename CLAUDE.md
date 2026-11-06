# Projekt ETL Koszela — Big Data Pipeline

> Ostatnia aktualizacja dokumentacji: 2026-11-05. Gdy zmieniasz kod, zaktualizuj ten plik.

## Co to jest

Studencki projekt Big Data: pipeline giełdowy na 5 maszynach wirtualnych.
Dane z Binance Spot API (świece OHLC kryptowalut) płyną przez Kafkę do ClickHouse (architektura medalionowa),
są przetwarzane przez dbt, karmią modele ML (predykcja cen + wykrywanie anomalii), a wyniki
wyświetlają się na dashboardach w Grafanie.

## Stos technologiczny

| Warstwa         | Technologia                           | VM  |
|-----------------|---------------------------------------|-----|
| Ingest          | Python producer + Kafka (KRaft)       | vm1 |
| Baza danych     | ClickHouse (bronze → silver → gold)   | vm2 |
| Orkiestracja    | Airflow (LocalExecutor) + dbt         | vm3 |
| AI / ML         | FastAPI + XGBoost + Isolation Forest  | vm4 |
| Wizualizacja    | Grafana + backup (rsync)              | vm5 |

Adresy VM (z `extra_hosts` w docker-compose): vm1 10.7.11.43, vm2 10.7.11.44, vm3 10.7.11.45, vm4 10.7.11.46, vm5 10.7.11.47.

## Kluczowe zasady architektury

1. **Pipeline jest NIELINIOWY** — po bronze dane rozgałęziają się na 3 równoległe
   ścieżki silver (prices, volumes, indicators), potem 3 gold, potem ML.
   Airflow zarządza zależnościami (fan-out / fan-in).

2. **Architektura medalionowa:**
   - Bronze = surowe JSON z API, append-only, zero zmian (baza `bronze`)
   - Silver = oczyszczone, z typami, zdeduplikowane (baza `analytics`)
   - Gold = agregaty gotowe pod dashboardy i ML (baza `analytics`)
   - ML = wyniki modeli (baza `ml`)

3. **Jedno repo, 5 VM** — całe repo klonowane na każdą VM, ale każda odpala
   tylko swój `docker compose` z katalogu `infra/vmX-*/`.

4. **Źródło danych:** Binance Spot API, endpoint `/api/v3/klines`
   na publicznym URL `https://data-api.binance.vision` (bez klucza API).
   Limit: 6000 wagi/min na IP; nasz polling (4 symbole × 1 req co 60 s) kosztuje
   ~8 wagi/min, czyli ~700× poniżej limitu.
   Symbole: BTCUSDT, SOLUSDT, BNBUSDT, XRPUSDT (rozszerzalne).
   Interwał: 1-minutowe świece. Przy starcie producer robi backfill `BACKFILL_LIMIT` świec (domyślnie 1000, ~16 h), potem polling co 60 s (2 ostatnie świece na symbol).

## Struktura repozytorium

```
bigdata_projekt/
├── CLAUDE.md                       # TEN PLIK — kontekst dla AI
├── architektura.md                 # Architektura z diagramami
├── HighLevel_Solution_Design.md    # Dokumentacja biznesowa (nietechniczna)
├── DEPLOYMENT.md                   # Jak uruchomić platformę
├── pyproject.toml                  # Zależności Python (uv)
│
├── infra/                          # Docker Compose — PER VM
│   ├── vm1-ingest/docker-compose.yml       # Kafka + Kafka UI + Producer
│   ├── vm2-clickhouse/                     # ClickHouse
│   │   ├── docker-compose.yml
│   │   ├── config/kafka.xml                # konfiguracja konsumenta Kafki
│   │   └── init/01_bronze.sql, 02_ml.sql   # bronze oraz schemat ml (auto przy PIERWSZYM starcie z pustym wolumenem)
│   ├── vm3-orchestration/docker-compose.yml # Airflow + Postgres + dbt
│   ├── vm4-ml/docker-compose.yml           # FastAPI ML
│   └── vm5-grafana/docker-compose.yml      # Grafana + backup cron
│
├── producer/                       # Kod producera Binance (używany przez vm1)
│   ├── producer.py                 #   KafkaSink + FileSink (ABC pattern)
│   ├── Dockerfile
│   ├── .env                        #   (gitignored)
│   └── .env.example
│
├── dbt/                            # Modele dbt (używany przez vm3)
│   ├── dbt_project.yml
│   ├── profiles.yml                #   ClickHouse vm2:8123, schema: analytics
│   └── models/
│       ├── staging/stg_klines.sql  #   parsuje JSON z bronze.raw_klines (view)
│       ├── silver/                 #   silver_prices, silver_volumes, silver_indicators
│       └── gold/                   #   gold_ohlc_1h, gold_vwap, gold_technical
│
├── airflow/dags/medallion_and_ml.py # DAG nieliniowy fan-out/fan-in, co 15 min
│
├── ml/                             # Serwis ML (vm4) — szczegóły: ml/README.md
│   ├── app.py, features.py, forecasting.py, anomaly_detection.py
│   ├── database.py, settings.py, Dockerfile, requirements.txt
│
├── grafana/
│   ├── provisioning/datasources/clickhouse.yml  # datasource (native, vm2:9000, baza analytics)
│   ├── provisioning/dashboards/dashboards.yml   # ładuje *.json z grafana/dashboards
│   ├── dashboards/                              # JSON-y dashboardów (patrz: do zrobienia)
│   └── export_dashboards.py                     # eksport dashboardów z UI do plików JSON
│
├── Claude outputs/                 # wygenerowane JSON-y dashboardów (do przeniesienia do grafana/dashboards)
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
DAG: schedule `*/15 * * * *`, `retries=2`, `max_active_runs=1`, `catchup=False`. Taski dbt używają `dbt run` (testów dbt w DAG-u jeszcze nie ma).

## Tabele w ClickHouse

### Bronze (baza `bronze`, auto z Kafka Engine)
| Tabela | Engine | Opis |
|---|---|---|
| `bronze.raw_klines_queue` | Kafka | czyta z topiku `raw.binance` (grupa `clickhouse-bronze-binance`) |
| `bronze.raw_klines` | MergeTree | surowe JSON, append-only, duplikaty dozwolone |
| `bronze.raw_klines_mv` | Materialized View | auto-przelew queue → tabela |

### Silver i gold (baza `analytics`, dbt)
| Tabela | Materializacja | Zależy od | Opis |
|---|---|---|---|
| `stg_klines` | view | `bronze.raw_klines` | parsuje JSON na kolumny typu Float64 |
| `silver_prices` | incremental (delete+insert po `event_id`, okno 2 h) | `stg_klines` | OHLC, dedup: najnowszy `fetched_at` per `event_id` |
| `silver_volumes` | incremental | `stg_klines` | wolumen, obrót, avg_trade_price |
| `silver_indicators` | table | `stg_klines` | RSI(14), SMA(20), EMA(12), Bollinger (okna po wierszach) |
| `gold_ohlc_1h` | incremental (po `symbol`, `hour`; bieżąca i poprzednia godzina) | `silver_prices` | świece 1h |
| `gold_vwap` | incremental | `silver_prices` + `silver_volumes` | VWAP godzinowy |
| `gold_technical` | table | `silver_indicators` | sygnały: rsi_signal, trend_signal, volatility_signal, composite_signal |

Uwaga: przy backfillu starszych danych lub zmianie schematu tabel incremental uruchom `dbt run --full-refresh`, bo tryb incremental patrzy tylko na ostatnie 1–2 godziny.

### ML (baza `ml`, pisane przez serwis z vm4; tabele ReplacingMergeTree — w zapytaniach używaj `FINAL`)
| Tabela | Opis |
|---|---|
| `ml.predictions` | predykcja close na 1..6 świec (1–6 minut przy świecach 1m), klucz (symbol, base_candle_time, horizon) |
| `ml.anomalies` | zdarzenia anomalii, score 0–1, typ, klucz (symbol, event_start_time) |
| `ml.model_runs` | log treningów i przebiegów z metrykami |

## Serwis ML (skrót — pełny opis w ml/README.md)

- `POST /predict`: XGBoost, jeden model na horyzont 1..6, cel = zwrot `close[t+h]/close[t]-1`; trening na ostatnich 7 dniach, ponowny co 60 min; walidacja czasowa (20%) z porównaniem do modelu naiwnego; `confidence = 1 - MAE_modelu/MAE_naiwnego` (bliskie 0 jest normalne dla świec 1m).
- `POST /detect-anomalies`: Isolation Forest per symbol, uczony na 3 dniach, ocenia ostatnie 60 min.
- `POST /train`, `GET /model-info`, `GET /health`.
- Predykcja używa tylko świec z ostatniej doby; przy starych danych zwraca `no_fresh_candles`.

## Co jest gotowe, co do zrobienia

### ✅ Gotowe
- [x] Producer (Python) z KafkaSink + FileSink + Dockerfile
- [x] Docker-compose dla vm1–vm5
- [x] Bronze (Kafka Engine + MergeTree + MV), schemat `ml` (init + auto przy starcie serwisu)
- [x] dbt: staging + 3× silver + 3× gold z dokumentacją; silver_prices, silver_volumes, gold_ohlc_1h i gold_vwap jako incremental
- [x] Airflow DAG `medallion_and_ml` (nieliniowy, co 15 min)
- [x] Serwis ML: /predict, /detect-anomalies, /train, /model-info
- [x] Grafana: provisioning datasource i providera dashboardów, skrypt eksportu
- [x] Dokumentacja architektury i biznesowa

### 🔲 Do zrobienia
- [ ] **Dashboardy:** przenieść JSON-y z `Claude outputs/` do `grafana/dashboards/` (provisioning czyta tylko ten katalog) i zweryfikować zapytania względem schematu `analytics`/`ml`
- [ ] **Producer:** backfill z `startTime` od ostatniej świecy w bazie (obecnie zawsze ostatnie 1000 świec, po przerwie >16 h powstaje luka w danych)
- [ ] **Testy:** `dbt test` (unique/not_null/accepted_values, spójność OHLC, brak dziur czasowych), krok testów w DAG-u, pytest dla producera i ML, test end-to-end
- [ ] **Bezpieczeństwo:** zmienić hasła `admin/admin` (Grafana) i `airflow/airflow`, ustawić `AIRFLOW__CORE__FERNET_KEY`, rozważyć hasło dla użytkownika ClickHouse
- [ ] **Backup:** objąć backupem także bazę `ml`, przetestować restore
- [ ] **Deploy:** /etc/hosts na VM, firewall (porty 9000/8123 na vm2, 3000 na vm5)

## Znane ograniczenia

- Po przerwie producera dłuższej niż ~16,7 h świece z luki nie są pobierane (patrz: do zrobienia).
- Restart producera dubluje backfill w bronze (silver deduplikuje po `event_id`); po pierwszym załadowaniu historii można ustawić `BACKFILL_LIMIT=0`.
- Wskaźniki w `silver_indicators` liczone są po wierszach (okna ROWS), a `ema_12` jest przybliżeniem (średnia z 12 świec), więc wartości nie są identyczne z tymi z giełdy.
- Kafka (1 broker, retencja 14 dni) i ClickHouse (1 instancja) nie mają redundancji — świadomy kompromis projektu.

## Konwencje

- Każda usługa = kontener Docker, docker-compose per VM w `infra/vmX-*/`
- Komunikacja: TCP po nazwie (`vm1:9092`, `vm2:8123`, `vm4:8000`)
- Python: uv, 3.14+ lokalnie (kontenery: 3.12)
- Tabele: `bronze.raw_klines`, `analytics.silver_prices`, `analytics.gold_technical`, `ml.predictions`
- Kafkowe topiki: `raw.{źródło}` np. `raw.binance`
- dbt tagi: `tag:staging`, `tag:silver`, `tag:gold`
- Schedule: DAG co 15 min, backup co 24h

## Uruchamianie per VM

Szczegóły w `DEPLOYMENT.md`. Skrót:

```bash
git clone <repo-url> && cd bigdata_projekt
cd infra/vm1-ingest && docker compose up -d        # vm1
cd infra/vm2-clickhouse && docker compose up -d    # vm2
cd infra/vm3-orchestration && docker compose up -d --build   # vm3
cd infra/vm4-ml && docker compose up -d            # vm4
cd infra/vm5-grafana && docker compose up -d       # vm5
```

## Przydatne komendy

```bash
# Topiki Kafki (vm1)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Bronze (vm2)
docker exec clickhouse clickhouse-client -q "SELECT count(), max(fetched_at) FROM bronze.raw_klines"

# Świeżość warstw (vm2)
docker exec clickhouse clickhouse-client -q "SELECT symbol, count(), min(candle_time), max(candle_time) FROM analytics.gold_technical GROUP BY symbol"

# dbt (vm3, w kontenerze airflow)
docker exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt"
docker exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt run --full-refresh --profiles-dir /opt/airflow/dbt"

# Airflow UI (vm3): http://vm3:8082  login: airflow / airflow
# ML serwis (vm4):  curl http://vm4:8000/health
# Grafana (vm5):    http://vm5:3000  login: admin / admin
# Eksport dashboardów z UI do repo: python3 grafana/export_dashboards.py
```
