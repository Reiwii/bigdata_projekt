# Projekt ETL Koszela — Big Data Pipeline

## Co to jest

Studencki projekt Big Data: pipeline giełdowy na 5 maszynach wirtualnych.
Dane z API AllTick (świece OHLC akcji) płyną przez Kafkę do ClickHouse (architektura medalionowa),
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

4. **Źródło danych:** AllTick API (https://alltick.co/), endpoint `/batch-kline`.
   Plan Free: max 10 grup na zapytanie, 1 req/10s, ~900 req/dzień.
   Symbole: TSLA.US, 700.HK (rozszerzalne).

## Struktura repozytorium

```
etl_koszela/
├── CLAUDE.md                       # TEN PLIK — kontekst dla AI
├── architektura.md                 # Pełna architektura z diagramami
├── pyproject.toml                  # Zależności Python (uv)
│
├── infra/                          # Docker Compose — PER VM
│   ├── vm1-ingest/
│   │   └── docker-compose.yml      #   Kafka + Kafka UI + Producer
│   ├── vm2-clickhouse/
│   │   ├── docker-compose.yml      #   ClickHouse
│   │   ├── config/kafka.xml        #   konfiguracja konsumenta Kafki
│   │   └── init/01_bronze.sql      #   tabele bronze (auto przy starcie)
│   ├── vm3-orchestration/
│   │   └── docker-compose.yml      #   Airflow + Postgres + dbt
│   ├── vm4-ml/
│   │   └── docker-compose.yml      #   FastAPI ML serwis
│   └── vm5-grafana/
│       └── docker-compose.yml      #   Grafana + backup cron
│
├── producer/                       # Kod producera (używany przez vm1)
│   ├── producer.py
│   ├── Dockerfile
│   ├── .env                        #   (gitignored)
│   └── .env.example
│
├── ml/                             # Serwis ML (używany przez vm4)
│   ├── app.py                      #   FastAPI: /predict + /detect-anomalies
│   └── Dockerfile
│
├── dbt/                            # Modele dbt (używany przez vm3)  [DO ZROBIENIA]
│
├── airflow/dags/                   # DAG-i Airflow (używany przez vm3) [DO ZROBIENIA]
│
├── grafana/                        # Dashboardy (używany przez vm5) [DO ZROBIENIA]
│   ├── provisioning/
│   └── dashboards/
│
└── data/                           # Lokalne dane testowe (gitignored)
```

## Uruchamianie per VM

```bash
# Na każdej VM — klonujesz repo RAZ:
git clone <repo-url> && cd etl_koszela

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

## Co jest gotowe, co do zrobienia

### ✅ Gotowe
- [x] Producer (Python) z KafkaSink + FileSink + Dockerfile
- [x] Docker-compose vm1: Kafka (KRaft) + Kafka UI + Producer + auto-topik
- [x] Docker-compose vm2: ClickHouse + bronze (Kafka Engine + MergeTree + MV)
- [x] Docker-compose vm3: Airflow (LocalExecutor) + Postgres + dbt mount
- [x] Docker-compose vm4: FastAPI ML serwis (placeholder)
- [x] Docker-compose vm5: Grafana + backup cron
- [x] Architektura opisana (architektura.md)

### 🔲 Do zrobienia (w tej kolejności)
- [ ] **dbt:** Projekt dbt-clickhouse — modele silver (3) i gold (3)
- [ ] **Airflow:** DAG `medallion_and_ml` (nieliniowy fan-out/fan-in)
- [ ] **ML:** Implementacja /predict (XGBoost) i /detect-anomalies (Isolation Forest)
- [ ] **Grafana:** Dashboardy: ceny live, predykcje vs real, alerty anomalii
- [ ] **Backup:** Przetestować rsync vm2 → vm5
- [ ] Testy integracyjne end-to-end

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

## Konwencje

- Każda usługa = kontener Docker, docker-compose per VM w `infra/vmX-*/`
- Komunikacja: TCP po nazwie (`vm1:9092`, `vm2:8123`, `vm4:8000`)
- Python: uv, 3.14+ (kontenery: 3.12)
- Nazwy tabel: `{warstwa}.{nazwa}` np. `bronze.raw_klines`, `gold.ohlc_1h`
- Kafkowe topiki: `raw.{źródło}` np. `raw.alltick`
- dbt tagi: `tag:silver`, `tag:gold`

## Przydatne komendy

```bash
# Sprawdź topiki Kafki (vm1)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Podejrzyj dane w topiku
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic raw.alltick --from-beginning --max-messages 5

# Producer lokalnie (bez Kafki, do pliku)
cd producer && SINK_TYPE=file uv run python producer.py

# Sprawdź bronze w ClickHouse (vm2)
docker exec clickhouse clickhouse-client -q "SELECT count() FROM bronze.raw_klines"

# Airflow UI (vm3)
# http://vm3:8082  login: airflow / airflow

# Test ML serwis (vm4)
curl http://vm4:8000/health

# Grafana (vm5)
# http://vm5:3000  login: admin / admin
```
