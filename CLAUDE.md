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

3. **Źródło danych:** AllTick API (https://alltick.co/), endpoint `/batch-kline`.
   Plan Free: max 10 grup na zapytanie, 1 req/10s, ~900 req/dzień.
   Symbole: TSLA.US, 700.HK (rozszerzalne).

## Struktura repozytorium

```
etl_koszela/
├── architektura.md          # Pełna architektura z diagramami i tabelami
├── CLAUDE.md                # TEN PLIK — kontekst dla AI
├── pyproject.toml           # Zależności Python (uv)
│
├── producer/                # [vm1] Python producer: API → Kafka
│   ├── producer.py          #   KafkaSink (prod) / FileSink (dev)
│   ├── .env                 #   tokeny i konfiguracja (gitignored)
│   └── .env.example         #   szablon .env
│
├── infra/                   # Docker Compose per VM
│   └── kafka/
│       └── docker-compose.yml  # [vm1] Kafka KRaft + Kafka UI + init topików
│
├── dbt/                     # [vm3] Projekt dbt-clickhouse (DO ZROBIENIA)
│   ├── models/
│   │   ├── bronze/          #   raw_klines (Kafka Engine)
│   │   ├── silver/          #   prices, volumes, indicators
│   │   └── gold/            #   ohlc_1h, vwap, technical
│   └── dbt_project.yml
│
├── airflow/                 # [vm3] DAG-i Airflow (DO ZROBIENIA)
│   └── dags/
│       ├── medallion_and_ml.py  # Główny DAG z fan-out/fan-in
│       └── backup_daily.py
│
├── ml/                      # [vm4] Serwis ML (DO ZROBIENIA)
│   ├── app.py               #   FastAPI: /predict + /detect-anomalies
│   ├── models/              #   Wytrenowane modele (.joblib)
│   └── train.py             #   Skrypt treningowy
│
├── grafana/                 # [vm5] Provisioning dashboardów (DO ZROBIENIA)
│   └── dashboards/
│
└── data/                    # Lokalne dane testowe (gitignored)
```

## Co jest gotowe, co do zrobienia

### ✅ Gotowe
- [x] Producer (Python) z KafkaSink + FileSink
- [x] Docker-compose Kafka (KRaft) + Kafka UI + auto-create topiku
- [x] Architektura opisana (architektura.md)
- [x] Dane testowe (data/raw_alltick_kline.txt)

### 🔲 Do zrobienia (w tej kolejności)
- [ ] **vm2:** Docker-compose ClickHouse + tabela bronze z Kafka Engine
- [ ] **vm3:** Projekt dbt — modele silver (3 tabele) i gold (3 tabele)
- [ ] **vm3:** Docker-compose Airflow + DAG `medallion_and_ml` (nieliniowy!)
- [ ] **vm4:** Serwis ML (FastAPI): predykcja XGBoost + anomalie Isolation Forest
- [ ] **vm3:** DAG podłączony do serwisu ML (SimpleHttpOperator)
- [ ] **vm5:** Grafana + dashboardy (ceny, predykcje vs real, anomalie)
- [ ] **vm5:** Backup: rsync ClickHouse → vm5, DAG `backup_daily`
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
               │                │
               └────────┬───────┘
                        ▼
                notify_grafana
```

ml_predict czeka na WSZYSTKIE trzy gold. ml_anomaly czeka tylko na gold_technical.
Backup leci w osobnym DAG-u (niezależny).

## Konwencje

- Każda usługa = jeden kontener Docker, zarządzany przez docker-compose per VM
- Komunikacja między VM: TCP po nazwie (`vm1:9092`, `vm2:8123`)
- Python: uv jako package manager, Python 3.14+
- dbt: dbt-clickhouse, modele tagowane: `tag:silver`, `tag:gold`
- Nazwy tabel: `{warstwa}.{nazwa}` np. `bronze.raw_klines`, `gold.ohlc_1h`
- Kafkowe topiki: `raw.{źródło}` np. `raw.alltick`

## Przydatne komendy

```bash
# Uruchom Kafkę (vm1)
cd infra/kafka && docker compose up -d

# Sprawdź topiki
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Uruchom producera lokalnie (z plikiem)
cd producer && SINK_TYPE=file uv run python producer.py

# Uruchom producera z Kafką
cd producer && SINK_TYPE=kafka KAFKA_BOOTSTRAP_SERVERS=localhost:9092 uv run python producer.py

# Podejrzyj dane w topiku
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic raw.alltick --from-beginning --max-messages 5
```
