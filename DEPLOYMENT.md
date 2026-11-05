# Jak odpalić platformę


## 1. Klonuj repo (na każdej VM) JUZ ZROBIONE NIE TRZEBA

```bash
git clone <repo-url> bigdata_projekt
cd bigdata_projekt
```

Aktualizacja kodu na VM: `git pull`, a potem w katalogu `infra/vmX-*` `sudo docker compose up -d --build`.

## 2. Odpal usługi w kolejności

### VM1 — Kafka + Producer 

Producer pobiera świece 1-minutowe z Binance Spot API (`https://data-api.binance.vision`) — publiczny endpoint, bez klucza API, bez konta. Domyślnie 4 pary: BTCUSDT, SOLUSDT, BNBUSDT, XRPUSDT; przy każdym starcie backfill ostatnich 1000 świec/symbol (~16 h historii), potem polling co 60 s.

Uwaga: backfill zawsze pobiera ostatnie 1000 świec, więc po przerwie dłuższej niż ~16 h w danych powstanie luka, a każdy restart dubluje część świec w bronze (silver je deduplikuje). Po załadowaniu historii możesz ustawić `BACKFILL_LIMIT=0` w `producer/.env`.

Plik `producer/.env` jest w `.gitignore`, więc skopiuj przykład (edycja opcjonalna — defaulty działają od razu):

```bash
cp producer/.env.example producer/.env  ## TO JUZ ZROBIONE NIE TRZEBA 
# opcjonalnie: nano producer/.env  (zmień SYMBOLS, BINANCE_INTERVAL itp.)
```

Odpal usługi:
```bash
cd infra/vm1-ingest && sudo docker compose up -d
```
Kafka UI: http://vm1:8080

### VM2 — ClickHouse
```bash
cd infra/vm2-clickhouse && sudo docker compose up -d
```
Skrypty z `init/` (bronze, schemat ml) wykonują się tylko przy pierwszym starcie z pustym wolumenem. Schemat `ml` serwis ML tworzy też sam przy starcie.

### VM3 — Airflow + dbt
```bash
cd infra/vm3-orchestration && sudo docker compose up -d --build
```
Pierwszy start trwa ~2 min (instalacja dbt-clickhouse).
Airflow UI: http://vm3:8082 (login: `airflow` / `airflow`) — włącz DAG `medallion_and_ml` (przełącznik Unpause). Dopóki DAG jest wyłączony, tabele silver i gold nie powstają.

### VM4 — Serwis ML
```bash
cd infra/vm4-ml && sudo docker compose up -d
```

### VM5 — Grafana
```bash
cd infra/vm5-grafana && sudo docker compose up -d
```
Grafana: http://vm5:3000 (login: `admin` / `admin`).
Datasource ClickHouse jest tworzony automatycznie z `grafana/provisioning/datasources/clickhouse.yml` (host `vm2`, port `9000`, protokół native, database `analytics`). Dashboardy ładują się z `grafana/dashboards/*.json`. Dashboardy zrobione w UI zapisujesz do repo skryptem `python3 grafana/export_dashboards.py`, a potem commit i push.

## 3. Sprawdzenie

```bash
# vm1 — producer:
sudo docker logs producer --tail 10

# vm2 — bronze rośnie:
sudo docker exec clickhouse clickhouse-client -q \
  "SELECT count(), max(fetched_at) FROM bronze.raw_klines"

# vm2 — silver/gold (po pierwszym runie DAG lub ręcznie na vm3):
sudo docker exec clickhouse clickhouse-client -q "SHOW TABLES FROM analytics"

# vm2 — świeżość danych w gold:
sudo docker exec clickhouse clickhouse-client -q \
  "SELECT symbol, count(), max(candle_time) FROM analytics.gold_technical GROUP BY symbol"

# vm3 — ręczny run dbt (opcjonalnie):
sudo docker exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt"

# vm3 — pełne przeliczenie tabel incremental (po backfillu lub zmianie schematu):
sudo docker exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt run --full-refresh --profiles-dir /opt/airflow/dbt"

# vm4 — ML:
curl http://vm4:8000/health
```

## 4. Stop / Start

```bash
# Zatrzymaj (dane zachowane):
cd infra/vmX-* && sudo docker compose down

# Start:
sudo docker compose up -d

# Czyste wymazanie (uwaga: kasuje dane!):
sudo docker compose down -v
```

## Schemat baz w ClickHouse

| Baza | Zawartość |
|---|---|
| `bronze` | surowe dane z Kafki (append-only) |
| `analytics` | silver + gold (dbt) |
| `ml` | predictions + anomalies + model_runs (serwis ML z vm4) |
