# Jak odpalić platformę


## 1. Klonuj repo (na każdej VM)

```bash
git clone <repo-url> bigdata_projekt
cd bigdata_projekt
```

## 2. Odpal usługi w kolejności

### VM1 — Kafka + Producer

Plik `producer/.env` jest w `.gitignore` (zawiera token AllTick), więc musi powstać ręcznie na VM1:

```bash
cp producer/.env.example producer/.env
nano producer/.env           # wpisz ALLTICK_TOKEN=<twój token z alltick.co>
```

Domyślnie producer pobiera 2 symbole krypto (BTCUSDT, ETHUSDT) z crypto API AllTick. Krypto handluje się 24/7, więc dane płyną cały czas. Aby zmienić listę — edytuj `SYMBOLS=` w `producer/.env`. Dla akcji użyj innego base URL (`https://quote.alltick.co/quote-stock-b-api`) i formatu symboli `AAPL.US`, `700.HK` itp.

Odpal usługi:
```bash
cd infra/vm1-ingest && sudo docker compose up -d
```
Kafka UI: http://vm1:8080

### VM2 — ClickHouse
```bash
cd infra/vm2-clickhouse && sudo docker compose up -d
```

### VM3 — Airflow + dbt
```bash
cd infra/vm3-orchestration && sudo docker compose up -d
```
Pierwszy start trwa ~2 min (instalacja dbt-clickhouse).
Airflow UI: http://vm3:8082 (login: `airflow` / `airflow`) — włącz DAG `medallion_and_ml`.

### VM4 — Serwis ML
```bash
cd infra/vm4-ml && sudo docker compose up -d
```

### VM5 — Grafana
```bash
cd infra/vm5-grafana && sudo docker compose up -d
```
Grafana: http://vm5:3000 (login: `admin` / `admin`).
Datasource ClickHouse: host `vm2`, port `8123`, database `analytics`.

## 3. Sprawdzenie

```bash
# vm1 — producer:
sudo docker logs producer --tail 5

# vm2 — bronze rośnie:
sudo docker exec clickhouse clickhouse-client -q \
  "SELECT count(), max(fetched_at) FROM bronze.raw_klines"

# vm2 — silver/gold (po pierwszym runie DAG lub ręcznie na vm3):
sudo docker exec clickhouse clickhouse-client -q "SHOW TABLES FROM analytics"

# vm3 — ręczny run dbt (opcjonalnie):
sudo docker exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt run"

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
| `ml` | predictions + anomalies (serwis ML z vm4) |
