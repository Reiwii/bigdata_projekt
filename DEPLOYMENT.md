# Jak odpalić platformę

## 0. Wymagania

- Docker + Docker Compose na każdej VM
- Sieć między VM (porty: 9092, 8123, 8082, 8000, 3000)
- `/etc/hosts` na każdej VM (lub DNS):

```
10.7.1.43  vm1
10.7.1.44  vm2
10.7.1.45  vm3
10.7.1.46  vm4
10.7.1.47  vm5
```


## 1. Klonuj repo (na każdej VM)

```bash
git clone <repo-url> bigdata_projekt
cd bigdata_projekt
```

## 2. Odpalaj w kolejności

### VM1 — Kafka + Producer

```bash
cd infra/vm1-ingest

# Skopiuj .env z tokenem AllTick
cp ../../producer/.env.example ../../producer/.env
nano ../../producer/.env          # wpisz ALLTICK_TOKEN

docker compose up -d
```

Sprawdź: `docker compose logs -f producer` — powinny lecieć logi z fetchami.

Kafka UI: http://vm1:8080

### VM2 — ClickHouse

```bash
cd infra/vm2-clickhouse
docker compose up -d
```

Sprawdź (poczekaj ~30s na start):
```bash
docker exec clickhouse clickhouse-client -q "SELECT count() FROM bronze.raw_klines"
```
Liczba powinna rosnąć co minutę.

### VM3 — Airflow + dbt

```bash
cd infra/vm3-orchestration
docker compose up -d
```

Poczekaj ~2 min na `airflow-init` (migracja bazy + instalacja dbt-clickhouse).

Airflow UI: http://vm3:8082 (login: `airflow` / `airflow`)

Włącz DAG `medallion_and_ml` w UI (domyślnie jest wyłączony).

### VM4 — Serwis ML

```bash
cd infra/vm4-ml
docker compose up -d
```

Sprawdź: `curl http://vm4:8000/health` → `{"status":"ok"}`

### VM5 — Grafana

```bash
cd infra/vm5-grafana
docker compose up -d
```

Grafana: http://vm5:3000 (login: `admin` / `admin`)

## 3. Weryfikacja end-to-end

```bash
# 1. Czy Kafka ma dane? (vm1)
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic raw.alltick --max-messages 1

# 2. Czy bronze rośnie? (vm2)
docker exec clickhouse clickhouse-client -q \
  "SELECT count(), max(fetched_at) FROM bronze.raw_klines"

# 3. Czy dbt przeszło? (vm3 — po pierwszym runie DAG)
docker exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt && dbt run --profiles-dir . 2>&1 | tail -5"

# 4. Czy ML odpowiada? (vm4)
curl -X POST http://vm4:8000/predict

# 5. Czy Grafana widzi dane? (vm5)
# Otwórz http://vm5:3000 → dodaj datasource ClickHouse (vm2:8123)
```

## Troubleshooting

| Problem | Rozwiązanie |
|---|---|
| `no route to host` | Sprawdź VPN / sieć kampusową + `/etc/hosts` |
| bronze count = 0 | Kafka nie działa lub producer nie ma tokena → `docker compose logs producer` |
| DAG failed `bronze_check` | Brak świeżych danych w bronze → sprawdź vm1 |
| dbt error | `docker exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt debug"` |
| Grafana: no data | Dodaj datasource: ClickHouse, URL: `http://vm2:8123`, database: `default` |

## Zatrzymanie

```bash
# Na każdej VM:
cd infra/vmX-* && docker compose down

# Z zachowaniem danych (volumes):
docker compose down

# Czyste wymazanie (uwaga — kasuje dane!):
docker compose down -v
```
