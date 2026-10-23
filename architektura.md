 # to wstepne jeszcze mozna zmienic
# Architektura platformy danych (medalion): Kafka -> ClickHouse -> dbt -> BI

Dokument opisuje, **jak przepływają dane** i **co działa na której maszynie**.
Założenia: 5 maszyn wirtualnych, każda usługa to jeden kontener (Docker Compose).
Bezpieczeństwo danych zapewniają **backupy** oraz **14-dniowa retencja w Kafce**. Replikację można dołożyć później (sekcja 9).

---

## 1. Obraz ogólny

```
                         ŹRÓDŁA
                  ┌─────────────────────┐
                  │  API giełdowe       │
                  │  (REST / WebSocket) │
                  └─────────┬───────────┘
                            │ pobieranie
                            v
 vm1  INGEST      ┌─────────────────────┐
                  │  Producer (Python)  │  streaming (WebSocket) lub
                  │  lub DAG z Airflow  │  mini-batch (REST co 1-5 min)
                  └─────────┬───────────┘
                            v
                  ┌─────────────────────┐
                  │  KAFKA (1 broker)   │  topik: raw.<źródło>
                  │  retencja 14 dni    │  = bufor i "kopia źródłowa"
                  └─────────┬───────────┘
                            │ silnik Kafka + Materialized View
                            v
 vm2  BAZA        ┌──────────────────────┐
                  │  CLICKHOUSE          │
                  │  BRONZE  (surowe)    │
                  │  SILVER  (czyste)    │
                  │  GOLD    (agregaty)  │
                  └───┬──────────────┬───┘
                      ^              │ BACKUP raz na dobę
                      │ dbt (SQL)    v
 vm3  ORKIESTRACJA    │        vm4  BACKUPY
      ┌───────────────┴────┐   ┌──────────────────────┐
      │ AIRFLOW (scheduler │   │ MinIO                │
      │  + web UI)         │   │ (kopie ClickHouse)   │
      │ dbt +dbt-clickhouse│   └──────────────────────┘
      │ POSTGRES (pamięć   │
      │  Airflow)          │   
      └────────────────────┘ 
```

---

## 2. Przepływ danych krok po kroku

| # | Etap | Co się dzieje | Kto to robi |
|---|---|---|---|
| 1 | **Pobranie** | Dane z API trafiają do systemu. Streaming: producer trzyma połączenie WebSocket. Mini-batch: DAG w Airflow odpytuje REST co 1-5 min. | Producer / Airflow (vm1, vm3) |
| 2 | **Kolejka** | Surowe zdarzenia (JSON) lądują w topiku Kafki `raw.<źródło>`. Kafka trzyma je 14 dni. | Kafka (vm1) |
| 3 | **Bronze** | ClickHouse sam czyta z Kafki i zapisuje **surowy JSON bez zmian** do tabeli append-only. | ClickHouse (vm2) |
| 4 | **Silver** | dbt czyści dane: parsuje JSON, nadaje typy, usuwa duplikaty. | dbt uruchamiany z Airflow (vm3) |
| 5 | **Gold** | dbt liczy agregaty: OHLC, VWAP, średnie w oknach czasowych. | dbt uruchamiany z Airflow (vm3) |
| 6 | **Odbiorcy** | Dashboardy i analitycy czytają **tylko z gold**. | Grafana / BI (vm5) |
| 7 | **Backup** | Raz na dobę DAG robi kopię bazy do MinIO. | Airflow -> MinIO (vm4) |

### Warstwy medalionu w jednym zdaniu

- **Bronze**: dane dokładnie takie, jak przyszły. Nic nie jest tracone, można zawsze przeliczyć od nowa.
- **Silver**: te same dane, ale oczyszczone i z typami. Bez duplikatów.
- **Gold**: gotowe agregaty pod konkretne raporty i wykresy.

---

## 3. Co działa na której maszynie

| VM | Rola | Usługi | Minimum zasobów |
|---|---|---|---|
| **vm1** | Ingest | Kafka (1 broker, KRaft), Kafka UI, producery API | 2-4 vCPU, 8 GB RAM, 100 GB dysku |
| **vm2** | Baza | ClickHouse (bronze, silver, gold) | 4-8 vCPU, 16 GB RAM, szybki SSD |
| **vm3** | Orkiestracja | Airflow (LocalExecutor), Postgres, dbt | 4 vCPU, 8 GB RAM |
| **vm4** | Backupy | MinIO | 2 vCPU, 4 GB RAM, duży dysk |
| **vm5** | Podgląd / zapas | Grafana; wolne miejsce na dev lub przyszłą replikę | 2 vCPU, 4 GB RAM |

Komunikacja między maszynami to zwykłe TCP po nazwach / IP (`vm1:9092`). Wystarczy firewall otwierający potrzebne porty.

---

## 4. Orkiestracja: odpowiednik jobów z Databricks

Airflow działa jak Databricks Workflows: **DAG = job**, **task = krok joba**.

| DAG | Harmonogram | Kroki |
|---|---|---|
| `ingest_<źródło>` | co 1-5 min (tylko dla REST) | pobierz z API -> wyślij do Kafki |
| `medallion_refresh` | co 5 min | sprawdź świeżość bronze -> `dbt run` silver -> `dbt test` -> `dbt run` gold -> `dbt test` |
| `backup_daily` | raz na dobę | `BACKUP DATABASE ... TO S3(MinIO)` -> sprawdzenie wyniku |

**dbt nie jest osobną usługą.** To pakiet zainstalowany w obrazie Airflow i wywoływany przez `BashOperator`, np. `dbt run --select tag:silver`.

**Postgres** to wyłącznie pamięć Airflow (stan DAG-ów, historia uruchomień). Nie trzyma żadnych danych giełdowych.

---

## 5. Backupy (jedyna ochrona danych w ClickHouse)

Skoro nie ma replikacji, backupy są kluczowe:

- **Co:** bazy `gold` i `silver` (codziennie), `bronze` opcjonalnie, bo da się je odtworzyć z Kafki.
- **Jak:** DAG `backup_daily` wykonuje `BACKUP DATABASE gold TO S3('http://vm4:9000/backups/gold/<data>', ...)`. Opcjonalnie backup przyrostowy (`base_backup`), żeby oszczędzić miejsce.
- **Gdzie:** MinIO na osobnej maszynie (vm4), nigdy na dysku ClickHouse.
- **Ile trzymamy:** np. 7 dziennych kopii, starsze usuwa DAG.
- **Test restore:** raz na jakiś czas odtwórz kopię na maszynie testowej. Backup, którego nie sprawdzono, nie jest backupem.
- **Dodatkowo:** raz na jakiś czas skopiuj katalog MinIO w drugie miejsce (`rsync` / `restic` / zewnętrzny S3), bo jedna kopia na jednym dysku to wciąż jeden punkt awarii.

---

## 6. Model awarii

| Co padnie | Skutek | Odtworzenie |
|---|---|---|
| Kontener ClickHouse | brak odczytów i analityki | restart; po starcie baza **doczytuje zaległe dane z Kafki** (do 14 dni wstecz) |
| Dysk ClickHouse (utrata danych) | utrata bazy | restore z MinIO + replay z Kafki od momentu backupu |
| Kafka (vm1) | zatrzymanie nowych danych | restart kontenera, dane na wolumenie zostają; producenci mają retry/bufor |
| Airflow (vm3) | stop liczenia silver/gold, bronze nadal się zapełnia | restart; DAG nadrabia zaległości |
| Błąd człowieka (`DROP TABLE`, zepsute dbt) | dane zepsute | restore z backupu (stan z poprzedniej doby) + przeliczenie dbt |

Brak automatycznego failoveru: przestoje wymagają ręcznego restartu, a odtworzenie po utracie dysku trwa godziny, nie sekundy. Dla analityki to akceptowalny kompromis.

**Ważne:** retencja Kafki (14 dni) ma być **dłuższa niż odstęp między backupami**. Dzięki temu po restore zawsze da się dograć brakujące dane.

---

## 7. Kolejność wdrożenia

| Krok | Co | Test, że działa |
|---|---|---|
| 1 | Docker na wszystkich VM, otwarte porty, wpisy `vmX` w `/etc/hosts` | `docker --context vmX ps` |
| 2 | Kafka + Kafka UI na vm1, utworzenie topiku `raw.*` | produce / consume z konsoli |
| 3 | ClickHouse na vm2 + tabele bronze | `SELECT count() FROM bronze.events` rośnie |
| 4 | Producer jednego API -> Kafka | dane w bronze po kilku sekundach |
| 5 | Airflow + dbt na vm3: silver i gold | DAG zielony, `dbt test` przechodzi |
| 6 | MinIO na vm4, DAG backupu, **test restore** | odtworzenie tabeli z backupu |

Szacunkowo kroki 1-5 to jeden do dwóch weekendów.

---

---

## 9. skalowanie wrazie co 

1. **Monitoring i alerty:** Prometheus + Grafana (alert na brak świeżych danych w bronze).
2. **Replikacja ClickHouse:** druga replika na vm5, Keeper (3 instancje) i `ReplicatedMergeTree`. Rdzeń (topiki, bronze/silver/gold, dbt) się nie zmienia, dochodzą tylko pliki konfiguracyjne i `cluster` w profilu dbt.
3. **Kafka z HA:** dwa kolejne brokery, RF=3.
4. **Archiwum surowych danych** (Parquet) w MinIO.
5. **Warstwa serwująca** (Redis / ScyllaDB) dla "ostatniej ceny" w milisekundach.
6. **CeleryExecutor** w Airflow, gdy jedna maszyna nie wystarczy.
