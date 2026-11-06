# Kto co zrobił

**Oliwier Gorzałczyński**
- architektura systemu (5 maszyn wirtualnych)
- infrastruktura: Docker Compose dla vm1–vm5, Kafka, ClickHouse
- producer pobierający dane z Binance
- Airflow (DAG) i modele dbt (bronze → silver → gold, modele inkrementalne)
- instrukcja wdrożenia i High Level Solution Design
- dashboardy Grafany

**Maciej Kasprzyk**
- serwis ML: predykcja cen (XGBoost) i wykrywanie anomalii (Isolation Forest)
- tabele ML w ClickHouse

**Jakub Brojek**
- podłączenie Grafany do ClickHouse (datasource)

**Julia Szewczyk**
- opis biznesowy projektu
- dokumentacja techniczna

**Mikołaj Mazurek**
- testy integracyjne
- smoke testy
- testy bezpieczeństwa
