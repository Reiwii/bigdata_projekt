# Platforma analityki giełdowej — dokumentacja biznesowa

## 1. Cel projektu

Zbudowanie platformy, która automatycznie pobiera dane giełdowe w czasie zbliżonym do rzeczywistego, przetwarza je i dostarcza:

- **bieżące dashboardy** z cenami akcji, wolumenami i wskaźnikami technicznymi,
- **predykcje cen** na najbliższe godziny (model AI),
- **alerty anomalii** — automatyczne wykrywanie nietypowych ruchów cen i wolumenów.

Platforma działa samodzielnie 24/7 bez ingerencji użytkownika.

---

## 2. Problem biznesowy

Analitycy i inwestorzy potrzebują szybkiego dostępu do oczyszczonych, zagregowanych danych giełdowych w jednym miejscu. Ręczne zbieranie danych z API, ich czyszczenie i wizualizacja zajmuje godziny — platforma robi to automatycznie co 5 minut.

Dodatkowo ludzkie oko nie wychwytuje wszystkich anomalii w danych. Model AI analizuje wzorce i sygnalizuje sytuacje wymagające uwagi szybciej niż człowiek.

---

## 3. Co platforma dostarcza

### Dashboardy (Grafana)

| Dashboard | Co pokazuje | Dla kogo |
|---|---|---|
| **Ceny live** | Świece OHLC (open/high/low/close), aktualizowane co 5 min | Analitycy, traderzy |
| **Wolumen i VWAP** | Obroty i cena ważona wolumenem | Analitycy |
| **Wskaźniki techniczne** | RSI, średnie kroczące, Bollinger Bands z sygnałami kupna/sprzedaży | Traderzy |
| **Predykcje AI** | Prognozowana cena zamknięcia na kolejne świece vs. rzeczywistość | Zarząd, analitycy |
| **Anomalie** | Lista wykrytych nietypowych zdarzeń z oceną ryzyka | Risk management |

### Alerty

Platforma wysyła powiadomienia gdy:
- wykryto anomalię (nagły skok wolumenu, nietypowy ruch ceny),
- RSI sygnalizuje wykupienie (>70) lub wyprzedanie (<30),
- predykcja AI odbiega znacząco od bieżącej ceny.

---

## 4. Źródła danych

| Źródło | Typ danych | Częstotliwość | Symbole |
|---|---|---|---|
| AllTick API | Świece cenowe (OHLC), wolumen, obrót | Co 5 minut | TSLA (Tesla), 700.HK (Tencent) |

Platforma jest przygotowana na dodawanie kolejnych symboli i źródeł danych bez przebudowy architektury.

---

## 5. Jak przepływają dane (uproszczony widok)

```
  Giełdy (API)
      │
      ▼
  Pobranie danych          co 5 minut, automatycznie
      │
      ▼
  Surowe dane              dokładna kopia z API, nic nie zmienione
      │
      ├──► Czyste ceny          ─── ► Agregaty cenowe (1h, 1d)
      ├──► Czyste wolumeny      ─── ► Średnia ważona wolumenem (VWAP)
      └──► Wskaźniki techniczne ─── ► Sygnały kupna / sprzedaży
                                          │
                              ┌────────────┤
                              ▼            ▼
                        Predykcja AI   Wykrywanie
                        (cena za 1-6h) anomalii
                              │            │
                              ▼            ▼
                          Dashboardy + alerty
```

Trzy środkowe ścieżki (ceny, wolumeny, wskaźniki) przetwarzają się **równolegle** — platforma nie czeka aż jedna skończy, żeby zacząć drugą.

---

## 6. Warstwy danych

| Warstwa | Co zawiera | Analogia |
|---|---|---|
| **Surowe dane** | Dokładna kopia z API — nic nie zmienione, nic nie usunięte | Kserokopia faktury |
| **Dane oczyszczone** | Te same dane, ale uporządkowane, bez duplikatów, z poprawnymi typami | Faktura wpisana do systemu |
| **Agregaty** | Gotowe zestawienia: średnie, sumy, wskaźniki | Raport miesięczny |
| **Wyniki AI** | Predykcje i wykryte anomalie | Rekomendacja analityka |

Jeśli coś pójdzie nie tak na dowolnym etapie, można przeliczyć wszystko od nowa z surowych danych — nic nie jest tracone.

---

## 7. Komponent AI

### Predykcja cen

Model uczenia maszynowego (XGBoost) analizuje historyczne ceny, wolumeny i wskaźniki techniczne, żeby prognozować cenę zamknięcia na kolejne 1-6 świec (5 min - 30 min do przodu).

Predykcje **nie są rekomendacją inwestycyjną** — to narzędzie wspierające decyzje analityka.

### Wykrywanie anomalii

Model (Isolation Forest) automatycznie identyfikuje nietypowe zdarzenia:
- nagły wzrost lub spadek wolumenu,
- ruch ceny odbiegający od wzorca,
- nietypowa relacja między wskaźnikami technicznymi.

Każda anomalia dostaje **score** (0-1) mówiący jak bardzo nietypowe jest zdarzenie.

---

## 8. Dostępność i bezpieczeństwo

| Aspekt | Rozwiązanie |
|---|---|
| **Dostępność** | Platforma działa 24/7, automatyczny restart usług po awarii |
| **Backup** | Codzienna kopia danych na osobną maszynę |
| **Odtwarzanie** | W razie awarii dane odtwarzalne z backupu + 14 dni surowych danych w buforze |
| **Czas odtwarzania** | Do kilku godzin (akceptowalne dla analityki, nie jest to system tradingowy) |

---

## 9. Infrastruktura (podsumowanie)

Platforma działa na **5 maszynach wirtualnych**, każda odpowiedzialna za jeden etap:

| Maszyna | Rola | Dostęp użytkownika |
|---|---|---|
| VM1 | Pobieranie danych i kolejkowanie | brak (wewnętrzna) |
| VM2 | Baza danych | brak (wewnętrzna) |
| VM3 | Harmonogram przetwarzania | Panel Airflow: `http://vm3:8082` |
| VM4 | Modele AI | brak (wewnętrzna, wywoływana automatycznie) |
| VM5 | **Dashboardy** | **Grafana: `http://vm5:3000`** ← tu wchodzi użytkownik |

Użytkownik końcowy korzysta tylko z **Grafany (vm5)** — reszta działa w tle.

---

## 10. Planowany rozwój

| Faza | Co dodajemy | Korzyść biznesowa |
|---|---|---|
| **Faza 2** | Więcej symboli (S&P 500, GPW) | Szersze pokrycie rynków |
| **Faza 3** | Analiza sentymentu z newsów | Kontekst do ruchów cen |
| **Faza 4** | Korelacje między symbolami | Analiza portfelowa |
| **Faza 5** | Alerty na Slacka / email | Natychmiastowe powiadomienia |
