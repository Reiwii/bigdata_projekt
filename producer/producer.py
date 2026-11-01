"""Producer: Binance Spot /api/v3/klines -> Kafka (lub plik jako fallback).

Co robi:
  * przy starcie opcjonalnie robi backfill: pobiera ostatnie BACKFILL_LIMIT świec
    dla każdego symbolu (do 1000 w jednym zapytaniu) — żeby bronze miał od razu
    kawałek historii, a dbt mógł policzyć wskaźniki (SMA20, RSI14 potrzebują
    kilkudziesięciu świec na start),
  * potem co POLL_INTERVAL_SECONDS pyta API o 2 ostatnie świece dla każdego
    symbolu i wysyła je do topiku Kafki raw.binance (klucz = symbol),
  * pilnuje limitów Binance (waga 6000/min na IP; polling 1m dla 4 symboli
    kosztuje ~8 wagi/min — mamy ~700x zapas).

Dlaczego 2 świece przy polling?
  Ostatnia świeca na Binance jest aktywna aż do swojego close_time, więc jej
  wartości (close, high, low, volume) jeszcze się zmieniają. Bierzemy ją razem
  z poprzednią (zamkniętą), bronze jest append-only, a silver i tak robi
  deduplikację po event_id biorąc wiersz z najnowszym fetched_at.

Tryby ujścia (SINK_TYPE w .env):
  * "kafka" (domyślny) — wysyła do Kafki na KAFKA_BOOTSTRAP_SERVERS
  * "file"             — append do pliku (do testów lokalnych bez Kafki)

Format pól w payload: timestamp jako sekundy Unix, open_price/high_price/
low_price/close_price/volume/turnover jako stringi. Dzięki temu bronze
pozostaje bez zmian, a stg_klines parsuje wszystko jako Float64 (volume
w krypto jest ułamkowy).
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import requests
from dotenv import load_dotenv

log = logging.getLogger("producer")


# ---------------------------------------------------------------------------
# Mapowanie interwału Binance ("1m", "5m", ...) na kline_type (minuty).
# Bronze przechowuje kline_type jako UInt8, więc trzymamy liczbę minut
# (1-godzinna = 60, 1-dniowa = 1440). Dla "1s" używamy 0 (poniżej minuty).
# ---------------------------------------------------------------------------
_MIN = {"m": 1, "h": 60, "d": 1440}


def interval_to_kline_type(interval: str) -> int:
    if interval == "1s":
        return 0
    unit = interval[-1]
    if unit not in _MIN:
        raise ValueError(f"Nieobsługiwany interwał Binance: {interval}")
    value = int(interval[:-1]) * _MIN[unit]
    if value > 255:
        raise ValueError(f"Interwał {interval} nie mieści się w UInt8 (max 255)")
    return value


# ---------------------------------------------------------------------------
# Konfiguracja
# ---------------------------------------------------------------------------
@dataclass
class Config:
    base_url: str
    symbols: list[str]
    interval: str
    kline_type: int
    poll_interval_s: int
    backfill_limit: int
    poll_limit: int
    minute_weight_budget: int
    # sink
    sink_type: str  # "kafka" | "file"
    kafka_bootstrap: str
    kafka_topic: str
    output_file: Path

    @classmethod
    def from_env(cls) -> Config:
        symbols = [
            s.strip().upper()
            for s in os.getenv("SYMBOLS", "").split(",")
            if s.strip()
        ]
        interval = os.getenv("BINANCE_INTERVAL", "1m").strip()
        cfg = cls(
            base_url=os.getenv(
                "BINANCE_BASE_URL", "https://data-api.binance.vision"
            ).rstrip("/"),
            symbols=symbols,
            interval=interval,
            kline_type=interval_to_kline_type(interval),
            poll_interval_s=int(os.getenv("POLL_INTERVAL_SECONDS", "60")),
            backfill_limit=int(os.getenv("BACKFILL_LIMIT", "1000")),
            poll_limit=int(os.getenv("POLL_LIMIT", "2")),
            minute_weight_budget=int(os.getenv("MINUTE_WEIGHT_BUDGET", "1200")),
            sink_type=os.getenv("SINK_TYPE", "kafka").lower(),
            kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "vm1:9092"),
            kafka_topic=os.getenv("KAFKA_TOPIC", "raw.binance"),
            output_file=Path(os.getenv("OUTPUT_FILE", "data/raw_binance_kline.txt")),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.symbols:
            sys.exit("Brak SYMBOLS (np. SYMBOLS=BTCUSDT,SOLUSDT,BNBUSDT,XRPUSDT).")
        if self.poll_limit < 1 or self.poll_limit > 1000:
            sys.exit("POLL_LIMIT musi być w [1, 1000].")
        if self.backfill_limit < 0 or self.backfill_limit > 1000:
            sys.exit("BACKFILL_LIMIT musi być w [0, 1000] (0 wyłącza backfill).")
        if self.poll_interval_s < 1:
            sys.exit("POLL_INTERVAL_SECONDS musi być >= 1.")
        if self.sink_type not in ("kafka", "file"):
            sys.exit(f"SINK_TYPE musi być 'kafka' lub 'file', jest: {self.sink_type}")


# ---------------------------------------------------------------------------
# Ujścia danych (Sink) — wspólny interfejs
# ---------------------------------------------------------------------------
class Sink(ABC):
    @abstractmethod
    def send(self, key: str, event: dict) -> None: ...

    @abstractmethod
    def flush(self) -> None: ...

    @abstractmethod
    def close(self) -> None: ...


class FileSink(Sink):
    """Zapis do pliku — do testów lokalnych bez Kafki."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._f = open(path, "a", encoding="utf-8")

    def send(self, key: str, event: dict) -> None:
        self._f.write(json.dumps(event, ensure_ascii=False) + "\n")

    def flush(self) -> None:
        self._f.flush()
        os.fsync(self._f.fileno())

    def close(self) -> None:
        self._f.close()


class KafkaSink(Sink):
    """Wysyłka do topiku Kafki. Klucz = symbol (partycjonowanie po symbolu)."""

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        from confluent_kafka import Producer as KProducer

        self._topic = topic
        self._producer = KProducer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "linger.ms": 100,
                "batch.size": 65536,
                "compression.type": "lz4",
                "retries": 5,
                "retry.backoff.ms": 500,
            }
        )
        self._delivery_errors: list[str] = []
        log.info("KafkaSink: %s → topik '%s'", bootstrap_servers, topic)

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self._delivery_errors.append(str(err))
            log.error("Kafka delivery error: %s", err)

    def send(self, key: str, event: dict) -> None:
        value = json.dumps(event, ensure_ascii=False).encode("utf-8")
        self._producer.produce(
            topic=self._topic,
            key=key.encode("utf-8"),
            value=value,
            callback=self._on_delivery,
        )
        self._producer.poll(0)

    def flush(self) -> None:
        remaining = self._producer.flush(timeout=10)
        if remaining > 0:
            log.warning("Kafka flush: %d wiadomości nie wysłanych.", remaining)
        if self._delivery_errors:
            errors = self._delivery_errors[:5]
            self._delivery_errors.clear()
            log.error("Błędy dostarczenia: %s", errors)

    def close(self) -> None:
        self._producer.flush(timeout=30)


def create_sink(cfg: Config) -> Sink:
    if cfg.sink_type == "kafka":
        return KafkaSink(cfg.kafka_bootstrap, cfg.kafka_topic)
    return FileSink(cfg.output_file)


# ---------------------------------------------------------------------------
# Wywołanie Binance API
# ---------------------------------------------------------------------------
class ApiError(Exception):
    pass


def _klines_weight(limit: int) -> int:
    """Oficjalna waga endpointu /api/v3/klines (Binance docs)."""
    if limit <= 100:
        return 1
    if limit <= 500:
        return 2
    if limit <= 1000:
        return 5
    return 10


def fetch_klines(
    session: requests.Session,
    base_url: str,
    symbol: str,
    interval: str,
    limit: int,
) -> list[list]:
    """Zwraca listę świec w surowym formacie Binance (lista list)."""
    resp = session.get(
        f"{base_url}/api/v3/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=15,
    )
    if resp.status_code == 429:
        raise ApiError(f"rate limit (429) retry-after={resp.headers.get('Retry-After')}")
    if resp.status_code == 418:
        raise ApiError(f"IP ban (418) retry-after={resp.headers.get('Retry-After')}")
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ApiError(f"Nieoczekiwana odpowiedź: {str(data)[:200]}")
    return data


def to_events(
    symbol: str,
    kline_type: int,
    klines: list[list],
    fetched_at: str,
) -> Iterator[tuple[str, dict]]:
    """Zamienia tablicę świec Binance na nasze zdarzenia (jedno na świecę).

    Format pola `payload`:
      * timestamp = open_time w SEKUNDACH Unix (Binance zwraca ms, dzielimy /1000)
      * open_price/high_price/low_price/close_price = jak w Binance (stringi)
      * volume = base asset volume (float jako string)
      * turnover = quote asset volume (iloczyn ceny i wolumenu w USDT)
    """
    for k in klines:
        if len(k) < 12:
            continue
        open_time_ms = int(k[0])
        ts_sec = open_time_ms // 1000
        candle = {
            "timestamp": str(ts_sec),
            "open_price": str(k[1]),
            "high_price": str(k[2]),
            "low_price": str(k[3]),
            "close_price": str(k[4]),
            "volume": str(k[5]),
            "turnover": str(k[7]),
        }
        yield (
            symbol,
            {
                "event_id": f"binance:{symbol}:{kline_type}:{ts_sec}",
                "source": "binance",
                "endpoint": "klines",
                "code": symbol,
                "kline_type": kline_type,
                "fetched_at": fetched_at,
                "payload": candle,
            },
        )


# ---------------------------------------------------------------------------
# Pętla główna
# ---------------------------------------------------------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def run(cfg: Config) -> None:
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    sink = create_sink(cfg)
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    failures = 0

    log.info(
        "Start: %d symboli (%s), interwał=%s (kline_type=%d), polling co %ds, sink=%s",
        len(cfg.symbols),
        ",".join(cfg.symbols),
        cfg.interval,
        cfg.kline_type,
        cfg.poll_interval_s,
        cfg.sink_type,
    )

    # -----------------------------------------------------------------------
    # Backfill — raz, przy starcie
    # -----------------------------------------------------------------------
    if cfg.backfill_limit > 0:
        log.info(
            "Backfill: %d świec na symbol (waga ~%d na symbol).",
            cfg.backfill_limit,
            _klines_weight(cfg.backfill_limit),
        )
        for symbol in cfg.symbols:
            if stop.is_set():
                break
            try:
                klines = fetch_klines(
                    session, cfg.base_url, symbol, cfg.interval, cfg.backfill_limit
                )
                fetched_at = _now_iso()
                count = 0
                for key, event in to_events(symbol, cfg.kline_type, klines, fetched_at):
                    sink.send(key, event)
                    count += 1
                sink.flush()
                log.info("Backfill %s: wysłano %d świec.", symbol, count)
            except (requests.RequestException, ApiError, ValueError) as exc:
                log.error("Backfill %s: błąd (%s): %s", symbol, type(exc).__name__, exc)
            # mała pauza między symbolami: 1 s wystarczy przy wadze 5 i limicie 6000/min
            if not stop.is_set():
                stop.wait(1.0)

    # -----------------------------------------------------------------------
    # Polling na żywo
    # -----------------------------------------------------------------------
    poll_weight = _klines_weight(cfg.poll_limit) * len(cfg.symbols)
    log.info(
        "Polling: %d świec/symbol co %ds (waga ~%d/req, ~%d/min dla %d symboli).",
        cfg.poll_limit,
        cfg.poll_interval_s,
        _klines_weight(cfg.poll_limit),
        poll_weight * (60 // max(1, cfg.poll_interval_s)),
        len(cfg.symbols),
    )

    while not stop.is_set():
        cycle_start = time.monotonic()
        total = 0
        cycle_failed = False

        for symbol in cfg.symbols:
            if stop.is_set():
                break
            try:
                klines = fetch_klines(
                    session, cfg.base_url, symbol, cfg.interval, cfg.poll_limit
                )
                fetched_at = _now_iso()
                for key, event in to_events(symbol, cfg.kline_type, klines, fetched_at):
                    sink.send(key, event)
                    total += 1
            except (requests.RequestException, ApiError, ValueError) as exc:
                cycle_failed = True
                failures += 1
                log.error(
                    "Polling %s: błąd (%s): %s", symbol, type(exc).__name__, exc
                )

        try:
            sink.flush()
        except Exception as exc:  # noqa: BLE001
            cycle_failed = True
            log.error("Flush sink: %s", exc)

        if not cycle_failed:
            failures = 0
            log.info("Cykl: wysłano %d świec dla %d symboli.", total, len(cfg.symbols))

        # dynamiczny backoff po błędach — rośnie wykładniczo, kap na 15 min
        if failures > 0:
            delay = min(cfg.poll_interval_s * 2**failures, 900)
            log.warning("Po błędach czekam %ds (próba #%d).", delay, failures)
        else:
            elapsed = time.monotonic() - cycle_start
            delay = max(1.0, cfg.poll_interval_s - elapsed)

        stop.wait(delay)

    sink.flush()
    sink.close()
    log.info("Zatrzymano.")


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    run(Config.from_env())


if __name__ == "__main__":
    main()
