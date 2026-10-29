"""Producer: AllTick /batch-kline -> Kafka (lub plik jako fallback).

Co robi:
  * co POLL_INTERVAL_SECONDS pyta API o 2 ostatnie świece dla listy symboli,
  * każdą świecę wysyła do topiku Kafki raw.alltick (klucz = symbol),
  * pilnuje limitów planu Free (odstęp, dzienny limit, backoff przy błędach).

Tryby ujścia (SINK_TYPE w .env):
  * "kafka" (domyślny) — wysyła do Kafki na KAFKA_BOOTSTRAP_SERVERS
  * "file"             — append do pliku (do testów lokalnych bez Kafki)

Uwaga: /batch-kline zwraca ZAWSZE 2 ostatnie świece, więc ta sama świeca
(ten sam event_id) pojawi się wielokrotnie, a ostatnia wersja ma finalne
wartości. To celowe: bronze jest surowe, deduplikacja dzieje się w silver
(zostaje wiersz z najnowszym fetched_at dla danego event_id).
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import requests
from dotenv import load_dotenv

log = logging.getLogger("producer")


# ---------------------------------------------------------------------------
# Konfiguracja
# ---------------------------------------------------------------------------
@dataclass
class Config:
    token: str
    base_url: str
    symbols: list[str]
    kline_types: list[int]
    interval_s: int
    max_groups: int
    daily_limit: int
    # sink
    sink_type: str  # "kafka" | "file"
    kafka_bootstrap: str
    kafka_topic: str
    output_file: Path

    @classmethod
    def from_env(cls) -> Config:
        token = os.getenv("ALLTICK_TOKEN", "").strip()
        symbols = [s.strip() for s in os.getenv("SYMBOLS", "").split(",") if s.strip()]
        kline_types = [
            int(k) for k in os.getenv("KLINE_TYPES", "2").split(",") if k.strip()
        ]
        cfg = cls(
            token=token,
            base_url=os.getenv(
                "ALLTICK_BASE_URL", "https://quote.alltick.co/quote-stock-b-api"
            ).rstrip("/"),
            symbols=symbols,
            kline_types=kline_types,
            interval_s=int(os.getenv("POLL_INTERVAL_SECONDS", "300")),
            max_groups=int(os.getenv("MAX_GROUPS", "10")),
            daily_limit=int(os.getenv("DAILY_REQUEST_LIMIT", "900")),
            sink_type=os.getenv("SINK_TYPE", "kafka").lower(),
            kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "vm1:9092"),
            kafka_topic=os.getenv("KAFKA_TOPIC", "raw.alltick"),
            output_file=Path(os.getenv("OUTPUT_FILE", "data/raw_alltick_kline.txt")),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.token:
            sys.exit("Brak ALLTICK_TOKEN (ustaw w .env).")
        if not self.symbols:
            sys.exit("Brak SYMBOLS (np. SYMBOLS=700.HK,GOOGL.US).")
        groups = len(self.symbols) * len(self.kline_types)
        if groups > self.max_groups:
            sys.exit(
                f"{groups} grup (symbole x typy świec) > limit {self.max_groups}. "
                "Zmniejsz liczbę symboli/typów albo zmień MAX_GROUPS."
            )
        if self.interval_s < 10:
            sys.exit("POLL_INTERVAL_SECONDS musi być >= 10 (limit Free: 1/10s).")
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
                "linger.ms": 100,  # mini-batching wewnątrz producenta
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
        # Obsługa callbacków bez blokowania
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
# Wywołanie API i zamiana odpowiedzi na zdarzenia
# ---------------------------------------------------------------------------
class ApiError(Exception):
    pass


def fetch_latest_klines(session: requests.Session, cfg: Config) -> dict:
    body = {
        "trace": str(uuid.uuid4()),
        "data": {
            "data_list": [
                {
                    "code": code,
                    "kline_type": kt,
                    "kline_timestamp_end": 0,
                    "query_kline_num": 2,
                    "adjust_type": 0,
                }
                for code in cfg.symbols
                for kt in cfg.kline_types
            ]
        },
    }
    resp = session.post(
        f"{cfg.base_url}/batch-kline",
        params={"token": cfg.token},
        json=body,
        timeout=15,
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("ret") != 200:
        raise ApiError(f"ret={payload.get('ret')} msg={payload.get('msg')}")
    return payload


def to_events(payload: dict, fetched_at: str) -> Iterator[tuple[str, dict]]:
    """Zwraca (klucz, zdarzenie): jedno zdarzenie na jedną świecę."""
    for item in payload.get("data", {}).get("kline_list", []):
        code = item["code"]
        kline_type = item["kline_type"]
        for candle in item.get("kline_data", []):
            yield (
                code,
                {
                    "event_id": f"alltick:{code}:{kline_type}:{candle['timestamp']}",
                    "source": "alltick",
                    "endpoint": "batch-kline",
                    "code": code,
                    "kline_type": kline_type,
                    "fetched_at": fetched_at,
                    "payload": candle,
                },
            )


# ---------------------------------------------------------------------------
# Pętla główna
# ---------------------------------------------------------------------------
def seconds_until_midnight() -> float:
    now = datetime.now()
    midnight = (now + timedelta(days=1)).replace(
        hour=0, minute=0, second=5, microsecond=0
    )
    return (midnight - now).total_seconds()


def run(cfg: Config) -> None:
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    sink = create_sink(cfg)
    session = requests.Session()
    failures = 0
    requests_today = 0
    today = datetime.now().date()

    log.info(
        "Start: %d symboli x %d typów świec, co %ds, sink=%s",
        len(cfg.symbols),
        len(cfg.kline_types),
        cfg.interval_s,
        cfg.sink_type,
    )

    try:
        while not stop.is_set():
            if datetime.now().date() != today:
                today = datetime.now().date()
                requests_today = 0

            if requests_today >= cfg.daily_limit:
                wait = seconds_until_midnight()
                log.warning(
                    "Dzienny limit (%d) wyczerpany, czekam %.0f s do północy.",
                    cfg.daily_limit,
                    wait,
                )
                stop.wait(wait)
                continue

            requests_today += 1
            try:
                payload = fetch_latest_klines(session, cfg)
                fetched_at = datetime.now(timezone.utc).isoformat(
                    timespec="milliseconds"
                )
                count = 0
                for key, event in to_events(payload, fetched_at):
                    sink.send(key, event)
                    count += 1
                sink.flush()
                failures = 0
                log.info(
                    "Wysłano %d świec (zapytań dziś: %d/%d)",
                    count,
                    requests_today,
                    cfg.daily_limit,
                )
                delay = cfg.interval_s
            except (requests.RequestException, ApiError, ValueError, KeyError) as exc:
                failures += 1
                msg = str(exc).replace(cfg.token, "***")
                delay = min(cfg.interval_s * 2**failures, 900)
                log.error(
                    "Błąd (%s): %s. Próba %d, ponawiam za %ds.",
                    type(exc).__name__,
                    msg,
                    failures,
                    delay,
                )

            stop.wait(delay)
    finally:
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
