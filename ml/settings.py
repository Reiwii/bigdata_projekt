"""Konfiguracja serwisu ML — wszystko z zmiennych środowiskowych (docker-compose vm4)."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _read_int(variable_name: str, default_value: int) -> int:
    return int(os.getenv(variable_name, str(default_value)))


def _read_float(variable_name: str, default_value: float) -> float:
    return float(os.getenv(variable_name, str(default_value)))


def _read_identifier(variable_name: str, default_value: str) -> str:
    """Nazwy baz wstawiamy do SQL jako identyfikatory, więc pilnujemy, żeby były bezpieczne."""
    value = os.getenv(variable_name, default_value).strip()
    if not _IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"{variable_name}={value!r} nie jest poprawną nazwą bazy ClickHouse")
    return value


def _read_horizons(variable_name: str, default_value: str) -> tuple[int, ...]:
    raw_value = os.getenv(variable_name, default_value)
    horizons = tuple(sorted({int(part) for part in raw_value.split(",") if part.strip()}))
    if not horizons or horizons[0] < 1 or horizons[-1] > 255:
        raise ValueError(f"{variable_name} musi zawierać liczby z zakresu 1..255, jest: {raw_value!r}")
    return horizons


@dataclass(frozen=True)
class Settings:
    # --- połączenie z ClickHouse (vm2) ---
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str
    analytics_database: str          # tu leżą tabele gold_* z dbt
    ml_database: str                 # tu zapisujemy predictions / anomalies / model_runs

    # --- wspólne ---
    candle_minutes: int              # długość świecy w gold_technical (1 = świece 1m)
    model_directory: Path            # gdzie trzymamy plik .joblib modelu predykcji

    # --- predykcja cen (XGBoost) ---
    forecast_horizons: tuple[int, ...]       # ile świec do przodu (domyślnie 1..6)
    forecast_training_window_days: int       # z ilu dni historii uczymy
    forecast_retrain_interval_minutes: int   # co ile minut model jest uczony od nowa
    forecast_minimum_training_rows: int      # poniżej tej liczby wierszy nie trenujemy

    # --- wykrywanie anomalii (Isolation Forest) ---
    anomaly_training_window_days: int
    anomaly_scoring_window_minutes: int      # ile ostatnich minut oceniamy w każdym wywołaniu
    anomaly_contamination: float             # oczekiwany odsetek anomalii (0.01 = 1%)
    anomaly_minimum_training_rows: int

    @classmethod
    def from_environment(cls) -> Settings:
        return cls(
            clickhouse_host=os.getenv("CLICKHOUSE_HOST", "vm2"),
            clickhouse_port=_read_int("CLICKHOUSE_PORT", 8123),
            clickhouse_user=os.getenv("CLICKHOUSE_USER", "default"),
            clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            analytics_database=_read_identifier("CLICKHOUSE_DB", "analytics"),
            ml_database=_read_identifier("ML_DATABASE", "ml"),
            candle_minutes=_read_int("CANDLE_MINUTES", 1),
            model_directory=Path(os.getenv("MODEL_DIRECTORY", "/models")),
            forecast_horizons=_read_horizons("FORECAST_HORIZONS", "1,2,3,4,5,6"),
            forecast_training_window_days=_read_int("FORECAST_TRAINING_WINDOW_DAYS", 7),
            forecast_retrain_interval_minutes=_read_int("FORECAST_RETRAIN_INTERVAL_MINUTES", 60),
            forecast_minimum_training_rows=_read_int("FORECAST_MINIMUM_TRAINING_ROWS", 300),
            anomaly_training_window_days=_read_int("ANOMALY_TRAINING_WINDOW_DAYS", 3),
            anomaly_scoring_window_minutes=_read_int("ANOMALY_SCORING_WINDOW_MINUTES", 60),
            anomaly_contamination=_read_float("ANOMALY_CONTAMINATION", 0.01),
            anomaly_minimum_training_rows=_read_int("ANOMALY_MINIMUM_TRAINING_ROWS", 200),
        )

    @property
    def forecast_model_path(self) -> Path:
        return self.model_directory / "price_forecast.joblib"
