"""Predykcja ceny zamknięcia na kolejne świece — XGBoost, jeden model na horyzont.

Model przewiduje ZWROT (close za N świec / close teraz − 1), a nie cenę wprost:
zwroty mają podobną skalę dla wszystkich symboli, więc jeden model uczy się na
wszystkich parach naraz. Cena = base_close × (1 + przewidziany zwrot).

Jakość mierzymy na ostatnich 20% danych (podział czasowy, bez mieszania przyszłości
z przeszłością) i porównujemy z modelem naiwnym "cena się nie zmieni".
confidence = 1 − MAE_modelu / MAE_naiwnego, przycięte do [0, 1]:
0 = model nie jest lepszy od zgadywania "bez zmian", 1 = model idealny.
Dla 1-minutowych świec krypto realnie wychodzi blisko 0 — to uczciwy wynik, nie błąd.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from xgboost import XGBRegressor

from features import PRICE_FEATURE_COLUMNS

log = logging.getLogger("ml.forecasting")

VALIDATION_FRACTION = 0.2
BUNDLE_FORMAT_VERSION = 1


class InsufficientDataError(Exception):
    pass


@dataclass
class HorizonModel:
    horizon: int
    regressor: XGBRegressor
    validation_mae_return: float
    naive_mae_return: float
    confidence: float
    training_rows: int
    validation_rows: int


@dataclass
class PriceForecastBundle:
    trained_at: datetime
    candle_minutes: int
    feature_columns: list[str]
    horizon_models: dict[int, HorizonModel]
    symbols: list[str]
    training_rows: int
    format_version: int = BUNDLE_FORMAT_VERSION
    extra_info: dict = field(default_factory=dict)

    @property
    def model_version(self) -> str:
        return "xgb-" + self.trained_at.strftime("%Y%m%dT%H%M%SZ")

    @property
    def horizons(self) -> tuple[int, ...]:
        return tuple(sorted(self.horizon_models))

    def age_minutes(self, now: datetime | None = None) -> float:
        now = now or datetime.now(timezone.utc)
        return (now - self.trained_at).total_seconds() / 60.0

    def summary(self) -> dict:
        return {
            "model_version": self.model_version,
            "trained_at": self.trained_at.isoformat(),
            "training_rows": self.training_rows,
            "symbols": self.symbols,
            "horizons": {
                str(horizon): {
                    "validation_mae_return": model.validation_mae_return,
                    "naive_mae_return": model.naive_mae_return,
                    "confidence": model.confidence,
                    "training_rows": model.training_rows,
                    "validation_rows": model.validation_rows,
                }
                for horizon, model in sorted(self.horizon_models.items())
            },
        }


def _new_regressor() -> XGBRegressor:
    return XGBRegressor(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        reg_lambda=1.0,
        tree_method="hist",
        objective="reg:squarederror",
        n_jobs=2,
        random_state=42,
    )


def _train_single_horizon(
    feature_frame: pd.DataFrame, horizon: int, candle_minutes: int, minimum_training_rows: int
) -> HorizonModel:
    target_column = f"target_return_h{horizon}"
    # XGBoost radzi sobie z brakami w cechach (np. brak kontekstu godzinowego w 1. godzinie),
    # więc wymagamy tylko znanego celu i podstawowego zwrotu.
    usable_rows = feature_frame.dropna(subset=[target_column, "return_1"]).sort_values("candle_time")
    if len(usable_rows) < minimum_training_rows:
        raise InsufficientDataError(
            f"horyzont {horizon}: tylko {len(usable_rows)} wierszy, potrzeba {minimum_training_rows}"
        )

    validation_start_time = usable_rows["candle_time"].quantile(1 - VALIDATION_FRACTION)
    # odstęp = horyzont, żeby cele treningowe nie "zaglądały" w okres walidacji
    training_end_time = validation_start_time - pd.Timedelta(minutes=horizon * candle_minutes)
    training_part = usable_rows[usable_rows["candle_time"] < training_end_time]
    validation_part = usable_rows[usable_rows["candle_time"] >= validation_start_time]

    if len(training_part) >= minimum_training_rows // 2 and len(validation_part) > 0:
        validation_regressor = _new_regressor()
        validation_regressor.fit(training_part[PRICE_FEATURE_COLUMNS], training_part[target_column])
        validation_predictions = validation_regressor.predict(validation_part[PRICE_FEATURE_COLUMNS])
        validation_mae_return = float(np.mean(np.abs(validation_predictions - validation_part[target_column])))
        naive_mae_return = float(np.mean(np.abs(validation_part[target_column])))  # "bez zmian" = zwrot 0
        confidence = 0.0 if naive_mae_return == 0 else float(
            np.clip(1.0 - validation_mae_return / naive_mae_return, 0.0, 1.0)
        )
    else:
        validation_mae_return = float("nan")
        naive_mae_return = float("nan")
        confidence = 0.0

    # model docelowy uczymy na WSZYSTKICH danych (walidacja służyła tylko do oceny)
    final_regressor = _new_regressor()
    final_regressor.fit(usable_rows[PRICE_FEATURE_COLUMNS], usable_rows[target_column])

    return HorizonModel(
        horizon=horizon,
        regressor=final_regressor,
        validation_mae_return=validation_mae_return,
        naive_mae_return=naive_mae_return,
        confidence=confidence,
        training_rows=len(usable_rows),
        validation_rows=len(validation_part),
    )


def train_price_forecast(
    feature_frame: pd.DataFrame,
    forecast_horizons: tuple[int, ...],
    candle_minutes: int,
    minimum_training_rows: int,
) -> PriceForecastBundle:
    if feature_frame.empty:
        raise InsufficientDataError("brak danych w gold_technical")

    horizon_models = {}
    for horizon in forecast_horizons:
        horizon_models[horizon] = _train_single_horizon(
            feature_frame, horizon, candle_minutes, minimum_training_rows
        )
        log.info(
            "Horyzont %d: MAE=%.6f naive=%.6f confidence=%.3f (%d wierszy)",
            horizon,
            horizon_models[horizon].validation_mae_return,
            horizon_models[horizon].naive_mae_return,
            horizon_models[horizon].confidence,
            horizon_models[horizon].training_rows,
        )

    return PriceForecastBundle(
        trained_at=datetime.now(timezone.utc).replace(microsecond=0),
        candle_minutes=candle_minutes,
        feature_columns=list(PRICE_FEATURE_COLUMNS),
        horizon_models=horizon_models,
        symbols=sorted(feature_frame["symbol"].unique().tolist()),
        training_rows=int(feature_frame["return_1"].notna().sum()),
    )


def predict_next_closes(
    bundle: PriceForecastBundle, feature_frame: pd.DataFrame, maximum_staleness_candles: int = 5
) -> pd.DataFrame:
    """Predykcja z OSTATNIEJ zamkniętej świecy każdego symbolu na każdy horyzont."""
    if feature_frame.empty:
        return pd.DataFrame()

    newest_candle_time = feature_frame["candle_time"].max()
    staleness_limit = newest_candle_time - pd.Timedelta(minutes=maximum_staleness_candles * bundle.candle_minutes)
    latest_rows = (
        feature_frame.dropna(subset=["return_1"])
        .sort_values("candle_time")
        .groupby("symbol", as_index=False)
        .tail(1)
    )
    latest_rows = latest_rows[latest_rows["candle_time"] >= staleness_limit]
    if latest_rows.empty:
        return pd.DataFrame()

    predicted_at = pd.Timestamp.now(tz="UTC").tz_localize(None)
    prediction_frames = []
    for horizon, horizon_model in sorted(bundle.horizon_models.items()):
        predicted_returns = horizon_model.regressor.predict(latest_rows[bundle.feature_columns])
        prediction_frames.append(
            pd.DataFrame(
                {
                    "symbol": latest_rows["symbol"].to_numpy(),
                    "base_candle_time": latest_rows["candle_time"].to_numpy(),
                    "target_candle_time": (
                        latest_rows["candle_time"] + pd.Timedelta(minutes=horizon * bundle.candle_minutes)
                    ).to_numpy(),
                    "horizon": np.uint8(horizon),
                    "base_close": latest_rows["close"].to_numpy(dtype=float),
                    "predicted_close": latest_rows["close"].to_numpy(dtype=float) * (1.0 + predicted_returns),
                    "predicted_return": predicted_returns.astype(float),
                    "validation_mae_return": horizon_model.validation_mae_return,
                    "confidence": np.float32(horizon_model.confidence),
                    "model_version": bundle.model_version,
                    "predicted_at": predicted_at,
                }
            )
        )
    predictions = pd.concat(prediction_frames, ignore_index=True)
    predictions["horizon"] = predictions["horizon"].astype("uint8")
    predictions["confidence"] = predictions["confidence"].astype("float32")
    predictions["validation_mae_return"] = predictions["validation_mae_return"].fillna(0.0)
    return predictions


def save_bundle(bundle: PriceForecastBundle, model_path: Path) -> None:
    model_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = model_path.with_suffix(".tmp")
    joblib.dump(bundle, temporary_path)
    temporary_path.replace(model_path)  # atomowa podmiana — nie zostawiamy połowy pliku


def load_bundle(model_path: Path) -> PriceForecastBundle | None:
    if not model_path.exists():
        return None
    try:
        bundle = joblib.load(model_path)
    except Exception as error:  # noqa: BLE001 — uszkodzony plik = po prostu trenujemy od nowa
        log.warning("Nie udało się wczytać modelu %s: %s", model_path, error)
        return None
    if getattr(bundle, "format_version", None) != BUNDLE_FORMAT_VERSION or bundle.feature_columns != PRICE_FEATURE_COLUMNS:
        log.info("Model %s ma inny format/cechy — zostanie wytrenowany od nowa.", model_path)
        return None
    return bundle
