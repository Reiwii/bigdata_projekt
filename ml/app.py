"""Serwis ML (vm4): predykcja cen (XGBoost) + wykrywanie anomalii (Isolation Forest).

Wywoływany przez Airflow (DAG medallion_and_ml) po przeliczeniu warstwy gold:
  POST /detect-anomalies   ← po gold_technical
  POST /predict            ← po gold_ohlc_1h + gold_vwap + gold_technical

Czyta z ClickHouse (baza analytics, tabele gold_*), zapisuje do bazy ml:
  ml.predictions   — przewidywana cena zamknięcia na 1..6 świec do przodu
  ml.anomalies     — zdarzenia anomalii (szczytowa świeca + czas trwania), score 0-1 i typ
  ml.model_runs    — log każdego przebiegu (metryki, liczba wierszy) do Grafany / debugowania

Model predykcji jest zapisywany do /models/price_forecast.joblib (wolumen Dockera)
i trenowany ponownie co FORECAST_RETRAIN_INTERVAL_MINUTES (domyślnie 60 min).
Isolation Forest jest lekki, więc uczymy go od nowa przy każdym wywołaniu.

Kody odpowiedzi: 200 również przy "za mało danych" (świeży start — DAG nie powinien
wtedy padać), 503 gdy ClickHouse jest niedostępny (Airflow ponowi próbę).
"""

from __future__ import annotations

import json
import logging
import threading
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

import database
from anomaly_detection import detect_anomalies
from features import build_feature_frame
from forecasting import (
    InsufficientDataError,
    PriceForecastBundle,
    load_bundle,
    predict_next_closes,
    save_bundle,
    train_price_forecast,
)
from settings import Settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("ml.app")

settings = Settings.from_environment()

# Airflow może odpalić /predict i /detect-anomalies równolegle — każdy ma własną blokadę,
# żeby ten sam endpoint nie liczył się dwa razy naraz (np. ręczne wywołanie + DAG).
forecast_lock = threading.Lock()
anomaly_lock = threading.Lock()
schema_ready = False
forecast_bundle: PriceForecastBundle | None = None


class PredictRequest(BaseModel):
    symbols: list[str] | None = Field(default=None, description="Domyślnie wszystkie symbole")
    force_retrain: bool = Field(default=False, description="Wymuś trening przed predykcją")


class AnomalyRequest(BaseModel):
    symbols: list[str] | None = None
    scoring_window_minutes: int | None = Field(default=None, ge=1, le=7 * 24 * 60)


def _clickhouse_client():
    """Klient + jednorazowe utworzenie schematu ml (gdyby ClickHouse nie działał przy starcie)."""
    global schema_ready
    try:
        client = database.create_client(settings)
        if not schema_ready:
            database.ensure_ml_schema(client, settings)
            schema_ready = True
        return client
    except Exception as error:  # noqa: BLE001
        log.error("ClickHouse niedostępny: %s", error)
        raise HTTPException(status_code=503, detail=f"ClickHouse niedostępny: {error}") from error


def _log_model_run(client, task: str, symbol: str, training_rows: int, model_version: str, metrics: dict) -> None:
    run_row = pd.DataFrame(
        {
            "run_at": [pd.Timestamp.now(tz="UTC").tz_localize(None)],
            "task": [task],
            "symbol": [symbol],
            "training_rows": [int(training_rows)],
            "model_version": [model_version],
            "metrics_json": [json.dumps(metrics, default=str)],
        }
    )
    run_row["training_rows"] = run_row["training_rows"].astype("uint32")
    try:
        database.insert_rows(client, settings, "model_runs", run_row)
    except Exception as error:  # noqa: BLE001 — log przebiegu nie może wywrócić predykcji
        log.warning("Nie zapisano model_runs: %s", error)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global forecast_bundle, schema_ready
    forecast_bundle = load_bundle(settings.forecast_model_path)
    if forecast_bundle:
        log.info("Wczytano model %s z %s", forecast_bundle.model_version, settings.forecast_model_path)
    try:
        database.ensure_ml_schema(database.create_client(settings), settings)
        schema_ready = True
        log.info("Schemat %s gotowy.", settings.ml_database)
    except Exception as error:  # noqa: BLE001 — spróbujemy ponownie przy pierwszym wywołaniu
        log.warning("Nie udało się przygotować schematu przy starcie: %s", error)
    yield


app = FastAPI(title="ETL Koszela ML Service", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Endpointy
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    clickhouse_reachable = False
    try:
        clickhouse_reachable = bool(database.create_client(settings).command("SELECT 1") == 1)
    except Exception:  # noqa: BLE001
        pass
    return {
        "status": "ok",
        "clickhouse": clickhouse_reachable,
        "forecast_model": forecast_bundle.model_version if forecast_bundle else None,
    }


@app.get("/model-info")
def model_info():
    if forecast_bundle is None:
        return {"forecast_model": None}
    return {"forecast_model": forecast_bundle.summary()}


def _train_forecast_model(client, symbols: list[str] | None) -> PriceForecastBundle:
    global forecast_bundle
    candles = database.load_technical_candles(client, settings, settings.forecast_training_window_days, symbols)
    hourly_context = database.load_hourly_context(client, settings, settings.forecast_training_window_days, symbols)
    feature_frame = build_feature_frame(
        candles, hourly_context, settings.candle_minutes, settings.forecast_horizons
    )
    new_bundle = train_price_forecast(
        feature_frame,
        settings.forecast_horizons,
        settings.candle_minutes,
        settings.forecast_minimum_training_rows,
    )
    save_bundle(new_bundle, settings.forecast_model_path)
    forecast_bundle = new_bundle
    _log_model_run(client, "price_forecast_training", "ALL", new_bundle.training_rows,
                   new_bundle.model_version, new_bundle.summary()["horizons"])
    log.info("Nowy model %s zapisany do %s", new_bundle.model_version, settings.forecast_model_path)
    return new_bundle


@app.post("/train")
def train(request: PredictRequest | None = None):
    """Ręczne wymuszenie treningu modelu predykcji (bez zapisu predykcji)."""
    request = request or PredictRequest()
    with forecast_lock:
        client = _clickhouse_client()
        try:
            trained_bundle = _train_forecast_model(client, request.symbols)
        except InsufficientDataError as error:
            return {"status": "insufficient_data", "detail": str(error)}
        return {"status": "trained", "model": trained_bundle.summary()}


@app.post("/predict")
def predict(request: PredictRequest | None = None):
    request = request or PredictRequest()
    with forecast_lock:
        client = _clickhouse_client()

        needs_training = (
            request.force_retrain
            or forecast_bundle is None
            or forecast_bundle.horizons != settings.forecast_horizons
            or forecast_bundle.candle_minutes != settings.candle_minutes
            or forecast_bundle.age_minutes() >= settings.forecast_retrain_interval_minutes
        )
        retrained = False
        if needs_training:
            try:
                # trening zawsze na wszystkich symbolach — filtr dotyczy tylko predykcji
                _train_forecast_model(client, None)
                retrained = True
            except InsufficientDataError as error:
                if forecast_bundle is None:
                    log.warning("Predykcja pominięta: %s", error)
                    return {"status": "insufficient_data", "detail": str(error)}
                log.warning("Trening pominięty (%s) — używam poprzedniego modelu.", error)

        # do predykcji wystarczą ostatnie godziny (cechy patrzą max 60 świec wstecz)
        candles = database.load_technical_candles(client, settings, 1, request.symbols)
        hourly_context = database.load_hourly_context(client, settings, 1, request.symbols)
        feature_frame = build_feature_frame(candles, hourly_context, settings.candle_minutes)
        predictions = predict_next_closes(forecast_bundle, feature_frame)
        inserted_rows = database.insert_rows(client, settings, "predictions", predictions)

        _log_model_run(client, "price_forecast", "ALL", forecast_bundle.training_rows,
                       forecast_bundle.model_version, {"predictions": inserted_rows, "retrained": retrained})

        return {
            "status": "ok" if inserted_rows else "no_fresh_candles",
            "model_version": forecast_bundle.model_version,
            "retrained": retrained,
            "predictions_written": inserted_rows,
            "predictions": [
                {
                    "symbol": row.symbol,
                    "base_candle_time": row.base_candle_time.isoformat(),
                    "horizon": int(row.horizon),
                    "base_close": round(row.base_close, 8),
                    "predicted_close": round(row.predicted_close, 8),
                    "confidence": round(float(row.confidence), 4),
                }
                for row in predictions.itertuples()
            ],
        }


@app.post("/detect-anomalies")
def detect_anomalies_endpoint(request: AnomalyRequest | None = None):
    request = request or AnomalyRequest()
    scoring_window_minutes = request.scoring_window_minutes or settings.anomaly_scoring_window_minutes
    with anomaly_lock:
        client = _clickhouse_client()
        candles = database.load_technical_candles(
            client, settings, settings.anomaly_training_window_days, request.symbols
        )
        feature_frame = build_feature_frame(candles, None, settings.candle_minutes)
        if feature_frame.empty:
            return {"status": "insufficient_data", "detail": "brak danych w gold_technical"}

        model_version, symbol_results = detect_anomalies(
            feature_frame,
            scoring_window_minutes,
            settings.anomaly_contamination,
            settings.anomaly_minimum_training_rows,
            settings.candle_minutes,
        )
        all_anomalies = [result.anomalies for result in symbol_results if not result.anomalies.empty]
        anomalies = pd.concat(all_anomalies, ignore_index=True) if all_anomalies else pd.DataFrame()
        inserted_rows = database.insert_rows(client, settings, "anomalies", anomalies)

        for result in symbol_results:
            _log_model_run(client, "anomaly_detection", result.symbol, result.training_rows, model_version,
                           {"status": result.status, "scored_rows": result.scored_rows,
                            "anomalies": len(result.anomalies)})

        return {
            "status": "ok" if any(result.status == "ok" for result in symbol_results) else "insufficient_data",
            "model_version": model_version,
            "scoring_window_minutes": scoring_window_minutes,
            "anomalies_written": inserted_rows,
            "per_symbol": {
                result.symbol: {
                    "status": result.status,
                    "training_rows": result.training_rows,
                    "scored_rows": result.scored_rows,
                    "anomalies": len(result.anomalies),
                }
                for result in symbol_results
            },
            "anomalies": [
                {
                    "symbol": row.symbol,
                    "candle_time": row.candle_time.isoformat(),
                    "anomaly_score": round(row.anomaly_score, 4),
                    "anomaly_type": row.anomaly_type,
                    "event_start_time": row.event_start_time.isoformat(),
                    "event_candle_count": int(row.event_candle_count),
                }
                for row in anomalies.itertuples()
            ] if not anomalies.empty else [],
        }
