"""Wykrywanie anomalii — Isolation Forest, osobny model dla każdego symbolu.

Osobny model per symbol, bo "normalny" wolumen czy zmienność BTC i XRP to zupełnie
inne wartości. Model uczy się na ostatnich dniach, a ocenia ostatnie N minut.

anomaly_score to oryginalna miara z pracy Liu et al. (2008): s = 2^(−E[h]/c(n)),
zawsze w przedziale (0, 1]. Około 0.5 = typowa świeca, im bliżej 1, tym bardziej
nietypowa. W sklearn to po prostu −score_samples().

Typ anomalii wyznaczamy po cesze, która najbardziej odbiega od normy
(odporny z-score: (x − mediana) / (1.4826 × MAD) liczony na danych treningowych).

Jedno zdarzenie = jeden wiersz. Po gwałtownym skoku ceny kolejne kilkanaście świec też
odstaje od normy (cena daleko od SMA20, wysoki RSI), więc oznaczone świece leżące obok
siebie (przerwa ≤ EVENT_GAP_CANDLES) łączymy w zdarzenie i zapisujemy świecę ze
szczytowym score + początek, koniec i długość zdarzenia. Kluczem w ml.anomalies jest
(symbol, event_start_time), więc kolejne przebiegi DAG-a aktualizują to samo zdarzenie
(np. wydłużają je), zamiast dopisywać nowe. Zdarzenia zaczynające się na pierwszej
świecy okna pomijamy — zaczęły się wcześniej i zapisał je już poprzedni przebieg.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

from features import ANOMALY_FEATURE_COLUMNS

log = logging.getLogger("ml.anomaly")

# cecha → (typ gdy wartość powyżej normy, typ gdy poniżej)
ANOMALY_TYPE_BY_FEATURE = {
    "return_1": ("price_spike", "price_drop"),
    "log_volume_ratio_60": ("volume_spike", "volume_drop"),
    "bollinger_position": ("bollinger_breakout_up", "bollinger_breakout_down"),
    "rsi_deviation": ("rsi_overbought", "rsi_oversold"),
    "close_to_sma_20": ("trend_deviation_up", "trend_deviation_down"),
}


EVENT_GAP_CANDLES = 2


@dataclass
class SymbolAnomalyResult:
    symbol: str
    training_rows: int
    scored_rows: int
    anomalies: pd.DataFrame
    status: str


def _robust_scale(training_features: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    feature_medians = training_features.median()
    median_absolute_deviation = (training_features - feature_medians).abs().median() * 1.4826
    # gdy MAD = 0 (cecha prawie stała), bierzemy odchylenie standardowe albo 1
    fallback_scale = training_features.std().replace(0, 1.0).fillna(1.0)
    feature_scales = median_absolute_deviation.where(median_absolute_deviation > 0, fallback_scale)
    return feature_medians, feature_scales


def detect_symbol_anomalies(
    symbol: str,
    symbol_features: pd.DataFrame,
    scoring_window_minutes: int,
    contamination: float,
    minimum_training_rows: int,
    model_version: str,
    candle_minutes: int,
) -> SymbolAnomalyResult:
    complete_rows = symbol_features.dropna(subset=ANOMALY_FEATURE_COLUMNS).sort_values("candle_time")
    if len(complete_rows) < minimum_training_rows:
        return SymbolAnomalyResult(symbol, len(complete_rows), 0, pd.DataFrame(), "insufficient_data")

    newest_candle_time = complete_rows["candle_time"].max()
    scoring_start_time = newest_candle_time - pd.Timedelta(minutes=scoring_window_minutes)
    rows_to_score = complete_rows[complete_rows["candle_time"] > scoring_start_time]

    isolation_forest = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=42,
        n_jobs=2,
    )
    isolation_forest.fit(complete_rows[ANOMALY_FEATURE_COLUMNS])

    anomaly_scores = -isolation_forest.score_samples(rows_to_score[ANOMALY_FEATURE_COLUMNS])
    is_anomaly = isolation_forest.predict(rows_to_score[ANOMALY_FEATURE_COLUMNS]) == -1

    feature_medians, feature_scales = _robust_scale(complete_rows[ANOMALY_FEATURE_COLUMNS])
    robust_zscores = (rows_to_score[ANOMALY_FEATURE_COLUMNS] - feature_medians) / feature_scales
    dominant_features = robust_zscores.abs().idxmax(axis=1)
    dominant_zscores = np.array(
        [robust_zscores.at[row_index, feature] for row_index, feature in dominant_features.items()]
    )
    anomaly_types = [
        ANOMALY_TYPE_BY_FEATURE[feature][0 if zscore >= 0 else 1]
        for feature, zscore in zip(dominant_features, dominant_zscores)
    ]

    detected_at = pd.Timestamp.now(tz="UTC").tz_localize(None)
    scored = pd.DataFrame(
        {
            "symbol": symbol,
            "candle_time": rows_to_score["candle_time"].to_numpy(),
            "anomaly_score": anomaly_scores.astype(float),
            "anomaly_type": anomaly_types,
            "dominant_feature": dominant_features.to_numpy(),
            "dominant_feature_zscore": dominant_zscores.astype(float),
            "close": rows_to_score["close"].to_numpy(dtype=float),
            "volume": rows_to_score["volume"].to_numpy(dtype=float),
            "rsi_14": rows_to_score["rsi_14"].to_numpy(dtype=float),
            "model_version": model_version,
            "detected_at": detected_at,
        }
    )
    flagged_candles = scored[is_anomaly].sort_values("candle_time").reset_index(drop=True)
    anomaly_events = _group_into_events(
        flagged_candles, first_scored_candle_time=rows_to_score["candle_time"].min(), candle_minutes=candle_minutes
    )
    return SymbolAnomalyResult(symbol, len(complete_rows), len(rows_to_score), anomaly_events, "ok")


def _group_into_events(
    flagged_candles: pd.DataFrame, first_scored_candle_time: pd.Timestamp, candle_minutes: int
) -> pd.DataFrame:
    if flagged_candles.empty:
        return flagged_candles
    maximum_gap = pd.Timedelta(minutes=EVENT_GAP_CANDLES * candle_minutes)
    starts_new_event = flagged_candles["candle_time"].diff() > maximum_gap
    flagged_candles = flagged_candles.assign(event_number=starts_new_event.cumsum())

    event_rows = []
    for _, event_candles in flagged_candles.groupby("event_number"):
        event_start_time = event_candles["candle_time"].min()
        if event_start_time == first_scored_candle_time:
            continue  # zdarzenie ucięte przez początek okna — zapisane w poprzednim przebiegu
        peak_candle = event_candles.loc[event_candles["anomaly_score"].idxmax()].copy()
        peak_candle["event_start_time"] = event_start_time
        peak_candle["event_end_time"] = event_candles["candle_time"].max()
        peak_candle["event_candle_count"] = len(event_candles)
        event_rows.append(peak_candle)

    if not event_rows:
        return pd.DataFrame()
    anomaly_events = pd.DataFrame(event_rows).drop(columns=["event_number"]).reset_index(drop=True)
    anomaly_events["event_candle_count"] = anomaly_events["event_candle_count"].astype("uint16")
    for float_column in ["anomaly_score", "dominant_feature_zscore", "close", "volume", "rsi_14"]:
        anomaly_events[float_column] = anomaly_events[float_column].astype(float)
    for time_column in ["candle_time", "event_start_time", "event_end_time", "detected_at"]:
        anomaly_events[time_column] = pd.to_datetime(anomaly_events[time_column])
    return anomaly_events


def detect_anomalies(
    feature_frame: pd.DataFrame,
    scoring_window_minutes: int,
    contamination: float,
    minimum_training_rows: int,
    candle_minutes: int,
) -> tuple[str, list[SymbolAnomalyResult]]:
    model_version = "iforest-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results = []
    for symbol, symbol_features in feature_frame.groupby("symbol", sort=True):
        result = detect_symbol_anomalies(
            symbol, symbol_features, scoring_window_minutes, contamination, minimum_training_rows,
            model_version, candle_minutes,
        )
        log.info(
            "%s: %s, trening=%d, ocenione=%d, zdarzenia=%d",
            symbol, result.status, result.training_rows, result.scored_rows, len(result.anomalies),
        )
        results.append(result)
    return model_version, results
