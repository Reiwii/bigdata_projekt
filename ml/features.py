"""Budowa cech (features) z warstwy gold — wspólne dla predykcji i anomalii.

Wszystkie cechy są BEZWYMIAROWE (zwroty, stosunki, wskaźniki 0-100), dzięki czemu
jeden model predykcji może uczyć się naraz na BTC (~100 000 USDT) i XRP (~0.5 USDT).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

PRICE_FEATURE_COLUMNS = [
    "return_1",
    "return_5",
    "return_15",
    "return_60",
    "volatility_15",
    "volatility_60",
    "rsi_14",
    "close_to_sma_20",
    "close_to_ema_12",
    "ema_12_to_sma_20",
    "bollinger_width",
    "bollinger_position",
    "log_volume_ratio_60",
    "close_to_previous_hour_vwap",
    "previous_hour_return",
    "previous_hour_range",
    "hour_of_day_sin",
    "hour_of_day_cos",
]

# Do anomalii tylko cechy "chwilowe" — opisujące TĘ świecę. Cechy z okien kroczących
# (volatility_15, bollinger_width) po jednym skoku są podbite przez kolejne 15-20 świec
# i oznaczałyby jako anomalię całe "echo" zdarzenia zamiast samego zdarzenia.
ANOMALY_FEATURE_COLUMNS = [
    "return_1",
    "log_volume_ratio_60",
    "bollinger_position",
    "rsi_deviation",
    "close_to_sma_20",
]


def _regular_candle_grid(symbol_candles: pd.DataFrame, candle_minutes: int) -> pd.DataFrame:
    """Rozkłada świece jednego symbolu na regularną siatkę czasu.

    Dzięki temu przesunięcie o N wierszy = dokładnie N świec, nawet gdy w danych
    jest dziura (brakujące świece stają się NaN i odpadają przy dropna).
    """
    symbol_candles = symbol_candles.drop_duplicates("candle_time", keep="last").set_index("candle_time")
    full_time_range = pd.date_range(
        symbol_candles.index.min(), symbol_candles.index.max(), freq=f"{candle_minutes}min"
    )
    return symbol_candles.reindex(full_time_range).rename_axis("candle_time")


def _candle_based_features(grid: pd.DataFrame) -> pd.DataFrame:
    close_price = grid["close"]
    features = pd.DataFrame(index=grid.index)
    features["close"] = close_price
    features["volume"] = grid["volume"]

    features["return_1"] = close_price.pct_change(1, fill_method=None)
    features["return_5"] = close_price.pct_change(5, fill_method=None)
    features["return_15"] = close_price.pct_change(15, fill_method=None)
    features["return_60"] = close_price.pct_change(60, fill_method=None)
    features["volatility_15"] = features["return_1"].rolling(15, min_periods=10).std()
    features["volatility_60"] = features["return_1"].rolling(60, min_periods=40).std()

    features["rsi_14"] = grid["rsi_14"]
    features["rsi_deviation"] = grid["rsi_14"] - 50.0
    features["close_to_sma_20"] = close_price / grid["sma_20"] - 1.0
    features["close_to_ema_12"] = close_price / grid["ema_12"] - 1.0
    features["ema_12_to_sma_20"] = grid["ema_12"] / grid["sma_20"] - 1.0
    features["bollinger_width"] = grid["bollinger_width"]

    band_width = grid["bollinger_upper"] - grid["bollinger_lower"]
    features["bollinger_position"] = np.where(
        band_width > 0, (close_price - grid["bollinger_lower"]) / band_width, 0.5
    )

    average_volume_60 = grid["volume"].rolling(60, min_periods=20).mean()
    volume_ratio = grid["volume"] / average_volume_60.replace(0, np.nan)
    features["log_volume_ratio_60"] = np.log1p(volume_ratio)

    hour_fraction = grid.index.hour + grid.index.minute / 60.0
    features["hour_of_day_sin"] = np.sin(2 * np.pi * hour_fraction / 24.0)
    features["hour_of_day_cos"] = np.cos(2 * np.pi * hour_fraction / 24.0)
    return features.replace([np.inf, -np.inf], np.nan)


def build_feature_frame(
    candles: pd.DataFrame,
    hourly_context: pd.DataFrame | None,
    candle_minutes: int,
    forecast_horizons: tuple[int, ...] = (),
) -> pd.DataFrame:
    """Zwraca jedną ramkę: symbol, candle_time, close, volume, cechy, (opcjonalnie) cele predykcji.

    Cele: target_return_h{N} = close za N świec / close teraz − 1.
    Kontekst godzinowy bierzemy z POPRZEDNIEJ, zamkniętej godziny — bieżąca godzina
    w gold_vwap zawiera już przyszłe minuty, co byłoby wyciekiem danych w treningu.
    """
    if candles.empty:
        return pd.DataFrame()

    per_symbol_frames = []
    for symbol, symbol_candles in candles.groupby("symbol", sort=True):
        grid = _regular_candle_grid(symbol_candles, candle_minutes)
        symbol_features = _candle_based_features(grid)
        for horizon in forecast_horizons:
            symbol_features[f"target_return_h{horizon}"] = (
                symbol_features["close"].shift(-horizon) / symbol_features["close"] - 1.0
            )
        symbol_features["symbol"] = symbol
        per_symbol_frames.append(symbol_features.reset_index())

    feature_frame = pd.concat(per_symbol_frames, ignore_index=True)
    feature_frame = feature_frame[feature_frame["close"].notna()]  # wiersze-dziury z siatki

    feature_frame["previous_hour"] = feature_frame["candle_time"].dt.floor("h") - pd.Timedelta(hours=1)
    if hourly_context is not None and not hourly_context.empty:
        previous_hour_context = hourly_context.rename(columns={"hour": "previous_hour"})
        feature_frame = feature_frame.merge(
            previous_hour_context, on=["symbol", "previous_hour"], how="left"
        )
        feature_frame["close_to_previous_hour_vwap"] = feature_frame["close"] / feature_frame["hour_vwap"] - 1.0
        feature_frame["previous_hour_return"] = feature_frame["hour_close"] / feature_frame["hour_open"] - 1.0
        feature_frame["previous_hour_range"] = (
            (feature_frame["hour_high"] - feature_frame["hour_low"]) / feature_frame["hour_close"]
        )
        feature_frame = feature_frame.drop(
            columns=["hour_open", "hour_high", "hour_low", "hour_close", "hour_vwap"]
        )
    else:
        feature_frame["close_to_previous_hour_vwap"] = np.nan
        feature_frame["previous_hour_return"] = np.nan
        feature_frame["previous_hour_range"] = np.nan

    feature_frame = feature_frame.drop(columns=["previous_hour"]).replace([np.inf, -np.inf], np.nan)
    return feature_frame.sort_values(["symbol", "candle_time"]).reset_index(drop=True)
