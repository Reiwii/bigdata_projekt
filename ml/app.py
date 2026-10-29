"""Serwis ML — placeholder do rozbudowy."""

from fastapi import FastAPI

app = FastAPI(title="ETL Koszela ML Service")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/predict")
def predict():
    # TODO: czytaj z gold.ohlc_1h + gold.vwap + gold.technical
    #       predykcja XGBoost -> zapis do ml.predictions
    return {"status": "not_implemented_yet"}


@app.post("/detect-anomalies")
def detect_anomalies():
    # TODO: czytaj z gold.technical
    #       Isolation Forest -> zapis do ml.anomalies
    return {"status": "not_implemented_yet"}
