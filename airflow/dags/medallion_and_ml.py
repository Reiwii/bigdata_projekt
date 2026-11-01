"""
DAG: medallion_and_ml
---------------------
Nieliniowy pipeline: bronze_check → 3× silver (równoległe) → 3× gold (równoległe) → ML.
Fan-out po bronze, fan-in przed ML predict.

Schedule: co 15 minut.
"""

from __future__ import annotations

import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

# ---------------------------------------------------------------------------
# Konfiguracja
# ---------------------------------------------------------------------------
DBT_DIR = "/opt/airflow/dbt"
DBT_CMD = f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_DIR}"

CLICKHOUSE_URL = "http://vm2:8123"
ML_CONN_ID = "ml_service"   # http://vm4:8000, zdefiniowane przez env var

default_args = {
    "owner": "etl-koszela",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="medallion_and_ml",
    description="Kafka → bronze → silver (fan-out) → gold → ML (fan-in)",
    default_args=default_args,
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "dbt", "ml"],
) as dag:

    # -----------------------------------------------------------------------
    # 0. Bronze check — czy w ostatnich 30 min są świeże dane
    # -----------------------------------------------------------------------
    bronze_check = BashOperator(
        task_id="bronze_check",
        bash_command=(
            f"COUNT=$(curl -s '{CLICKHOUSE_URL}/?query="
            f"SELECT%20count()%20FROM%20bronze.raw_klines%20"
            f"WHERE%20fetched_at%20%3E%20now()%20-%20INTERVAL%2030%20MINUTE'); "
            f"echo \"Świeżych rekordów: $COUNT\"; "
            f"test \"$COUNT\" -gt 0"
        ),
    )

    # -----------------------------------------------------------------------
    # 1. SILVER — 3 modele równolegle (fan-out)
    # -----------------------------------------------------------------------
    silver_prices = BashOperator(
        task_id="silver_prices",
        bash_command=f"{DBT_CMD} --select silver_prices",
    )

    silver_volumes = BashOperator(
        task_id="silver_volumes",
        bash_command=f"{DBT_CMD} --select silver_volumes",
    )

    silver_indicators = BashOperator(
        task_id="silver_indicators",
        bash_command=f"{DBT_CMD} --select silver_indicators",
    )

    # -----------------------------------------------------------------------
    # 2. GOLD — 3 modele równolegle (ale z różnymi zależnościami)
    # -----------------------------------------------------------------------
    gold_ohlc = BashOperator(
        task_id="gold_ohlc_1h",
        bash_command=f"{DBT_CMD} --select gold_ohlc_1h",
    )

    gold_vwap = BashOperator(
        task_id="gold_vwap",
        bash_command=f"{DBT_CMD} --select gold_vwap",
    )

    gold_technical = BashOperator(
        task_id="gold_technical",
        bash_command=f"{DBT_CMD} --select gold_technical",
    )

    # -----------------------------------------------------------------------
    # 3. ML — wywołanie serwisu na vm4
    # -----------------------------------------------------------------------
    ml_anomaly = SimpleHttpOperator(
        task_id="ml_anomaly",
        http_conn_id=ML_CONN_ID,
        endpoint="/detect-anomalies",
        method="POST",
        headers={"Content-Type": "application/json"},
        data='{}',
        response_check=lambda resp: resp.status_code == 200,
        log_response=True,
    )

    ml_predict = SimpleHttpOperator(
        task_id="ml_predict",
        http_conn_id=ML_CONN_ID,
        endpoint="/predict",
        method="POST",
        headers={"Content-Type": "application/json"},
        data='{}',
        response_check=lambda resp: resp.status_code == 200,
        log_response=True,
    )

    # -----------------------------------------------------------------------
    # 4. ZALEŻNOŚCI — nieliniowy DAG (fan-out / fan-in)
    # -----------------------------------------------------------------------

    # Bronze → 3× Silver (fan-out)
    bronze_check >> [silver_prices, silver_volumes, silver_indicators]

    # Silver → Gold (każdy gold ma swoje zależności)
    silver_prices                    >> gold_ohlc
    [silver_prices, silver_volumes]  >> gold_vwap      # VWAP łączy ceny + wolumeny
    silver_indicators                >> gold_technical

    # Gold → ML (fan-in)
    gold_technical                              >> ml_anomaly   # anomalie tylko z technical
    [gold_ohlc, gold_vwap, gold_technical]      >> ml_predict   # predykcja potrzebuje wszystkiego
