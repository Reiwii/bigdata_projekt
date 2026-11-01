FROM apache/airflow:2.9.3-python3.11
USER airflow
RUN pip install --no-cache-dir --user \
    "dbt-core==1.8.9" \
    "dbt-clickhouse==1.8.5"
