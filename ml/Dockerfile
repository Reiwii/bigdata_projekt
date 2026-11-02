FROM python:3.12-slim

# libgomp1 — OpenMP, wymagane przez XGBoost i scikit-learn do liczenia na wielu rdzeniach
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./

ENV PYTHONUNBUFFERED=1 \
    MODEL_DIRECTORY=/models
VOLUME /models
EXPOSE 8000

# obraz slim nie ma curl — healthcheck w czystym Pythonie
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health', timeout=4).status == 200 else 1)"

# JEDEN worker celowo: model i blokady trzymane są w pamięci procesu
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
