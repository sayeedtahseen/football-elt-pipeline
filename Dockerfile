FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY elt/ ./elt
COPY dbt/ ./dbt

RUN cd dbt/football && dbt deps --profiles-dir .


ENV PYTHONUNBUFFERED=1 \
    GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/sa.json \
    DBT_PROFILES_DIR=/app/dbt/football

ENTRYPOINT ["python", "-m", "elt.run"]
