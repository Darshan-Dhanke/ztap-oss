# Build context is the repo root (see docker-compose.yml) so we can install the
# shared type-engine package alongside the service.
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY packages/type-engine /opt/type-engine
RUN pip install --no-cache-dir /opt/type-engine

COPY services/sync/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/sync/app ./app

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
