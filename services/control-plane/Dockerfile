# Build context is the repo root (see docker-compose.yml) so we can install the
# shared type-engine package alongside the service.
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Install the shared type-engine (custom component #4) first for layer caching.
COPY packages/type-engine /opt/type-engine
RUN pip install --no-cache-dir /opt/type-engine

COPY services/control-plane/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/control-plane/app ./app

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
