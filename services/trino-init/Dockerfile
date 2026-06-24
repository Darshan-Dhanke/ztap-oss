FROM python:3.12-slim

WORKDIR /app

COPY services/trino-init/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/trino-init/app ./app

CMD ["python", "-m", "app.register"]
