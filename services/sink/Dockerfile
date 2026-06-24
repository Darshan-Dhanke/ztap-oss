FROM python:3.12-slim

WORKDIR /app

COPY services/sink/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/sink/app ./app

CMD ["python", "-m", "app.consumer"]
