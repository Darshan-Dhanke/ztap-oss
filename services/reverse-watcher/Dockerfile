FROM python:3.12-slim

WORKDIR /app

COPY services/reverse-watcher/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/reverse-watcher/app ./app

CMD ["python", "-m", "app.watcher"]
