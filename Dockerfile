FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends gcc librdkafka-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pg_to_kafka.py .
ENV PYTHONUNBUFFERED=1
USER 1000:1000
CMD ["python", "pg_to_kafka.py"]
