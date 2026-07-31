FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/

# Roda como usuario sem privilegio (nao toca no lakehouse, so no Kafka).
RUN useradd --create-home --uid 10001 producer
USER 10001

CMD ["python", "-u", "src/producer.py"]
