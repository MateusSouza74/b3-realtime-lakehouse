FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY .streamlit/ /app/.streamlit/
COPY app.py /app/app.py

EXPOSE 8501

# Le as tabelas Delta direto do volume via delta-rs (nenhuma dependencia de Spark
# ou JVM na camada de apresentacao).
CMD ["streamlit", "run", "app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
