FROM python:3.9-slim

WORKDIR /app

# Gerekli paketleri yükle
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install mlflow>=2.3.0 psycopg2-binary>=2.9.3

# Uygulama kodunu kopyala
COPY . .

# MLflow için port
EXPOSE 5000

# Çalıştırma komutu
CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000"]