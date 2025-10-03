FROM python:3.9-slim

WORKDIR /mlflow

# Cài MLflow + boto3 để kết nối MinIO
RUN pip install --no-cache-dir mlflow boto3 pymysql

# Tạo thư mục lưu model local
RUN mkdir -p /mlflow

EXPOSE 5000

CMD ["mlflow", "server", \
     "--backend-store-uri", "sqlite:///mlflow.db", \
     "--default-artifact-root", "s3://mlflow/", \
     "--host", "0.0.0.0", \
     "--port", "5000"]
