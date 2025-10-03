FROM apache/airflow:2.7.2-python3.9

# Switch to root to create directories
USER root
# Tạo thư mục cho DAGs, plugins, logs
RUN mkdir -p /usr/local/airflow/dags /usr/local/airflow/logs /usr/local/airflow/plugins

# Switch back to airflow user để cài packages
USER airflow
# Cài thư viện cần cho crawl + Spark + MinIO + MLflow
RUN pip install --no-cache-dir \
    requests \
    beautifulsoup4 \
    pandas \
    pyspark==3.3.2 \
    boto3 \
    minio \
    mlflow \
    scikit-learn \
    xgboost

VOLUME ["/usr/local/airflow/dags", "/usr/local/airflow/logs", "/usr/local/airflow/plugins"]
