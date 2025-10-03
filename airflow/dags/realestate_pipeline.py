"""
Airflow DAG: realestate_lakehouse_pipeline

Pipeline:
 - ingest_local_to_bronze: move files from /usr/local/airflow/datasets -> minio://realestate/bronze/
 - bronze_to_silver: Spark job: read raw (json/csv) -> clean -> parquet -> minio://realestate/silver/
 - silver_to_gold: Spark job: feature engineering -> parquet -> minio://realestate/gold/
 - train_and_log: read gold -> train RandomForest -> log to MLflow -> upload model to MinIO
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import glob
import logging
import tempfile
from minio import Minio
import json
import pandas as pd
import joblib

# ML libs
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import mlflow
import mlflow.sklearn

# Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# ---------- Config from env ----------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY", "minio"))
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", os.getenv("MINIO_SECRET_KEY", "minio123"))
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "realestate")

# MLflow tracking uri (service name used inside docker network)
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")

# Local datasets directory inside Airflow container (mounted)
LOCAL_DATASETS_DIR = os.getenv("LOCAL_DATASETS_DIR", "/usr/local/airflow/datasets")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# ---------- Helpers ----------
def get_minio_client():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    return client

def ensure_bucket():
    client = get_minio_client()
    found = client.bucket_exists(MINIO_BUCKET)
    if not found:
        client.make_bucket(MINIO_BUCKET)
        logging.info("Created bucket: %s", MINIO_BUCKET)
    else:
        logging.info("Bucket exists: %s", MINIO_BUCKET)

def create_spark_session(app_name="realestate-spark-job"):
    # create SparkSession with S3A (MinIO) configs
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    return spark

# ---------- Task functions ----------
def ingest_local_to_bronze(**context):
    """
    Move any files in LOCAL_DATASETS_DIR to MinIO bronze folder.
    Supports .json and .csv and .ndjson
    """
    ensure_bucket()
    client = get_minio_client()
    patterns = ["*.json", "*.ndjson", "*.csv"]
    files_found = []
    for pat in patterns:
        files_found.extend(glob.glob(os.path.join(LOCAL_DATASETS_DIR, pat)))

    if not files_found:
        logging.info("No files found in %s to ingest.", LOCAL_DATASETS_DIR)
        return

    for fpath in files_found:
        fname = os.path.basename(fpath)
        object_name = f"bronze/{fname}"
        # upload file
        client.fput_object(MINIO_BUCKET, object_name, fpath)
        logging.info("Uploaded %s -> s3://%s/%s", fpath, MINIO_BUCKET, object_name)
        # optional: move to processed folder locally
        try:
            processed_dir = os.path.join(LOCAL_DATASETS_DIR, "ingested")
            os.makedirs(processed_dir, exist_ok=True)
            os.replace(fpath, os.path.join(processed_dir, fname))
            logging.info("Moved %s -> %s", fpath, processed_dir)
        except Exception as e:
            logging.warning("Could not move local file after upload: %s", e)

def bronze_to_silver(**context):
    """
    Read raw files from minio/bronze -> parse -> basic cleaning -> write parquet to minio/silver/
    Assumes raw files can be read by Spark (json or csv).
    """
    spark = create_spark_session("bronze_to_silver")
    try:
        # List objects in bronze
        # We'll read a wildcard path to handle multiple files
        bronze_path = f"s3a://{MINIO_BUCKET}/bronze/*"
        logging.info("Reading bronze from %s", bronze_path)
        # Try JSON first; if fails, try CSV
        try:
            df = spark.read.option("multiLine", "true").json(bronze_path)
        except Exception as e_json:
            logging.info("Read JSON failed (%s), trying CSV", e_json)
            try:
                df = spark.read.option("header", "true").csv(bronze_path)
            except Exception as e_csv:
                logging.error("Failed to read bronze as JSON or CSV: %s / %s", e_json, e_csv)
                raise

        # Basic cleaning: drop null id and price, cast numeric columns
        # Try common fields: id, location, area, bedrooms, price
        # If columns missing, create them with nulls
        cols = df.columns
        for c in ["id", "location", "area", "bedrooms", "price"]:
            if c not in cols:
                df = df.withColumn(c, col(c))  # creates column null if not exist (no-op attempt)
        # drop rows without price or area
        df_clean = df.dropna(subset=["price", "area"])
        df_clean = df_clean.dropDuplicates()

        # ensure correct types
        df_clean = df_clean.withColumn("area", col("area").cast("double")) \
                           .withColumn("bedrooms", col("bedrooms").cast("int")) \
                           .withColumn("price", col("price").cast("double"))

        silver_path = f"s3a://{MINIO_BUCKET}/silver/realestate_clean.parquet"
        logging.info("Writing cleaned parquet to %s", silver_path)
        df_clean.write.mode("overwrite").parquet(silver_path)
        logging.info("bronze_to_silver finished.")
    finally:
        spark.stop()

def silver_to_gold(**context):
    """
    Feature engineering: read silver parquet -> add encoded features -> write gold parquet
    Example: encode 'location' to 'location_encoded', add 'price_per_m2'
    """
    spark = create_spark_session("silver_to_gold")
    try:
        silver_path = f"s3a://{MINIO_BUCKET}/silver/realestate_clean.parquet"
        logging.info("Reading silver from %s", silver_path)
        df = spark.read.parquet(silver_path)

        # Example encoding: HCM -> 1, HN -> 2, else 0 (simple mapping)
        df = df.withColumn(
            "location_encoded",
            when(col("location") == "HCM", 2)
            .when(col("location") == "HN", 1)
            .otherwise(0)
        )

        # price_per_m2
        df = df.withColumn("price_per_m2", col("price") / col("area"))

        gold_path = f"s3a://{MINIO_BUCKET}/gold/training_dataset.parquet"
        logging.info("Writing gold dataset to %s", gold_path)
        df.write.mode("overwrite").parquet(gold_path)
        logging.info("silver_to_gold finished.")
    finally:
        spark.stop()

def train_and_log(**context):
    """
    Train a scikit-learn RandomForest on gold dataset, log to MLflow, upload model to MinIO
    """
    # configure mlflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    EXPERIMENT_NAME = "realestate-price-prediction"
    mlflow.set_experiment(EXPERIMENT_NAME)

    # Read gold parquet via Spark to Pandas (or use s3 client to download parquet)
    spark = create_spark_session("train_and_log")
    try:
        gold_path = f"s3a://{MINIO_BUCKET}/gold/training_dataset.parquet"
        logging.info("Reading gold from %s", gold_path)
        df_spark = spark.read.parquet(gold_path)
        df = df_spark.toPandas()
    finally:
        spark.stop()

    if df.empty:
        logging.warning("Gold dataset is empty; skipping training.")
        return

    # Prepare features/target (use simple set)
    # Ensure columns exist
    for c in ["area", "bedrooms", "location_encoded", "price"]:
        if c not in df.columns:
            df[c] = 0

    X = df[["area", "bedrooms", "location_encoded"]].fillna(0)
    y = df["price"].fillna(0)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    score = model.score(X_test, y_test)
    logging.info("Model trained. Test score: %s", score)

    # Log to mlflow
    with mlflow.start_run():
        mlflow.log_param("n_estimators", 100)
        mlflow.log_metric("r2_score", float(score))
        mlflow.sklearn.log_model(model, "model")

        # also save a local copy and upload to MinIO (models directory)
        with tempfile.TemporaryDirectory() as td:
            model_path = os.path.join(td, "model.pkl")
            joblib.dump(model, model_path)
            client = get_minio_client()
            object_name = "models/model.pkl"
            client.fput_object(MINIO_BUCKET, object_name, model_path)
            logging.info("Uploaded model to s3://%s/%s", MINIO_BUCKET, object_name)

# ---------- DAG ----------
with DAG(
    dag_id="realestate_lakehouse_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["realestate", "lakehouse", "ml"],
) as dag:

    t1 = PythonOperator(
        task_id="ingest_local_to_bronze",
        python_callable=ingest_local_to_bronze,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=bronze_to_silver,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="silver_to_gold",
        python_callable=silver_to_gold,
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id="train_and_log",
        python_callable=train_and_log,
        provide_context=True,
    )

    t1 >> t2 >> t3 >> t4
