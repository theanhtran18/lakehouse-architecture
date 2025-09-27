#!/usr/bin/env python3
"""
Silver Pipeline - Transform từ Bronze và lưu vào MinIO (Silver Layer) - PySpark + Parquet
"""

import re
import os
import json
import boto3
from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import DoubleType, IntegerType, StringType

# ==================== CONFIG ====================
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET = "lakehouse"
BRONZE_PREFIX = "bronze/"
PROCESSED_PREFIX = "bronze/processed/"
SILVER_PREFIX = "silver/"
PROCESS_ALL = True

# ==================== MinIO Client ====================
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

# Auto-create bucket nếu chưa tồn tại
existing_buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
if BUCKET not in existing_buckets:
    s3.create_bucket(Bucket=BUCKET)
    print(f"Created bucket: {BUCKET}")


# ==================== HELPERS ====================
def parse_area(value):
    if value is None or str(value).strip() == "":
        return None
    import re
    try:
        nums = re.findall(r'[\d,.]+', str(value))
        if nums:
            return float(nums[0].replace(",", ""))
    except:
        return None
    return None


def parse_number(value):
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(float(str(value)))
    except:
        return None


def normalize_price(value):
    if value is None or str(value).strip() == "":
        return None
    s = str(value).lower()
    import re
    try:
        if "tỷ" in s:
            nums = re.findall(r'[\d.]+', s)
            if nums:
                return float(nums[0])
        elif "triệu" in s:
            nums = re.findall(r'[\d.]+', s)
            if nums:
                return float(nums[0]) / 1000
        else:
            price_str = re.sub(r"[^\d]", "", str(value))
            if price_str:
                return int(price_str) / 1e9
    except:
        return None
    return None


# Register UDFs cho Spark
parse_area_udf = udf(parse_area, DoubleType())
parse_number_udf = udf(parse_number, IntegerType())
normalize_price_udf = udf(normalize_price, DoubleType())


# ==================== MAIN ====================
def run_silver():
    spark = SparkSession.builder \
    .appName("SilverPipeline") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.375") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()


    # Lấy danh sách file chưa xử lý trong Bronze
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=BRONZE_PREFIX)
    objs = resp.get("Contents", []) if resp else []
    objs = [o for o in objs if not o["Key"].startswith(PROCESSED_PREFIX) and o["Key"].endswith(".json")]

    if not objs:
        print("Không tìm thấy dữ liệu Bronze chưa xử lý")
        return

    to_process = sorted(objs, key=lambda x: x["LastModified"])
    if not PROCESS_ALL:
        to_process = [to_process[-1]]

    for obj in to_process:
        key = obj["Key"]
        print(f"Xử lý file: {key}")

        raw_bytes = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        try:
            data = json.loads(raw_bytes.decode("utf-8"))
            if isinstance(data, dict):
                data = [data]
        except Exception as e:
            print(f"Không parse được JSON từ {key}: {e}")
            move_to_processed(key)
            continue

        if not data:
            print(f"File rỗng: {key}")
            move_to_processed(key)
            continue

        # Load vào Spark DataFrame
        df = spark.read.json(spark.sparkContext.parallelize([json.dumps(r, ensure_ascii=False) for r in data]))
        print(f"ℹSố record raw: {df.count()}")

        # Chuẩn hóa schema
        df_standard = df.select(
            col("address").alias("Address"),
            col("Diện tích đất").alias("Area"),
            col("Chiều ngang").alias("Frontage"),
            col("Đặc điểm nhà/đất").alias("Access Road"),
            col("Hướng cửa chính").alias("House Direction"),
            col("Tổng số tầng").alias("Floors"),
            col("Số phòng ngủ").alias("Bedrooms"),
            col("Số phòng vệ sinh").alias("Bathrooms"),
            col("Giấy tờ pháp lý").alias("Legal Status"),
            col("Tình trạng nội thất").alias("Furniture State"),
            col("price").alias("Price")
        )


        print(f"ℹSau transform: {df_standard.count()} records")

        # Lưu Parquet
        # Lấy timestamp từ tên file bronze (crawl_YYYYMMDD_HHMMSS.json)
        filename = os.path.basename(key)  # crawl_20250927_213859.json
        timestamp = filename.split("_")[1]  # 20250927
        date_fmt = f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:]}"  # 2025-09-27

        # Tạo key dạng partitioned
        silver_key = key.replace(BRONZE_PREFIX, f"{SILVER_PREFIX}/date={date_fmt}/") \
              .replace(".json", ".parquet") \
              .replace("crawl_", "crawl_cleaned_")

        silver_path = f"s3a://{BUCKET}/{silver_key}"

        df_standard.write.mode("overwrite").parquet(silver_path)
        print(f"Đã lưu Silver (Parquet) -> {silver_key}")

        # Move file từ bronze -> bronze/processed
        move_to_processed(key)


def move_to_processed(key):
    try:
        processed_key = key.replace(BRONZE_PREFIX, PROCESSED_PREFIX)
        s3.copy_object(Bucket=BUCKET, CopySource={"Bucket": BUCKET, "Key": key}, Key=processed_key)
        s3.delete_object(Bucket=BUCKET, Key=key)
        print(f"Đã move {key} -> {processed_key}")
    except Exception as e:
        print(f"Lỗi khi move_to_processed: {e}")


if __name__ == "__main__":
    run_silver()
