from pyspark.sql import SparkSession

# Tạo SparkSession với config để kết nối MinIO (S3 compatible)
spark = SparkSession.builder \
    .appName("SparkMinIOTest") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Đọc dữ liệu từ MinIO bucket
df = spark.read.parquet("s3a://lakehouse/silver/date=2025-09-28/crawl_cleaned_20250928_001034.parquet")

print("===== DataFrame preview =====")
df.show()

spark.stop()
