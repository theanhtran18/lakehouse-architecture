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
df = spark.read.option("header", "true").csv("s3a://test/vietnam_housing_dataset.csv")

print("===== DataFrame preview =====")
df.show()

# Ghi kết quả lại vào MinIO dưới dạng Parquet
df.write.mode("overwrite").parquet("s3a://test/output_parquet")

spark.stop()
