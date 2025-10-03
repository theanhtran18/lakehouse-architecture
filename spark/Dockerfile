FROM bitnami/spark:3.3

USER root

# Cài đặt wget để download JAR files
RUN apt-get update && \
    apt-get install -y wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Cài thêm Hadoop AWS để Spark đọc/ghi từ MinIO (S3 compatible)
RUN mkdir -p /opt/bitnami/spark/jars && \
    wget -O /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -O /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Copy config Hive-site nếu cần
COPY ./hive-site.xml /opt/bitnami/spark/conf/hive-site.xml

USER 1001
