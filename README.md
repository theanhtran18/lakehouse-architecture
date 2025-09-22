# 🏠 Lakehouse Project – Real Estate Data

## 📌 Giới thiệu

Dự án này minh họa cách xây dựng **Data Lakehouse** cho phân tích dữ liệu bất động sản, sử dụng các thành phần chính:

- **MinIO**: Object Storage (S3-compatible) đóng vai trò Data Lake.
- **Apache Spark**: Compute engine để xử lý dữ liệu lớn.
- **Delta Lake + Hive Metastore**: Quản lý định dạng bảng và metadata.
- **dbt**: Thực hiện transformation, tạo các tầng dữ liệu Bronze → Silver → Gold.
- **Power BI**: Trực quan hóa dữ liệu phục vụ phân tích.

---

## ⚙️ Yêu cầu

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/)

---

## 🚀 Khởi chạy MinIO

### 1. Clone project

```bash
git clone https://github.com/theanhtran18/lakehouse-architecture.git
cd lakehouse-architecture
```

### 2. Chạy docker

```bash
docker compose up -d
or
docker-compose up -d
```

#### 2.1. Truy cập MinIO

Web Console: http://localhost:9001

User: minioadmin

Password: minioadmin

API Endpoint (S3): http://localhost:9000

### 3. Chạy spark

Spark Master UI: http://localhost:8080

### 4. Test kết nối

```bash
docker exec -it spark-master_v1 /bin/bash
spark-submit --master spark://spark-master:7077 /app/read_data_from_minIO.py
```

---

## 🐳 Các lệnh Docker hay dùng

### Container Management

#### Khởi động/Dừng container

```bash
# Khởi động container
docker start <container_name>

# Dừng container
docker stop <container_name>

# Khởi động lại container
docker restart <container_name>

# Dừng tất cả container đang chạy
docker stop $(docker ps -q)
```

#### Xóa container

```bash
# Xóa một container (phải dừng trước)
docker rm <container_name>

# Force xóa container đang chạy
docker rm -f <container_name>

# Xóa toàn bộ container (cả đang chạy)
docker rm -f $(docker ps -aq)
```

### Docker Compose

```bash
# Chạy với file mặc định (docker-compose.yml)
docker compose up -d

# Chạy với file tùy chỉnh
docker compose -f docker-compose.spark.yml up -d

# Chạy và rebuild image
docker compose up -d --build

# Chạy chỉ một service cụ thể
docker compose up -d spark-master
```

#### Truy cập container

```bash
# Vào bash của container
docker exec -it <container_name> /bin/bash

# Chạy lệnh trong container
docker exec <container_name> <command>

# Copy file từ container ra host
docker cp <container_name>:/path/to/file /host/path

# Copy file từ host vào container
docker cp /host/path <container_name>:/path/to/file
```
