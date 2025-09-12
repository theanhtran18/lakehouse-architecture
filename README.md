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
git clone https://github.com/<your-repo>/.git
cd lakehouse-real-estate
```
