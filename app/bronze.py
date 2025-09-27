#!/usr/bin/env python3
"""
Bronze Pipeline - Crawl raw data và lưu vào MinIO (Bronze Layer)
"""

import requests
import time
import json
from datetime import datetime
import boto3
from io import BytesIO
import os

# ==================== CONFIG ====================
BASE_URL = "https://gateway.chotot.com/v1/public/ad-listing"
DETAIL_URL = "https://gateway.chotot.com/v1/public/ad-listing/{}"
HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_PAGES = 200
LIMIT = 20

# MinIO config
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET = "lakehouse"
BRONZE_PREFIX = "bronze/"

# File lưu các list_id đã crawl
SEEN_FILE = "list_ids.txt"


# ==================== MinIO Client ====================
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)


# ==================== FUNCTIONS ====================
def ensure_bucket(bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
    except Exception:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Đã tạo bucket '{bucket_name}'")


def load_seen_ids():
    if not os.path.exists(SEEN_FILE):
        return set()
    with open(SEEN_FILE, "r") as f:
        return set(line.strip() for line in f if line.strip())


def save_seen_ids(new_ids):
    with open(SEEN_FILE, "a") as f:
        for i in new_ids:
            f.write(str(i) + "\n")


def fetch_list(offset=0, limit=20):
    params = {"cg": 1000, "o": offset, "limit": limit}
    r = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=20)
    r.raise_for_status()
    return r.json().get("ads", [])


def fetch_detail(list_id, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(DETAIL_URL.format(list_id), headers=HEADERS, timeout=20)
            r.raise_for_status()
            data = r.json()
            ad = data.get("ad", {})

            # Thông tin cơ bản
            result = {
                "list_id": list_id,
                "title": ad.get("subject"),
                "price": ad.get("price_string"),
                "address": data.get("ad_params", {}).get("address", {}).get("value"),
                "images": ad.get("images", []),
            }

            # Các thông số trong parameters
            for p in data.get("parameters", []):
                result[p["label"]] = p["value"]

            return result
        except Exception as e:
            print(f"⚠️ Lỗi {list_id} (lần {attempt+1}/{retries}): {e}")
            time.sleep(2 ** attempt)
    return None


def run_bronze():
    ensure_bucket(BUCKET)
    seen_ids = load_seen_ids()
    new_ids = []
    all_details = []

    for p in range(1, MAX_PAGES + 1):
        offset = (p - 1) * LIMIT
        ads = fetch_list(offset, LIMIT)
        if not ads:
            break

        for ad in ads:
            list_id = str(ad["list_id"])
            if list_id in seen_ids:
                continue

            detail = fetch_detail(list_id)
            if detail:
                all_details.append(detail)
                new_ids.append(list_id)

        print(f"Page {p}: tổng {len(all_details)} records mới")
        time.sleep(0.2)

    if not all_details:
        print("Không có data mới")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    key = f"{BRONZE_PREFIX}crawl_{timestamp}.json"

    data_bytes = BytesIO(json.dumps(all_details, ensure_ascii=False).encode("utf-8"))
    s3.put_object(Bucket=BUCKET, Key=key, Body=data_bytes)

    save_seen_ids(new_ids)

    print(f"Đã lưu {len(all_details)} records mới vào MinIO: {key}")
    print(f"Cập nhật {len(new_ids)} ID vào {SEEN_FILE}")


if __name__ == "__main__":
    run_bronze()
