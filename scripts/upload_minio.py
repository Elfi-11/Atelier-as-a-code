#!/usr/bin/env python3
"""Upload des CSV bruts vers MinIO (bucket hopital-data/raw/)."""
from __future__ import annotations

import glob
import os
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parent.parent
ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minio")
SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minio123")
BUCKET = os.environ.get("MINIO_BUCKET", "hopital-data")


def main() -> None:
    client = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
    )

    try:
        client.head_bucket(Bucket=BUCKET)
    except Exception:
        client.create_bucket(Bucket=BUCKET)

    candidates = []
    for folder in [ROOT, ROOT / "data", Path("/opt/data")]:
        candidates.extend(glob.glob(str(folder / "patients_*.csv")))
    for path in sorted(set(candidates)):
        key = f"raw/{Path(path).name}"
        client.upload_file(path, BUCKET, key)
        print(f"  -> s3://{BUCKET}/{key}")


if __name__ == "__main__":
    main()
