"""
DAG hopital_csv_to_postgres — MinIO (fichiers bruts) -> nettoyage -> PostgreSQL

Les CSV bruts sont déposés dans MinIO au démarrage (service minio-init).
Ce DAG enchaîne : fetch -> clean -> load.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

STAGING_DIR = "/opt/airflow/data/staging"
MINIO_BUCKET = Variable.get("minio_bucket", default_var="hopital-data")
MINIO_PREFIX = Variable.get("minio_prefix", default_var="raw/")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def fetch_from_minio(bucket: str, prefix: str, **_) -> list[str]:
    from clean_and_load import run_download_from_minio

    return run_download_from_minio(STAGING_DIR, bucket, prefix)


def clean_patient_csv(ti, **_) -> str:
    from clean_and_load import run_clean_patient_csv

    file_paths = ti.xcom_pull(task_ids="fetch_from_minio")
    if not file_paths:
        raise ValueError("Aucun fichier reçu depuis fetch_from_minio")
    return run_clean_patient_csv(STAGING_DIR, file_paths)


def load_to_postgres(ti, **_) -> None:
    from clean_and_load import run_load_to_postgres

    cleaned_path = ti.xcom_pull(task_ids="clean_patient_csv")
    if not cleaned_path:
        raise ValueError("Aucun CSV nettoyé reçu depuis clean_patient_csv")
    run_load_to_postgres(cleaned_path)


with DAG(
    dag_id="hopital_csv_to_postgres",
    description="CSV patients dans MinIO : fetch, nettoyage, chargement PostgreSQL",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["hopital", "minio", "postgres"],
    params={
        "bucket": MINIO_BUCKET,
        "prefix": MINIO_PREFIX,
    },
) as dag:
    t_fetch = PythonOperator(
        task_id="fetch_from_minio",
        python_callable=fetch_from_minio,
        op_kwargs={
            "bucket": "{{ params.bucket }}",
            "prefix": "{{ params.prefix }}",
        },
    )

    t_clean = PythonOperator(
        task_id="clean_patient_csv",
        python_callable=clean_patient_csv,
    )

    t_load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    t_fetch >> t_clean >> t_load
