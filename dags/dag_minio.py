"""
DAG Airflow — Upload d'un dossier local vers MinIO
====================================================
Ce DAG scanne récursivement un dossier local et uploade chaque fichier
dans un bucket MinIO, en conservant la structure des sous-dossiers.

Prérequis
---------
1. Installer les dépendances dans votre image Docker (Dockerfile) :
       pip install apache-airflow-providers-amazon minio

2. Créer une connexion Airflow "minio_default" (Admin → Connections) :
       Conn Type : Amazon Web Services  (ou "S3")
       Host      : http://minio:9000
       Login     : minio                   (MINIO_ROOT_USER)
       Password  : minio123               (MINIO_ROOT_PASSWORD)
       Extra     : {"endpoint_url": "http://minio:9000"}

3. Optionnel — passer les variables via les Airflow Variables
   (Admin → Variables) plutôt que de modifier les constantes ci-dessous.

Paramètres configurables
------------------------
SOURCE_FOLDER  : chemin absolu du dossier à uploader (dans le conteneur)
MINIO_BUCKET   : nom du bucket de destination
MINIO_PREFIX   : préfixe (sous-dossier) dans le bucket  (vide = racine)
CONN_ID        : identifiant de la connexion Airflow
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Paramètres — modifiez ici ou surchargez via les Airflow Variables
# ---------------------------------------------------------------------------
SOURCE_FOLDER: str = Variable.get("minio_source_folder", default_var="/opt/airflow/data/upload")
MINIO_BUCKET: str  = Variable.get("minio_bucket",        default_var="my-bucket")
MINIO_PREFIX: str  = Variable.get("minio_prefix",        default_var="")          # ex: "raw/2024/"
CONN_ID: str       = "minio_default"

# ---------------------------------------------------------------------------
# Fonctions métier
# ---------------------------------------------------------------------------

def _get_s3_hook():
    """Retourne un S3Hook configuré pour MinIO."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    return S3Hook(aws_conn_id=CONN_ID)


def create_bucket_if_missing(bucket: str, **_) -> None:
    """Crée le bucket MinIO s'il n'existe pas encore."""
    hook = _get_s3_hook()
    if not hook.check_for_bucket(bucket):
        hook.create_bucket(bucket_name=bucket)
        print(f"✅ Bucket créé : {bucket}")
    else:
        print(f"ℹ️  Bucket déjà existant : {bucket}")


def scan_folder(source_folder: str, ti, **_) -> list[str]:
    """
    Parcourt récursivement le dossier source et pousse la liste
    des chemins absolus dans XCom.
    """
    folder = Path(source_folder)
    if not folder.exists():
        raise FileNotFoundError(f"Le dossier source n'existe pas : {folder}")

    files = [str(p) for p in folder.rglob("*") if p.is_file()]

    if not files:
        print(f"⚠️  Aucun fichier trouvé dans {folder}")
    else:
        print(f"📂 {len(files)} fichier(s) trouvé(s) dans {folder}")

    ti.xcom_push(key="files", value=files)
    return files


def upload_files(source_folder: str, bucket: str, prefix: str, ti, **_) -> None:
    """
    Uploade chaque fichier détecté par scan_folder vers MinIO,
    en conservant l'arborescence relative au dossier source.
    """
    hook  = _get_s3_hook()
    files: list[str] = ti.xcom_pull(task_ids="scan_folder", key="files") or []

    if not files:
        print("⚠️  Aucun fichier à uploader.")
        return

    base = Path(source_folder)
    success, errors = 0, []

    for file_path in files:
        local_path  = Path(file_path)
        relative    = local_path.relative_to(base)          # structure conservée
        s3_key      = f"{prefix.rstrip('/')}/{relative}" if prefix else str(relative)
        s3_key      = s3_key.replace("\\", "/")             # compatibilité Windows

        try:
            hook.load_file(
                filename=str(local_path),
                key=s3_key,
                bucket_name=bucket,
                replace=True,
            )
            print(f"  ✅ {local_path.name}  →  s3://{bucket}/{s3_key}")
            success += 1
        except Exception as exc:  # noqa: BLE001
            print(f"  ❌ Erreur sur {local_path.name} : {exc}")
            errors.append(str(local_path))

    print(f"\n📊 Résumé : {success}/{len(files)} fichier(s) uploadé(s).")

    if errors:
        raise RuntimeError(
            f"{len(errors)} fichier(s) en erreur :\n" + "\n".join(errors)
        )


def verify_upload(bucket: str, prefix: str, ti, **_) -> None:
    """
    Vérifie que le nombre de fichiers dans MinIO correspond
    au nombre de fichiers scannés localement.
    """
    hook   = _get_s3_hook()
    files  = ti.xcom_pull(task_ids="scan_folder", key="files") or []
    listed = hook.list_keys(bucket_name=bucket, prefix=prefix or None) or []

    print(f"🔍 Fichiers locaux    : {len(files)}")
    print(f"🔍 Objets dans MinIO  : {len(listed)}")

    if len(listed) < len(files):
        raise ValueError(
            f"Incohérence détectée : {len(files)} fichiers uploadés "
            f"mais seulement {len(listed)} objets présents dans le bucket."
        )
    print("✅ Vérification OK.")


# ---------------------------------------------------------------------------
# Définition du DAG
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="hopital_csv_to_postgres",
    description="CSV patients : upload MinIO, nettoyage, chargement PostgreSQL (1 service -> N patients)",
    schedule_interval=None,          # déclenché manuellement (ou changez en cron)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["minio", "s3", "ingestion"],
    params={                          # paramètres surchargeables au déclenchement
        "source_folder": SOURCE_FOLDER,
        "bucket":        MINIO_BUCKET,
        "prefix":        MINIO_PREFIX,
    },
) as dag:

    t_create_bucket = PythonOperator(
        task_id="create_bucket_if_missing",
        python_callable=create_bucket_if_missing,
        op_kwargs={"bucket": "{{ params.bucket }}"},
    )

    t_scan = PythonOperator(
        task_id="scan_folder",
        python_callable=scan_folder,
        op_kwargs={"source_folder": "{{ params.source_folder }}"},
    )

    t_upload = PythonOperator(
        task_id="upload_files",
        python_callable=upload_files,
        op_kwargs={
            "source_folder": "{{ params.source_folder }}",
            "bucket":        "{{ params.bucket }}",
            "prefix":        "{{ params.prefix }}",
        },
    )

    t_verify = PythonOperator(
        task_id="verify_upload",
        python_callable=verify_upload,
        op_kwargs={
            "bucket": "{{ params.bucket }}",
            "prefix": "{{ params.prefix }}",
        },
    )

    # --- Tasks PostgreSQL (pipeline MinIO -> Postgres, option A) ---

    def fetch_and_clean_from_minio(bucket: str, prefix: str, **_) -> str:
        from clean_and_load import run_fetch_and_clean_from_minio

        return run_fetch_and_clean_from_minio(
            staging_dir="/opt/airflow/data/staging",
            bucket=bucket,
            prefix=prefix,
        )

    def load_to_postgres_task(ti, **_) -> None:
        from clean_and_load import run_load_to_postgres

        cleaned_path = ti.xcom_pull(task_ids="fetch_and_clean_from_minio")
        if not cleaned_path:
            raise ValueError("Aucun chemin CSV nettoyé reçu depuis fetch_and_clean_from_minio")
        run_load_to_postgres(cleaned_path)

    t_fetch_clean = PythonOperator(
        task_id="fetch_and_clean_from_minio",
        python_callable=fetch_and_clean_from_minio,
        op_kwargs={
            "bucket": "{{ params.bucket }}",
            "prefix": "{{ params.prefix }}",
        },
    )

    t_load_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres_task,
    )

    # Chaîne complète : MinIO (Amandine) -> PostgreSQL (Marina)
    t_create_bucket >> t_scan >> t_upload >> t_verify >> t_fetch_clean >> t_load_postgres