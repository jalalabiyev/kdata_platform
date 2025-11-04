#!/usr/bin/env python3
import os
import pendulum
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


# config
LOCAL_TZ = pendulum.timezone("Europe/Berlin")
BUCKET = "kdata-silver-jalal"              #AWS S3 bucket
CONN_ID = "S3"                         # Airflow Connections
PREFIX = ""               # S3 folder name
# -----


def bronze_to_s3(**ctx):
    """Bronze CSV.GZ -> Parquet -> S3 upload"""
    ds = ctx.get("ds", datetime.today().strftime("%Y-%m-%d"))

    # HDD
    mounts = ["/host_hdd", "/media/jako/datalake", "/media/jako/Bronzedisk"]
    mount = next((m for m in mounts if os.path.isdir(m)), None)
    if not mount:
        raise RuntimeError("❌ HDD : /host_hdd və ya /media/jako/datalake keine")

    bronze_dir = f"{mount}/datalakehouse/bronze/dt={ds}"
    silver_dir = f"{mount}/datalakehouse/silver/dt={ds}"
    os.makedirs(silver_dir, exist_ok=True)

    in_csv = f"{bronze_dir}/kredit_stammdaten_{ds}_first100k.csv.gz"
    out_pq = f"{silver_dir}/kredit_stammdaten_{ds}_first100k.parquet"

    #CSV -> DataFrame
    print(f"[INFO] Oxunur: {in_csv}")
    df = pd.read_csv(in_csv, low_memory=False)

    #DataFrame -> Parquet
    df.to_parquet(out_pq, index=False)
    print(f"[OK] Silver Parquet created: {out_pq}")

    #Parquet -> S3 upload
    s3_key = f"{PREFIX}/dt={ds}/kredit_stammdaten_{ds}_first100k.parquet"
    hook = S3Hook(aws_conn_id=CONN_ID)
    hook.load_file(filename=out_pq, key=s3_key, bucket_name=BUCKET, replace=True)
    print(f"[S3] Yükləndi: s3://{BUCKET}/{s3_key}")


   #dag
with DAG(
    dag_id="silver_parquet_upload_s3_daily",
    start_date=datetime(2024, 1, 1, tzinfo=LOCAL_TZ),
    schedule_interval="10 22 * * *",   # taglisch 22:10
    catchup=False,
    tags=["bronze", "silver", "s3"],
) as dag:
    upload_task = PythonOperator(
        task_id="convert_and_upload_silver",
        python_callable=bronze_to_s3,
    )
