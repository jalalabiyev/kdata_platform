
#!/usr/bin/env python3
from datetime import datetime
import os, gzip, shutil, tempfile
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
local_tz = pendulum.timezone("Europe/Berlin")
def export_to_hdd(**ctx):
    # 1) HDD mount (container and  lokal)
    for m in ["/host_hdd", "/media/jako/datalake", "/media/jako/Bronzedisk"]:
        if os.path.isdir(m):
            mount = m
            break
    else:
        raise RuntimeError("HDD tapılmadı: /host_hdd və ya /media/jako/... yoxdur")

    # 2) Paths
    dt = (ctx["ds"] if "ds" in ctx else datetime.today().strftime("%Y-%m-%d"))
    bronze_root = os.path.join(mount, "datalakehouse", "bronze")
    out_dir = os.path.join(bronze_root, f"dt={dt}")
    os.makedirs(out_dir, exist_ok=True)
    out_gz = os.path.join(out_dir, f"kredit_stammdaten_{dt}_first100k.csv.gz")

    # 3) Postgres → CSV
    hook = PostgresHook(postgres_conn_id="bronze_pg")
    sql = f"""
    COPY (
      SELECT *
      FROM kredit_stammdaten
      WHERE erstellt_am >= DATE '{dt}'
        AND erstellt_am <  DATE '{dt}' + INTERVAL '1 day'
      ORDER BY erstellt_am ASC
      LIMIT 100000
    ) TO STDOUT WITH CSV HEADER
    """
    # CSV and  gzip -1 
    with tempfile.NamedTemporaryFile(delete=False,
    mode="wb") as tmp:
        tmp_path = tmp.name
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.copy_expert(sql, tmp)

    with open(tmp_path, "rb") as fin, gzip.open(out_gz, "wb", compresslevel=1) as fout:
        shutil.copyfileobj(fin, fout)
    os.remove(tmp_path)
    print(f"[OK] {out_gz}")

with DAG(
    dag_id="bronze_export_direct",
    start_date=datetime(2024, 1, 1),
    schedule_interval= "0 22 * * *",   # istəsən: "0 2 * * *"
    catchup=False,
) as dag:
    PythonOperator(
        task_id="export_kredit_stammdaten",
        python_callable=export_to_hdd,
    )
