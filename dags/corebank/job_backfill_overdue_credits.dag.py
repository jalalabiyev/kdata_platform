from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="corebank_backfill_overdue_credits",
    start_date=datetime(2025, 10, 25),
    schedule="0 23 * * *",
    catchup=False,
    tags=["corebank","kdata"],
) as dag:
    portfolio = BashOperator(
        task_id="portfolio_regen",
        bash_command="python /opt/airflow/kdata_platform/scripts/job_backfill_overdue_credits.py"
)
