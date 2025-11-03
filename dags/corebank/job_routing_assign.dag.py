from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="corebank_routing_assign",
    start_date=datetime(2025, 10, 25),
    schedule="0 20 * * *",
    catchup=False,
    tags=["corebank","kdata"],
) as dag:
    portfolio = BashOperator(
        task_id="portfolio_regen",
        bash_command="python /opt/airflow/kdata_platform/scripts/job_routing_assign.py",
    )

