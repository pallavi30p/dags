from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="pre_upgrade_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["upgrade_flow"],
) as dag:

    start = EmptyOperator(task_id="start")

    precheck = EmptyOperator(task_id="precheck")

    end = EmptyOperator(task_id="end")

    start >> precheck >> end