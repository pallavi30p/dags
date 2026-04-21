from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="post_upgrade_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["upgrade_flow"],
) as dag:

    wait_for_upgrade = ExternalTaskSensor(
        task_id="wait_for_upgrade",
        external_dag_id="upgrade_dag",
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_delta=timedelta(days=0),
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
        deferrable=True,
    )

    post_checks = EmptyOperator(task_id="post_checks")

    wait_for_upgrade >> post_checks