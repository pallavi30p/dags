from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="upgrade_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["upgrade_flow"],
) as dag:

    wait_for_pre_upgrade = ExternalTaskSensor(
        task_id="wait_for_pre_upgrade",
        external_dag_id="pre_upgrade_dag",
        external_task_id=None,  # wait for full DAG
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_delta=timedelta(days=0),  # SAME schedule alignment
        poke_interval=60,
        timeout=60 * 60,
        mode="reschedule",
        deferrable=True,
    )

    upgrade_step = EmptyOperator(task_id="upgrade_step")

    wait_for_pre_upgrade >> upgrade_step