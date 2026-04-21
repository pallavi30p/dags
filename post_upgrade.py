from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState


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
        external_task_id=None,  # wait for full DAG run

        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],

        execution_delta=timedelta(days=0),

        mode="reschedule",
        deferrable=True,
        poke_interval=60,
        timeout=3600,
    )

    post_checks = EmptyOperator(task_id="post_checks")

    wait_for_upgrade >> post_checks
