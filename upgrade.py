from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState


with DAG(
    dag_id="upgrade_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    wait_for_pre_upgrade = ExternalTaskSensor(
        task_id="wait_for_pre_upgrade",
        external_dag_id="pre_upgrade_dag",
        external_task_id=None,

        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],

        execution_delta=timedelta(days=0),

        mode="reschedule",
        deferrable=True,
        poke_interval=60,
        timeout=3600,
    )

    upgrade_step = EmptyOperator(task_id="upgrade_step")

    wait_for_pre_upgrade >> upgrade_step
