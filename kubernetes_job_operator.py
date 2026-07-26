"""
Example DAG that runs a sleep command in a custom container via KubernetesJobOperator.

KubernetesJobOperator is disabled by default; remove it from
``airflow.extraConfig.disabledOperators``
"""

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.sdk import Param
from pendulum import datetime

with DAG(
    dag_id="kubernetes_job_operator",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "sleep_seconds": Param(30, type="integer", minimum=1),
        "image": Param(
            "docker.io/library/alpine:3.21",
            type="string",
            description="Custom container image used to run the sleep command",
        ),
    },
) as dag:
    KubernetesJobOperator(
        task_id="sleep_in_custom_image",
        name="sleep-in-custom-image",
        image="{{ params.image }}",
        cmds=["sleep"],
        arguments=["{{ params.sleep_seconds }}"],
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        wait_until_job_complete=True,
    ) 
