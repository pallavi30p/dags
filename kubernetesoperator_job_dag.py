"""
DAG: kubernetes_job_operator_validation

Purpose:
    Validate that the KubernetesJobOperator is enabled and functioning
    correctly in the Airflow deployment.

Test Coverage:
    - Verifies that KubernetesJobOperator is available.
    - Creates a Kubernetes Job.
    - Executes a simple command.
    - Waits for the Job to complete successfully.

Expected Result:
    The task should complete successfully and the Job logs should contain:
        KubernetesJobOperator validation successful!

Notes:
    - This DAG assumes Airflow has permissions to create Kubernetes Jobs.
    - Replace the image if your environment requires an approved image.
    - If KubernetesJobOperator is disabled, this DAG should fail during
      parsing or execution.
"""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator

with DAG(
    dag_id="kubernetes_job_operator_validation",
    description="Validation DAG for KubernetesJobOperator.",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["validation", "operator", "kubernetes", "job"],
    doc_md="""
# KubernetesJobOperator Validation

## Objective

Validate that KubernetesJobOperator is enabled and can successfully
create and execute a Kubernetes Job.

## Validation Steps

1. Create a Kubernetes Job.
2. Run a simple shell command.
3. Wait for Job completion.
4. Exit successfully.

## Expected Outcome

The task should succeed and the Job logs should contain:
KubernetesJobOperator validation successful!
""",
) as dag:

    validate_kubernetes_job = KubernetesJobOperator(
        task_id="validate_kubernetes_job",
        namespace="default",
        image="busybox:1.36",
        cmds=["/bin/sh", "-c"],
        arguments=[
            "echo 'KubernetesJobOperator validation successful!' && exit 0"
        ],
        name="kjo-validation",
        wait_until_job_complete=True,
        delete_on_status="Complete",
    )
