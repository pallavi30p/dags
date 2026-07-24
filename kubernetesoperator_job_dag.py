"""
DAG: kubernetes_job_operator_validation

Purpose:
    Validate that the KubernetesJobOperator is enabled and functioning
    correctly in the Airflow deployment.

Test Coverage:
    - Verifies that KubernetesJobOperator is available.
    - Creates a Kubernetes Job.
    - Executes a simple command inside the Job.
    - Waits for the Job to complete successfully.
    - Streams logs back to Airflow.

Expected Result:
    - The Job is created successfully.
    - The command executes successfully.
    - The Job completes with status Complete.
    - The task succeeds in Airflow.

Notes:
    - This DAG assumes Airflow has permissions to create Kubernetes Jobs.
    - Replace the image if your environment requires an approved image.
    - Intended only as a validation DAG.
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
    tags=["validation", "kubernetes", "job", "operator"],
    doc_md="""
# KubernetesJobOperator Validation

## Objective

Validate that **KubernetesJobOperator** is enabled and can successfully
create and execute a Kubernetes Job.

## Validation Steps

1. Create a Kubernetes Job.
2. Run a simple shell command inside the Job.
3. Print a validation message.
4. Wait for the Job to complete.
5. Return success to Airflow.

## Expected Outcome

The task should succeed and the Job logs should contain:

```
KubernetesJobOperator validation successful!
```
""",
) as dag:

    validate_kubernetes_job = KubernetesJobOperator(
        task_id="validate_kubernetes_job",
        namespace="default",
        name="kjo-validation",
        image="busybox:1.36",
        cmds=["/bin/sh", "-c"],
        arguments=[
            "echo 'KubernetesJobOperator validation successful!' && exit 0"
        ],
        wait_until_job_complete=True,
        get_logs=True,
    )
