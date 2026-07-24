"""
DAG: kubernetes_pod_operator_validation

Purpose:
    Validate that the KubernetesPodOperator is enabled and functioning
    correctly in the Airflow deployment.

Test Coverage:
    - Verifies that KubernetesPodOperator is available.
    - Launches a Kubernetes Pod.
    - Executes a simple command in the Pod.
    - Verifies successful completion.

Expected Result:
    The task should succeed and the pod logs should contain:
        KubernetesPodOperator validation successful!

Notes:
    - This DAG assumes the Airflow deployment has access to a Kubernetes
      cluster.
    - Replace the image if your environment requires an approved image.
    - If KubernetesPodOperator is disabled, this DAG should fail during
      parsing or execution.
"""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
    dag_id="kubernetes_pod_operator_validation",
    description="Validation DAG for KubernetesPodOperator.",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["validation", "operator", "kubernetes", "pod"],
    doc_md="""
# KubernetesPodOperator Validation

## Objective

Validate that KubernetesPodOperator is enabled and can successfully
launch a Kubernetes Pod.

## Validation Steps

1. Create a Kubernetes Pod.
2. Execute a simple shell command.
3. Print a validation message.
4. Exit successfully.

## Expected Outcome

The task should succeed and the pod logs should contain:
KubernetesPodOperator validation successful!
""",
) as dag:

    validate_kubernetes_pod = KubernetesPodOperator(
        task_id="validate_kubernetes_pod",
        name="kpo-validation",
        namespace="default",
        image="busybox:1.36",
        cmds=["/bin/sh", "-c"],
        arguments=[
            "echo 'KubernetesPodOperator validation successful!' && exit 0"
        ],
        get_logs=True,
        is_delete_operator_pod=True,
    )
