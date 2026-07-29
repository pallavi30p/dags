"""
Test Kubernetes Cluster Connection from Airflow.

Test case:
    K8s connection from Airflow works.

Expected:
    Airflow authenticates to the Kubernetes cluster using the
    Kubernetes Cluster Connection and successfully creates,
    runs, logs, and deletes a temporary pod.

Prerequisite:
    Create an Airflow Connection:

        Connection ID:
            kubernetes_test

        Connection Type:
            Kubernetes Cluster Connection

        Extra:
            {
                "in_cluster": true
            }

    No Kubernetes Pod, Deployment, Service, Namespace, ConfigMap,
    Secret, or other Kubernetes resource needs to be created
    beforehand.

    The KubernetesPodOperator creates the temporary pod used
    by this test and deletes it after successful execution.
"""

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from pendulum import datetime


with DAG(
    dag_id="test_kubernetes_cluster_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test_kubernetes_connection = KubernetesPodOperator(
        task_id="test_kubernetes_connection",
        name="test-kubernetes-connection",
        image="docker.io/library/alpine:3.21",
        cmds=["sh", "-c"],
        arguments=[
            'echo "Successfully connected to Kubernetes cluster from Airflow"'
        ],
        kubernetes_conn_id="kubernetes_test",
        get_logs=True,
        is_delete_operator_pod=True,
    )
