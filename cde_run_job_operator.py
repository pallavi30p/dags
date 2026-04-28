from airflow import DAG
from datetime import datetime
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="cde_job_trigger_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["cde"],
) as dag:

    run_cde_job = CdeRunJobOperator(
        task_id="run_cde_job",
        connection_id="cde_operator",
        job_name="vc-admin-job",
        wait=True,
        timeout=3600,
    )

    run_cde_job
