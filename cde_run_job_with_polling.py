from airflow import DAG
from airflow.models import Variable
from datetime import datetime

from cloudera.airflow.providers.operators.cde import CdeRunJobOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
}

# Fetch the CDE job name from Airflow Variables
# Create this variable in Airflow UI:
# Key: cde_job_name
# Value: your-cde-job-name
CDE_JOB_NAME = Variable.get("cde_job_name")

with DAG(
    dag_id="cde_job_trigger_with_variable_polling",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["cde", "variable"],
) as dag:

    run_cde_job = CdeRunJobOperator(
        task_id="run_cde_job",
        connection_id="cde_operator",
        job_name=CDE_JOB_NAME,
        
        # Wait for job completion
        wait=True,

        # Maximum wait time in seconds
        timeout=3600,

        # Optional: polling interval in seconds
        job_poll_interval=30,
    )

    run_cde_job
