from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.cloudera.operators.cde import CdeRunJobOperator

# Default args
default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="cde_job_run_operator_example",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["cde"],
) as dag:

    run_cde_job = CdeRunJobOperator(
        task_id="run_cde_job",
        job_name="your_cde_job_name",   
        connection_id="cde_operator",    
        variables={"param1": "value1"}, 
        timeout=3600,                  
    )

    run_cde_job
