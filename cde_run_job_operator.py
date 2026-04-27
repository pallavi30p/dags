from airflow import DAG
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator

# Default args
default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="cde_job_run_operator_example",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["cde"],
) as dag:

    run_cde_job = CdeRunJobOperator(
        task_id="run_cde_job",
        job_name="cde_job_operator_test",   
        connection_id="cde_operator",    
        variables={"param1": "value1"}, 
        timeout=3600,                  
    )

    run_cde_job
