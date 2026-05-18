from airflow import DAG
from datetime import datetime
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="cde_job_trigger_dag_awc",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["cde"],
) as dag:

    run_cde_job = CdeRunJobOperator(
        task_id="run_cde_job",
        connection_id="cde_operator_awc",
        job_name="example-scala-pi",
        wait=True,
        timeout=3600,
    )

    run_cde_job


# steps for creating cde connection
# 1. Admin > Connections > Add Connection
# 2. Cloudera Data Engineering.
# 3. Host -> vc link
# 4. Port -> 443
# 5. CDP Access Key , CDP Private Key
# 6. Schema -> default
# 7. Extra Fields - 
#     {
#       "cdp_endpoint": "",
#       "form_factor": "private",
#       "insecure": true
#     }
# also get a job name to replace in L21
