"""
DAG: cde_job_trigger_with_variable_polling

Triggers a CDE job whose name is stored in the Airflow Variable
``cde_job_name``, then waits for the CDE job to complete.

Note on the ``cde_job_name`` Variable:
  The variable is resolved via a Jinja template
  (``{{ var.value.cde_job_name }}``) which is expanded **at task execution
  time**, NOT at DAG parse time. This means:

    - The DAG parses cleanly on any Airflow instance even when the variable
      is missing (no ``VARIABLE_NOT_FOUND`` import error).
    - The variable only needs to exist when the DAG is *triggered*.
    - Tests can create the variable just-in-time before triggering the DAG
      and delete it on teardown.

  The previous version called ``Variable.get("cde_job_name")`` at the module
  top level, which is a classic Airflow anti-pattern: it forces every
  DAG-processor parse to fetch the variable, and the DAG file ends up in
  the import-errors list whenever the variable isn't present.

Airflow connection required at runtime:
  Connection ID: ``cde_operator``
  Type:          ``cde``
  Host:          <CDE Virtual Cluster URL>
  Login:         <OAuth client ID>
  Password:      <OAuth client secret>
  Extra:         {"auth_mode":"awc","awc_console_url":"...","cache_dir":"/tmp/","insecure":"true"}
"""

from datetime import datetime

from airflow import DAG

from cloudera.airflow.providers.operators.cde import CdeRunJobOperator


default_args = {
    "owner": "airflow",
    "retries": 1,
}


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
        # Resolved at execution time from the Airflow Variable — the DAG
        # parses cleanly even when the variable is unset.
        job_name="{{ var.value.cde_job_name }}",

        # Wait for job completion
        wait=True,

        # Maximum wait time in seconds
        timeout=3600,

        # Optional: polling interval in seconds
        job_poll_interval=30,
    )

    run_cde_job
