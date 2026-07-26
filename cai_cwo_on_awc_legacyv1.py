from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable, Connection
from datetime import datetime
import requests
import urllib3


# Suppress SSL warnings for sandbox environments
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "start_date": datetime(2026, 1, 1),
}


def trigger_cai_job_legacy():

    # =========================================================
    # 1. Retrieve CAI v1 Legacy configuration
    # =========================================================

    base_url_legacy = Variable.get("cai_url_legacy").rstrip("/")
    project_user_legacy = Variable.get("cai_project_user_legacy")
    project_name_legacy = Variable.get("cai_project_name_legacy")
    job_id_legacy = Variable.get("cai_job_id_legacy")


    # =========================================================
    # 2. Retrieve API key from Airflow Connection
    # =========================================================

    conn_legacy = Connection.get("cai_api_token_legacy")

    # For CAI v1:
    #   Basic Auth username = API Key
    #   Basic Auth password = empty
    api_key_legacy = conn_legacy.login

    if not api_key_legacy:
        raise ValueError(
            "CAI legacy API key is missing from "
            "Airflow Connection 'cai_api_token_legacy'"
        )


    # =========================================================
    # 3. Build CAI v1 Legacy endpoint
    # =========================================================

    endpoint_legacy = (
        f"{base_url_legacy}"
        f"/api/v1/projects/"
        f"{project_user_legacy}/"
        f"{project_name_legacy}/"
        f"jobs/"
        f"{job_id_legacy}/"
        f"start"
    )


    # =========================================================
    # 4. Logging (do NOT print the API key)
    # =========================================================

    print("CAI Legacy Job Trigger")
    print(f"Endpoint: {endpoint_legacy}")
    print(f"Project User: {project_user_legacy}")
    print(f"Project Name: {project_name_legacy}")
    print(f"Job ID: {job_id_legacy}")
    print(f"API Key: {api_key_legacy[:8]}... (redacted)")


    # =========================================================
    # 5. Trigger CAI v1 Legacy Job
    # =========================================================

    response_legacy = requests.post(
        endpoint_legacy,

        # CAI v1 Legacy authentication
        # username = API key
        # password = empty
        auth=(api_key_legacy, ""),

        headers={
            "Content-Type": "application/json",
        },

        # Empty JSON request body
        json={},

        # Sandbox certificate
        verify=False,

        timeout=60,
    )


    # =========================================================
    # 6. Process response
    # =========================================================

    print(f"HTTP Status: {response_legacy.status_code}")
    print(f"Response: {response_legacy.text}")


    if response_legacy.status_code in (200, 201):

        print("CAI legacy job triggered successfully!")

        try:
            return response_legacy.json()
        except ValueError:
            return response_legacy.text


    raise Exception(
        "CAI Legacy Job Trigger Failed: "
        f"HTTP {response_legacy.status_code} - "
        f"{response_legacy.text}"
    )


# =============================================================
# DAG
# =============================================================

with DAG(
    dag_id="cai_cwo_trigger_legacy",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["cai", "legacy", "integration"],
) as dag:

    run_job_legacy = PythonOperator(
        task_id="trigger_cai_job_legacy_task",
        python_callable=trigger_cai_job_legacy,
    )

    run_job_legacy
