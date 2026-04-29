from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable, Connection 
from datetime import datetime
import requests
import json
import urllib3

# Suppress SSL warnings for sandbox environments
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Default arguments matching your CDE structure
default_args = {
    "owner": "airflow",
    "retries": 1,
    "start_date": datetime(2026, 1, 1),
}

def trigger_cai_workbench_logic():
    """Logic to trigger a Job in Cloudera AI Workbench"""
    
    # 1. Fetch the NEW Variable names (cai_cde appended)
    base_url = Variable.get("cai_cde_url")
    project_id = Variable.get("cai_cde_project_id")
    job_id = Variable.get("cai_cde_job_id")
    
    # 2. Securely fetch Connection credentials
    conn = Connection.get("cai_api_token")
    api_secret = conn.password.strip() if conn.password else ""

    # Log verification (Redacted)
    print(f"Targeting Project: {project_id}")
    print(f"Targeting Job: {job_id}")
    print(f"Auth Secret detected: {len(api_secret)} characters")

    # 3. Construct API Endpoint
    endpoint = f"{base_url}/api/v2/projects/{project_id}/jobs/{job_id}/runs"
    
    headers = {
        "Authorization": f"Bearer {api_secret}",
        "Content-Type": "application/json"
    }

    # 4. Execute the Trigger
    # verify=False is used for sandbox SSL bypass
    response = requests.post(
        endpoint, 
        headers=headers, 
        data=json.dumps({}), 
        verify=False
    )

    # 5. Success Check (Allowing 200 and 201)
    if response.status_code in [200, 201]:
        run_data = response.json()
        print("--- JOB TRIGGERED SUCCESSFULLY ---")
        print(f"Run ID: {run_data.get('id')}")
        print(f"Initial Status: {run_data.get('status')}")
    else:
        print(f"Trigger Failed with Status: {response.status_code}")
        print(f"Response Body: {response.text}")
        raise Exception(f"CAI Job Trigger Failed: {response.text}")

# DAG Definition
with DAG(
    dag_id="cai_cde_job_integration",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["cai", "cde", "integration"],
) as dag:

    run_cai_job = PythonOperator(
        task_id="trigger_workbench_job",
        python_callable=trigger_cai_workbench_logic,
    )

    # Task dependency / Call
    run_cai_job
