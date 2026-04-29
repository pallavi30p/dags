from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable, Connection 
from datetime import datetime
import requests
import json
import urllib3

# Suppress SSL warnings for sandbox environments
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "start_date": datetime(2026, 1, 1),
}

def trigger_cai_job():
    # 1. Retrieve IDs and URL from Variables
    base_url = Variable.get("cai_url")
    project_id = Variable.get("cai_project_id")
    job_id = Variable.get("cai_job_id")
    
    # 2. Retrieve Connection (Both ID and Secret)
    conn = Connection.get("cai_api_token")
    
    # The API Key ID (Stored in 'login') - useful for logging/tracking
    api_key_id = conn.login 
    # The Secret Key (Stored in 'password') - this is your actual Token
    api_secret = conn.password.strip() if conn.password else ""

    print(f"Using API Key ID: {api_key_id[:8]}... (Redacted)")
    print(f"API Secret length: {len(api_secret)} characters")

    # 3. Build the Trigger Request
    endpoint = f"{base_url}/api/v2/projects/{project_id}/jobs/{job_id}/runs"
    
    # Authentication: In CAI v2, the Secret is passed as the Bearer token
    headers = {
        "Authorization": f"Bearer {api_secret}",
        "Content-Type": "application/json"
    }

    # API Call
    response = requests.post(endpoint, headers=headers, data=json.dumps({}), verify=False)

    if response.status_code in [200, 201]:
        print(f"Success! Job triggered successfully.")
        print(f"Run ID: {response.json().get('id')}")
    else:
        print(f"Failed! Status: {response.status_code}")
        print(f"Response: {response.text}")
        raise Exception(f"CAI Job Trigger Failed: {response.text}")

with DAG(
    dag_id="cai_cwo_trigger",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["cai", "integration"],
) as dag:

    run_job = PythonOperator(
        task_id='trigger_cai_job_task',
        python_callable=trigger_cai_job
    )

    run_job
