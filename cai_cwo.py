from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable, Connection
from datetime import datetime
import requests
import json
import urllib3

# This suppresses the SSL warnings in the logs caused by verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def trigger_cai_job():
    # 1. Fetch values from Airflow
    base_url = Variable.get("cai_url")
    project_id = Variable.get("cai_project_id")
    job_id = Variable.get("cai_job_id")
    
    # Retrieves the API key from the 'Password' field of your connection
    conn = Connection.get_connection_from_secrets("cai_api_token")
    api_key = conn.password

    # 2. Build the API Endpoint
    endpoint = f"{base_url}/api/v2/projects/{project_id}/jobs/{job_id}/runs"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    print(f"Sending request to: {endpoint}")

    # 3. Make the call (verify=False is key for your sandbox)
    response = requests.post(
        endpoint, 
        headers=headers, 
        data=json.dumps({}), 
        verify=False 
    )

    if response.status_code == 201:
        run_id = response.json().get('id')
        print(f"Successfully triggered! CAI Job Run ID: {run_id}")
    else:
        print(f"Error: {response.status_code}")
        print(f"Details: {response.text}")
        raise Exception(f"CAI Job Trigger Failed: {response.text}")

with DAG(
    'cwo_to_cai_trigger',
    schedule=None,
    catchup=False,
    tags=['cai_integration']
) as dag:

    run_job = PythonOperator(
        task_id='trigger_cai_job_task',
        python_callable=trigger_cai_job
    )

    run_job
