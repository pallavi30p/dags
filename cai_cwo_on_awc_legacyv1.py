from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable, Connection
from datetime import datetime
import requests
import urllib3


# ============================================================
# Sandbox SSL
# ============================================================

# Suppress SSL warnings for sandbox environments.
# For production, use verify=True with the appropriate CA.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ============================================================
# Default DAG arguments
# ============================================================

default_args = {
    "owner": "airflow",
    "retries": 1,
    "start_date": datetime(2026, 1, 1),
}


# ============================================================
# Main function
# ============================================================

def trigger_cai_job_v2():

    # --------------------------------------------------------
    # 1. Read configuration from Airflow Variables
    # --------------------------------------------------------

    cai_url = Variable.get("cai_url_legacy").rstrip("/")
    cai_project_name = Variable.get("cai_project_name_legacy")
    cai_job_name = Variable.get("cai_job_name_legacy")

    print(f"CAI URL: {cai_url}")
    print(f"CAI Project: {cai_project_name}")
    print(f"CAI Job: {cai_job_name}")


    # --------------------------------------------------------
    # 2. Get API key from Airflow Connection
    # --------------------------------------------------------

    conn = Connection.get("cai_api_token_legacy")

    api_key = conn.login

    if not api_key:
        raise ValueError(
            "CAI API key is missing from Airflow Connection "
            "'cai_api_token'"
        )

    print(f"Using CAI API key: {api_key[:8]}... (redacted)")


    # --------------------------------------------------------
    # 3. Create HTTP headers
    # --------------------------------------------------------

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


    # --------------------------------------------------------
    # 4. Find project by project name
    # --------------------------------------------------------

    projects_endpoint = f"{cai_url}/api/v2/projects"

    print(f"Looking up project: {cai_project_name}")

    project_response = requests.get(
        projects_endpoint,
        headers=headers,
        verify=False,
        timeout=60,
    )

    print(f"Projects API status: {project_response.status_code}")

    if project_response.status_code != 200:
        raise Exception(
            "Failed to retrieve CAI projects. "
            f"HTTP {project_response.status_code}: "
            f"{project_response.text}"
        )

    projects_data = project_response.json()

    projects = projects_data.get("projects", [])

    matching_projects = [
        project
        for project in projects
        if project.get("name") == cai_project_name
    ]

    if not matching_projects:
        available_projects = [
            project.get("name")
            for project in projects
        ]

        raise ValueError(
            f"CAI project '{cai_project_name}' was not found. "
            f"Available projects: {available_projects}"
        )

    if len(matching_projects) > 1:
        raise ValueError(
            f"Multiple CAI projects found with name "
            f"'{cai_project_name}'. Project names must be unique."
        )

    project = matching_projects[0]

    cai_project_id = project["id"]

    print(
        f"Found project '{cai_project_name}' "
        f"with ID '{cai_project_id}'"
    )


    # --------------------------------------------------------
    # 5. Find job by job name
    # --------------------------------------------------------

    jobs_endpoint = (
        f"{cai_url}/api/v2/projects/"
        f"{cai_project_id}/jobs"
    )

    print(f"Looking up job: {cai_job_name}")

    jobs_response = requests.get(
        jobs_endpoint,
        headers=headers,
        verify=False,
        timeout=60,
    )

    print(f"Jobs API status: {jobs_response.status_code}")

    if jobs_response.status_code != 200:
        raise Exception(
            "Failed to retrieve CAI jobs. "
            f"HTTP {jobs_response.status_code}: "
            f"{jobs_response.text}"
        )

    jobs_data = jobs_response.json()

    jobs = jobs_data.get("jobs", [])

    matching_jobs = [
        job
        for job in jobs
        if job.get("name") == cai_job_name
    ]

    if not matching_jobs:
        available_jobs = [
            job.get("name")
            for job in jobs
        ]

        raise ValueError(
            f"CAI job '{cai_job_name}' was not found "
            f"in project '{cai_project_name}'. "
            f"Available jobs: {available_jobs}"
        )

    if len(matching_jobs) > 1:
        raise ValueError(
            f"Multiple CAI jobs found with name "
            f"'{cai_job_name}' in project "
            f"'{cai_project_name}'."
        )

    job = matching_jobs[0]

    cai_job_id = job["id"]

    print(
        f"Found job '{cai_job_name}' "
        f"with ID '{cai_job_id}'"
    )


    # --------------------------------------------------------
    # 6. Trigger the job
    # --------------------------------------------------------

    run_endpoint = (
        f"{cai_url}/api/v2/projects/"
        f"{cai_project_id}/jobs/"
        f"{cai_job_id}/runs"
    )

    print(f"Triggering CAI job...")
    print(f"Run endpoint: {run_endpoint}")

    run_response = requests.post(
        run_endpoint,
        headers=headers,
        json={},
        verify=False,
        timeout=60,
    )

    print(f"Run API status: {run_response.status_code}")
    print(f"Run API response: {run_response.text}")

    if run_response.status_code not in (200, 201):
        raise Exception(
            "Failed to trigger CAI job. "
            f"HTTP {run_response.status_code}: "
            f"{run_response.text}"
        )


    # --------------------------------------------------------
    # 7. Return run information
    # --------------------------------------------------------

    try:
        run_data = run_response.json()
    except ValueError:
        run_data = run_response.text

    print(
        f"Successfully triggered CAI job "
        f"'{cai_job_name}' in project "
        f"'{cai_project_name}'"
    )

    print(f"Project ID: {cai_project_id}")
    print(f"Job ID: {cai_job_id}")
    print(f"Run response: {run_data}")

    return run_data


# ============================================================
# DAG definition
# ============================================================

with DAG(
    dag_id="cai_job_trigger_legacy",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["cai", "v2", "legacy", "integration"],
) as dag:

    trigger_job = PythonOperator(
        task_id="trigger_cai_job",
        python_callable=trigger_cai_job_v2,
    )

    trigger_job
