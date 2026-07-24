"""
DAG: python_virtualenv_connection_env_validation

Purpose:
    Validate that Airflow connections configured via AIRFLOW_CONN_*
    environment variables are available inside a
    PythonVirtualenvOperator virtual environment.

Test Coverage:
    - Creates an isolated Python virtual environment.
    - Reads an Airflow connection using BaseHook.
    - Verifies the connection can be resolved.
    - Prints connection metadata.

Expected Result:
    - The configured AIRFLOW_CONN_* environment variable is recognized.
    - BaseHook.get_connection() returns the expected connection.
    - The task completes successfully.

Prerequisite:
    Configure an Airflow connection through an environment variable, e.g.

    AIRFLOW_CONN_TEST_HTTP=http://user:password@example.com:80

    or

    AIRFLOW_CONN_TEST_HTTP=https://example.com

Notes:
    This DAG validates connection resolution only. It does not make any
    network requests.
"""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonVirtualenvOperator

# Connection ID expected to be provided through AIRFLOW_CONN_TEST_HTTP
CONN_ID = "test_http"


def validate_connection():
    """
    Execute inside the virtual environment.

    Successfully retrieving the connection confirms that
    AIRFLOW_CONN_* variables are available inside the venv.
    """

    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection("test_http")

    print("=" * 60)
    print("AIRFLOW_CONN_* validation successful.")
    print(f"Connection ID : {conn.conn_id}")
    print(f"Connection Type : {conn.conn_type}")
    print(f"Host : {conn.host}")
    print(f"Port : {conn.port}")
    print("=" * 60)

    assert conn.conn_id == "test_http"

    return {
        "status": "SUCCESS",
        "conn_id": conn.conn_id,
        "conn_type": conn.conn_type,
    }


with DAG(
    dag_id="python_virtualenv_connection_env_validation",
    description="Validate AIRFLOW_CONN_* environment variables inside PythonVirtualenvOperator.",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=[
        "validation",
        "python",
        "virtualenv",
        "connection",
    ],
    doc_md="""
# AIRFLOW_CONN_* Validation

## Objective

Validate that Airflow connections configured through
`AIRFLOW_CONN_*` environment variables are available
inside a PythonVirtualenvOperator virtual environment.

## Validation Steps

1. Create a Python virtual environment.
2. Resolve an Airflow connection using `BaseHook.get_connection()`.
3. Print connection metadata.
4. Complete successfully.

## Expected Outcome

The task succeeds and prints the configured connection
information, confirming that the connection is available
inside the virtual environment.
""",
) as dag:

    validate_airflow_connection = PythonVirtualenvOperator(
        task_id="validate_airflow_connection",
        python_callable=validate_connection,
        requirements=[],
        system_site_packages=True,
    )
