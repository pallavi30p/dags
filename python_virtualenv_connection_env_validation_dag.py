"""
DAG: python_virtualenv_connection_validation

Purpose:
    Validate that an Airflow Connection configured through
    Admin → Connections is accessible inside a
    PythonVirtualenvOperator virtual environment.

Test Coverage:
    - Creates an isolated Python virtual environment.
    - Reuses the base Airflow installation.
    - Retrieves an Airflow Connection using BaseHook.
    - Validates the retrieved connection metadata.

Expected Result:
    - The configured connection is successfully retrieved.
    - Connection metadata matches the configured values.
    - The task completes successfully.

Prerequisite:
    Create the following Airflow Connection:

    Connection ID   : test_http
    Connection Type : HTTP
    Host            : example.com
    Login           : user (optional)
    Password        : password (optional)
    Port            : 80

Notes:
    This DAG validates connection resolution only.
    It does not perform any network communication.
"""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonVirtualenvOperator

CONN_ID = "test_http"


def validate_connection():
    """
    Execute inside the virtual environment.

    Successfully retrieving the Airflow Connection confirms
    that Airflow metadata is accessible within the virtual
    environment.
    """

    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(CONN_ID)

    print("=" * 60)
    print("Airflow Connection validation successful.")
    print(f"Connection ID   : {conn.conn_id}")
    print(f"Connection Type : {conn.conn_type}")
    print(f"Host            : {conn.host}")
    print(f"Port            : {conn.port}")
    print("=" * 60)

    assert conn.conn_id == CONN_ID
    assert conn.conn_type == "http"
    assert conn.host == "example.com"
    assert conn.port == 80

    return {
        "status": "SUCCESS",
        "conn_id": conn.conn_id,
        "conn_type": conn.conn_type,
        "host": conn.host,
        "port": conn.port,
    }


with DAG(
    dag_id="python_virtualenv_connection_validation",
    description="Validate Airflow Connection availability inside PythonVirtualenvOperator.",
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
# PythonVirtualenvOperator Connection Validation

## Objective

Validate that an Airflow Connection configured through
**Admin → Connections** is accessible inside a
`PythonVirtualenvOperator` virtual environment.

## Validation Steps

1. Create a Python virtual environment.
2. Retrieve the configured Airflow Connection.
3. Validate the connection metadata.
4. Complete successfully.

## Expected Outcome

The task succeeds and prints:

- Connection ID
- Connection Type
- Host
- Port

Successful completion confirms that Airflow Connections
are available inside the virtual environment.
""",
) as dag:

    validate_airflow_connection = PythonVirtualenvOperator(
        task_id="validate_airflow_connection",
        python_callable=validate_connection,
        requirements=[],
        # Airflow must be available inside the virtual environment
        # to import BaseHook.
        system_site_packages=True,
    )
