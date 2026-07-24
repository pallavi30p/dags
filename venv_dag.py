"""
DAG: python_virtualenv_operator_validation

Purpose:
    Validate that the PythonVirtualenvOperator is enabled and functioning
    correctly in the Airflow deployment.

Test Coverage:
    - Verifies that PythonVirtualenvOperator is available.
    - Creates an isolated Python virtual environment.
    - Installs an external dependency (requests).
    - Executes Python code inside the virtual environment.
    - Returns successfully if the operator works as expected.

Expected Result:
    The DAG should complete successfully and the task logs should show:
        - The Python version used inside the virtual environment.
        - The installed requests package version.
        - A success message.

Notes:
    - If PythonVirtualenvOperator is disabled in the configuration,
      this DAG will fail during parsing or execution.
"""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator


def validate_virtualenv():
    """
    Function executed inside the isolated virtual environment.

    This function intentionally imports an external package that is
    installed via the `requirements` argument. Successful execution
    confirms that:

    1. The virtual environment was created successfully.
    2. PythonVirtualenvOperator executed correctly.
    3. External Python packages were installed in the virtual environment.
    """

    import platform

    import requests

    print("=" * 60)
    print("PythonVirtualenvOperator validation successful.")
    print(f"Python version : {platform.python_version()}")
    print(f"Requests version: {requests.__version__}")
    print("Virtual environment created successfully.")
    print("=" * 60)

    return {
        "python_version": platform.python_version(),
        "requests_version": requests.__version__,
        "status": "SUCCESS",
    }


with DAG(
    dag_id="python_virtualenv_operator_validation",
    description="Validation DAG for PythonVirtualenvOperator.",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["validation", "operator", "python", "virtualenv"],
    doc_md="""
# PythonVirtualenvOperator Validation

## Objective

This DAG validates that **PythonVirtualenvOperator** is enabled and working
correctly in the Airflow deployment.

## Validation Steps

1. Create a temporary Python virtual environment.
2. Install the `requests` package.
3. Execute Python code inside the virtual environment.
4. Print the Python and requests versions.
5. Complete successfully.

## Expected Outcome

The task should succeed and the logs should contain:

- Python version
- Requests package version
- Success message indicating the virtual environment was created successfully.
""",
) as dag:

    validate_python_virtualenv = PythonVirtualenvOperator(
        task_id="validate_python_virtualenv",
        python_callable=validate_virtualenv,
        requirements=["requests==2.32.3"],
        system_site_packages=False,
    )
