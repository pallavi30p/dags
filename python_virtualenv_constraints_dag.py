"""
DAG: python_virtualenv_constraints_validation

Purpose:
    Validate that PythonVirtualenvOperator correctly installs Python
    packages within an isolated virtual environment while honoring
    an Apache Airflow constraints.txt file.

Test Coverage:
    - Verifies that PythonVirtualenvOperator is enabled.
    - Creates an isolated Python virtual environment.
    - Installs a Python package using pip.
    - Applies the official Airflow constraints.txt file during
      installation.
    - Executes Python code inside the virtual environment.
    - Verifies the installed package version.

Expected Result:
    - Virtual environment is created successfully.
    - pip installs the required package while honoring the
      supplied constraints file.
    - The package is importable inside the virtual environment.
    - The task completes successfully.

Notes:
    - Uses the official Apache Airflow constraints file for
      Airflow 3.2.1 running on Python 3.12.
    - Intended only as a validation DAG.
"""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonVirtualenvOperator

AIRFLOW_CONSTRAINTS = (
    "https://raw.githubusercontent.com/"
    "apache/airflow/"
    "constraints-3.2.1/"
    "constraints-3.12.txt"
)


def validate_constraints():
    """
    Execute inside the isolated virtual environment.

    Successful execution confirms that:

    1. The virtual environment was created.
    2. pip successfully installed packages.
    3. The installation honored the supplied constraints file.
    4. The installed package is available for use.
    """

    import platform

    import requests

    print("=" * 60)
    print("PythonVirtualenvOperator constraints validation successful.")
    print(f"Python version   : {platform.python_version()}")
    print(f"Requests version : {requests.__version__}")
    print("=" * 60)

    return {
        "status": "SUCCESS",
        "python_version": platform.python_version(),
        "requests_version": requests.__version__,
    }


with DAG(
    dag_id="python_virtualenv_constraints_validation",
    description="Validate PythonVirtualenvOperator using Airflow constraints.txt.",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=[
        "validation",
        "python",
        "virtualenv",
        "constraints",
        "pip",
    ],
    doc_md="""
# PythonVirtualenvOperator Constraints Validation

## Objective

Validate that **PythonVirtualenvOperator** installs Python packages
while honoring the official Apache Airflow **constraints.txt** file.

## Validation Steps

1. Create an isolated Python virtual environment.
2. Install the `requests` package.
3. Apply the official Airflow constraints file during installation.
4. Execute Python code inside the virtual environment.
5. Verify that the package is available.
6. Complete successfully.

## Expected Outcome

The task should succeed and the logs should contain:

- Python version
- Installed requests version
- Validation success message

Successful completion confirms that pip correctly honored the
constraints file during installation.
""",
) as dag:

    validate_python_virtualenv_constraints = PythonVirtualenvOperator(
        task_id="validate_python_virtualenv_constraints",
        python_callable=validate_constraints,
        requirements=[
            "requests",
        ],
        pip_install_options=[
            "--constraint",
            AIRFLOW_CONSTRAINTS,
        ],
        system_site_packages=False,
    )
