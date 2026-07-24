"""
DAG: python_virtualenv_extra_provider_validation

Purpose:
    Validate that an additional Airflow provider can be installed
    within a PythonVirtualenvOperator virtual environment.

Test Coverage:
    - Creates an isolated Python virtual environment.
    - Reuses the base Airflow installation via system_site_packages.
    - Installs an additional Airflow provider only for this virtual
      environment.
    - Imports a class from the provider successfully.

Expected Result:
    - pip installs the provider inside the virtual environment.
    - The provider can be imported successfully.
    - The task completes successfully.

Notes:
    - This DAG assumes the selected provider is NOT already installed
      in the base Airflow image.
"""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import PythonVirtualenvOperator

PROVIDER = "apache-airflow-providers-trino==6.3.0"


def validate_provider():
    """
    Execute inside the virtual environment.

    Successfully importing the provider confirms that the provider
    was installed into the virtual environment.
    """

    from airflow.providers.trino.hooks.trino import TrinoHook

    print("=" * 60)
    print("Extra provider validation successful.")
    print(f"Imported class: {TrinoHook.__name__}")
    print("=" * 60)

    return {
        "status": "SUCCESS",
        "provider": "apache-airflow-providers-trino",
    }


with DAG(
    dag_id="python_virtualenv_extra_provider_validation",
    description="Validate installation of an additional Airflow provider in a virtual environment.",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=[
        "validation",
        "python",
        "virtualenv",
        "provider",
    ],
    doc_md="""
# PythonVirtualenvOperator Extra Provider Validation

## Objective

Validate that an additional Airflow provider can be installed
inside a Python virtual environment created by
PythonVirtualenvOperator.

## Validation Steps

1. Create a Python virtual environment.
2. Reuse the base Airflow installation.
3. Install an additional Airflow provider.
4. Import the provider successfully.
5. Complete successfully.

## Expected Outcome

The task succeeds and the logs show:

- pip installing the provider
- Successful provider import
- Validation success message
""",
) as dag:

    validate_extra_provider = PythonVirtualenvOperator(
        task_id="validate_extra_provider",
        python_callable=validate_provider,
        requirements=[
            PROVIDER,
        ],
        system_site_packages=True,
    )
