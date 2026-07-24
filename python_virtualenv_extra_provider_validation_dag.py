"""
DAG: python_virtualenv_extra_provider_validation

Purpose:
    Validate that an additional Airflow provider can be installed
    inside a PythonVirtualenvOperator virtual environment without
    affecting the base Airflow installation.

Test Coverage:
    - Verifies the provider is NOT installed in the base environment.
    - Creates an isolated Python virtual environment.
    - Installs an additional Airflow provider in the virtual environment.
    - Successfully imports the provider inside the virtual environment.
    - Verifies the provider is still NOT installed in the base environment.

Expected Result:
    - Base environment cannot import the provider.
    - Virtual environment imports the provider successfully.
    - Base environment remains unchanged after task completion.

Notes:
    - Choose a provider that is not already present in your Airflow image.
    - This DAG is intended for validation purposes only.
"""

from __future__ import annotations

import importlib.util

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import (
    PythonOperator,
    PythonVirtualenvOperator,
)

# ---------------------------------------------------------------------
# Choose a provider that is NOT installed in your base Airflow image.
# ---------------------------------------------------------------------

PROVIDER_PACKAGE = "apache-airflow-providers-microsoft-azure==12.8.0"

IMPORT_MODULE = "airflow.providers.microsoft.azure.hooks.wasb"


def verify_provider_not_installed():
    """
    Verify that the provider is not installed in the base Airflow
    environment.
    """
    spec = importlib.util.find_spec(IMPORT_MODULE)

    assert (
        spec is None
    ), (
        f"{IMPORT_MODULE} is already installed in the base image. "
        "Choose a provider that is not preinstalled."
    )

    print(f"{IMPORT_MODULE} is NOT installed in the base environment.")


def validate_provider_in_virtualenv():
    """
    Execute inside the virtual environment.

    Successfully importing the provider confirms that it was installed
    only inside the virtual environment.
    """

    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

    print("=" * 60)
    print("Successfully imported provider inside virtual environment.")
    print(f"Imported class: {WasbHook.__name__}")
    print("=" * 60)

    return "SUCCESS"


with DAG(
    dag_id="python_virtualenv_extra_provider_validation",
    description="Validate provider installation only inside a virtual environment.",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=[
        "validation",
        "python",
        "virtualenv",
        "providers",
    ],
    doc_md="""
# Extra Provider Installation Validation

## Objective

Validate that an additional Airflow provider can be installed
inside a PythonVirtualenvOperator virtual environment without
modifying the base Airflow installation.

## Validation Steps

1. Verify the provider is not installed in the base environment.
2. Create a Python virtual environment.
3. Install the provider in the virtual environment.
4. Import the provider successfully.
5. Verify the provider is still unavailable in the base environment.

## Expected Outcome

- Base environment cannot import the provider.
- Virtual environment imports the provider successfully.
- Base environment remains unchanged.
""",
) as dag:

    check_before = PythonOperator(
        task_id="verify_provider_not_installed_before",
        python_callable=verify_provider_not_installed,
    )

    install_provider = PythonVirtualenvOperator(
        task_id="install_provider_in_virtualenv",
        python_callable=validate_provider_in_virtualenv,
        requirements=[
            PROVIDER_PACKAGE,
        ],
        system_site_packages=False,
    )

    check_after = PythonOperator(
        task_id="verify_provider_not_installed_after",
        python_callable=verify_provider_not_installed,
    )

    check_before >> install_provider >> check_after
