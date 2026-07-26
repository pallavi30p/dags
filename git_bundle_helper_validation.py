from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from helpers.bundle_helper import get_bundle_validation_message


def validate_helper_and_config():
    message = get_bundle_validation_message()

    print(message)

    assert message == "SUCCESS: config.yaml was synced from GitDagBundle", (
        f"Unexpected config message: {message}"
    )


with DAG(
    dag_id="git_bundle_helper_config_validation",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["git-bundle", "validation"],
) as dag:

    validate_helper_and_config_task = PythonOperator(
        task_id="validate_helper_and_config",
        python_callable=validate_helper_and_config,
    )
