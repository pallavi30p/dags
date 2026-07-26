from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from helpers.bundle_helper import get_bundle_validation_message


def validate_helper():
    message = get_bundle_validation_message()

    print(message)

    assert message == "SUCCESS: helper.py was synced from GitDagBundle", (
        "The helper returned an unexpected value"
    )


with DAG(
    dag_id="git_bundle_helper_validation",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["git-bundle", "validation"],
) as dag:

    validate_helper_task = PythonOperator(
        task_id="validate_helper",
        python_callable=validate_helper,
    )
