from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def execute_nested_operator(**context):
    nested_operator = EmptyOperator(
        task_id="nested_operator"
    )

    # Directly execute the nested operator
    result = nested_operator.execute(context=context)

    return result


with DAG(
    dag_id="test_nested_operator_execute",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test_nested_execute = PythonOperator(
        task_id="test_nested_execute",
        python_callable=execute_nested_operator,
    )
