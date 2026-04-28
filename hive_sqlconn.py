from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="cdw-hive-sql-test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
     create_db = SQLExecuteQueryOperator(
        task_id="create_db",
        conn_id="cdw-hive-sql",
        sql="""
            CREATE DATABASE IF NOT EXISTS test_sql
        """,
    )

    create_table_hive_task = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="cdw-hive-sql",
        sql="""
            CREATE TABLE IF NOT EXISTS test_sql.example_hive (
                a STRING,
                b INT
            )
            PARTITIONED BY (c INT)
        """,
    )


    # fmt: off
    # pylint: disable=pointless-statement
    create_db >> create_table_hive_task
    # fmt: on
