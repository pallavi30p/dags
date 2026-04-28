from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="cdw-hive-sql-hms_test",
    schedule=None,
    catchup=False,
) as dag:

    # 1. Create database
    create_db = SQLExecuteQueryOperator(
        task_id="create_db",
        conn_id="cdw-hive-sql",
        sql="""
            CREATE DATABASE IF NOT EXISTS test_hive
        """,
    )

    # 2. Create table
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="cdw-hive-sql",
        sql="""
            CREATE TABLE IF NOT EXISTS test_hive.example_hive_hms (
                a STRING,
                b INT
            )
            PARTITIONED BY (c INT)
        """,
    )

    # 3. List tables (metadata check)
    show_tables = SQLExecuteQueryOperator(
        task_id="show_tables",
        conn_id="cdw-hive-sql",
        sql="""
            SHOW TABLES IN test_hive
        """,
    )

    # 4. Describe table schema
    describe_table = SQLExecuteQueryOperator(
        task_id="describe_table",
        conn_id="cdw-hive-sql",
        sql="""
            DESCRIBE test_hive.example_hive_hms
        """,
    )

    # 5. Full metadata from HMS via Hive
    describe_formatted = SQLExecuteQueryOperator(
        task_id="describe_formatted",
        conn_id="cdw-hive-sql",
        sql="""
            DESCRIBE FORMATTED test_hive.example_hive_hms
        """,
    )

    # Task dependencies
    create_db >> create_table >> show_tables >> describe_table >> describe_formatted
