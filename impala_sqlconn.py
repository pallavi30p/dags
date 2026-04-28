from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="cdw-impala-sql",
    schedule=None,
    catchup=False,
) as dag:

    create_db = SQLExecuteQueryOperator(
        task_id="create_db",
        conn_id="cdw-impala-sql",
        sql="""
            CREATE DATABASE IF NOT EXISTS test_impala
        """,
    )
    
    create_table_impala = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="cdw-impala-sql",
        sql="""
                CREATE TABLE IF NOT EXISTS test_impala.example_impala (
                    a STRING,
                    b INT
                )
                PARTITIONED BY (c INT)
            """,
    )

    write_table_impala = SQLExecuteQueryOperator(
        task_id="write_table",
        conn_id="cdw-impala-sql",
        sql="INSERT INTO example_impala PARTITION (c)  VALUES ('test1', 1, 2)",
    )

    read_table_impala = SQLExecuteQueryOperator(
        task_id="read_table",
        conn_id="cdw-impala-sql",
        sql="SELECT * FROM example_impala",
    )

    # fmt: off
    # pylint: disable=pointless-statement
    create_db >> create_table_impala >> write_table_impala >> read_table_impala
    # fmt: on
