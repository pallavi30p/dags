from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="cdw-impala-sql_jwt",
    schedule=None,
    catchup=False,
) as dag:

    create_db = SQLExecuteQueryOperator(
        task_id="create_db",
        conn_id="cdw-impala-sql-jwt",
        sql="""
            CREATE DATABASE IF NOT EXISTS test_impala
        """,
    )
    
    create_table_impala = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="cdw-impala-sql-jwt",
        sql="""
                CREATE TABLE IF NOT EXISTS test_impala.example_impala_jwt (
                    a STRING,
                    b INT
                )
                PARTITIONED BY (c INT)
            """,
    )

    write_table_impala = SQLExecuteQueryOperator(
        task_id="write_table",
        conn_id="cdw-impala-sql-jwt",
        sql="INSERT INTO test_impala.example_impala_jwt PARTITION (c)  VALUES ('test1', 1, 2)",
    )

    read_table_impala = SQLExecuteQueryOperator(
        task_id="read_table",
        conn_id="cdw-impala-sql-jwt",
        sql="SELECT * FROM test_impala.example_impala_jwt",
    )

    # fmt: off
    # pylint: disable=pointless-statement
    create_db >> create_table_impala >> write_table_impala >> read_table_impala
    # fmt: on
