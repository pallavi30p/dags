from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'depends_on_past': False,
}

dag = DAG(
    'executequery_operator_dag',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='@once',
    catchup=False,
    is_paused_upon_creation=False
)

create_table_impala_task = SQLExecuteQueryOperator(
    task_id="create_table_impala",
    conn_id="impala_conn",
    sql="""
            CREATE TABLE IF NOT EXISTS cde_test_impala_base (
                a STRING,
                b INT
            )
            PARTITIONED BY (c INT)
        """,
    dag=dag
)

write_table_impala_task = SQLExecuteQueryOperator(
    task_id="write_table_impala",
    conn_id="impala_conn",
    sql="INSERT INTO cde_test_impala_base PARTITION (c)  VALUES ('test1', 1, 2)",
    dag=dag
)

read_table_impala_task = SQLExecuteQueryOperator(
    task_id="read_table_impala",
    conn_id="impala_conn",
    sql="SELECT * FROM cde_test_impala_base",
    dag=dag
)

drop_table_impala_task = SQLExecuteQueryOperator(
    task_id="drop_table_impala",
    conn_id="impala_conn",
    sql="DROP TABLE cde_test_impala_base",
    dag=dag
)

# pylint: disable=pointless-statement
(create_table_impala_task  >> write_table_impala_task >> read_table_impala_task >> drop_table_impala_task)
