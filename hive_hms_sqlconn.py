from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


def pretty_log_handler(cursor):
    """
    Safe handler:
    - Handles queries with and without result sets
    - Pretty prints tabular output when available
    """

    # 👉 If no result set (DDL like CREATE, DROP, etc.)
    if cursor.description is None:
        print("Query executed successfully (no result set).")
        return None

    # 👉 Fetch rows safely
    rows = cursor.fetchall()
    headers = [col[0] for col in cursor.description]

    if not rows:
        print("No rows returned.")
        return rows

    # Compute column widths
    col_widths = []
    for i in range(len(headers)):
        max_len = len(headers[i])
        for row in rows:
            val = str(row[i]) if row[i] is not None else "NULL"
            max_len = max(max_len, len(val))
        col_widths.append(max_len)

    def format_row(row):
        return " | ".join(
            str(val if val is not None else "NULL").ljust(col_widths[i])
            for i, val in enumerate(row)
        )

    # Print table
    print("\n" + format_row(headers))
    print("-+-".join("-" * w for w in col_widths))

    for row in rows:
        print(format_row(row))

    print(f"\nTotal rows: {len(rows)}\n")

    return rows


with DAG(
    dag_id="cdw-hive-sql-hms_test",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # 1. Create database
    create_db = SQLExecuteQueryOperator(
        task_id="create_db",
        conn_id="cdw-hive-sql",
        sql="""
            CREATE DATABASE IF NOT EXISTS test_hive
        """,
        handler=pretty_log_handler,
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
        handler=pretty_log_handler,
    )

    # 3. List tables
    show_tables = SQLExecuteQueryOperator(
        task_id="show_tables",
        conn_id="cdw-hive-sql",
        sql="""
            SHOW TABLES IN test_hive
        """,
        handler=pretty_log_handler,
    )

    # 4. Describe table
    describe_table = SQLExecuteQueryOperator(
        task_id="describe_table",
        conn_id="cdw-hive-sql",
        sql="""
            DESCRIBE test_hive.example_hive_hms
        """,
        handler=pretty_log_handler,
    )

    # 5. Describe formatted
    describe_formatted = SQLExecuteQueryOperator(
        task_id="describe_formatted",
        conn_id="cdw-hive-sql",
        sql="""
            DESCRIBE FORMATTED test_hive.example_hive_hms
        """,
        handler=pretty_log_handler,
    )

    # Dependencies
    create_db >> create_table >> show_tables >> describe_table >> describe_formatted
