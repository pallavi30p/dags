"""cdw-hive-sql-jwt — end-to-end CRUD DAG for Hive over CDW with JWT auth.

Identical shape to hive_sqlconn.py; differs only in dag_id and CONN_ID.
The JWT-vs-LDAP distinction lives entirely on the Airflow connection's
``extra`` field ({"auth_mechanism": "JWT", ...}) — the DAG source is
identical to the plain variant.

Steps: make_names → create_db → create_table → insert_rows → read_rows →
update_row → read_after_update → drop_table → drop_db
"""

from __future__ import annotations

import uuid
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

CONN_ID = "cdw-hive-sql-jwt"

with DAG(
    dag_id="cdw-hive-sql-jwt",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["cwo-qe", "cdw", "hive", "jwt"],
    default_args={"retries": 1},
) as dag:

    @task
    def make_names() -> dict:
        suffix = uuid.uuid4().hex[:8]
        return {"db": f"cwoqe_e2e_{suffix}", "table": f"tbl_{suffix}"}

    names = make_names()

    _db = "{{ ti.xcom_pull(task_ids='make_names')['db'] }}"
    _tbl = "{{ ti.xcom_pull(task_ids='make_names')['table'] }}"

    create_db = SQLExecuteQueryOperator(
        task_id="create_db",
        conn_id=CONN_ID,
        sql=f"CREATE DATABASE IF NOT EXISTS {_db}",
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id=CONN_ID,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {_db}.{_tbl} (
                a STRING,
                b INT
            )
            PARTITIONED BY (c INT)
        """,
    )

    insert_rows = SQLExecuteQueryOperator(
        task_id="insert_rows",
        conn_id=CONN_ID,
        sql=f"""
            INSERT INTO {_db}.{_tbl} PARTITION (c) VALUES
              ('alpha', 1, 10),
              ('beta',  2, 20)
        """,
    )

    read_rows = SQLExecuteQueryOperator(
        task_id="read_rows",
        conn_id=CONN_ID,
        sql=f"SELECT COUNT(*) FROM {_db}.{_tbl}",
        do_xcom_push=True,
    )

    update_row = SQLExecuteQueryOperator(
        task_id="update_row",
        conn_id=CONN_ID,
        sql=f"""
            INSERT OVERWRITE TABLE {_db}.{_tbl} PARTITION (c=10)
              SELECT 'alpha-updated', 99
        """,
    )

    read_after_update = SQLExecuteQueryOperator(
        task_id="read_after_update",
        conn_id=CONN_ID,
        sql=f"SELECT a, b FROM {_db}.{_tbl} WHERE c = 10",
        do_xcom_push=True,
    )

    drop_table = SQLExecuteQueryOperator(
        task_id="drop_table",
        conn_id=CONN_ID,
        trigger_rule="all_done",
        sql=f"DROP TABLE IF EXISTS {_db}.{_tbl}",
    )

    drop_db = SQLExecuteQueryOperator(
        task_id="drop_db",
        conn_id=CONN_ID,
        trigger_rule="all_done",
        sql=f"DROP DATABASE IF EXISTS {_db} CASCADE",
    )

    # fmt: off
    # pylint: disable=pointless-statement
    (
        names
        >> create_db
        >> create_table
        >> insert_rows
        >> read_rows
        >> update_row
        >> read_after_update
        >> drop_table
        >> drop_db
    )
    # fmt: on
