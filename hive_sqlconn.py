"""cdw-hive-sql-test — end-to-end CRUD DAG for Hive over CDW.

Multi-step trigger-only DAG driven by cwo-qe (see
tests/functional/providers/test_cloudera_providers.py::test_cdw_hive_sql_dag_run_succeeds).

Each run:
  1. make_names        — random db + table suffix, published to XCom
  2. create_db         — CREATE DATABASE IF NOT EXISTS
  3. create_table      — CREATE TABLE IF NOT EXISTS (partitioned by c)
  4. insert_rows       — INSERT two rows across two partitions
  5. read_rows         — SELECT COUNT(*)  (XCom-published)
  6. update_row        — INSERT OVERWRITE PARTITION (Hive-idiomatic update)
  7. read_after_update — SELECT to observe the update  (XCom-published)
  8. drop_table        — DROP TABLE IF EXISTS  (trigger_rule=all_done)
  9. drop_db           — DROP DATABASE IF EXISTS CASCADE  (trigger_rule=all_done)

Random naming per run prevents cross-run collisions.  Cleanup steps use
``trigger_rule='all_done'`` so a mid-DAG failure still tears the DB down —
the warehouse stays clean between runs.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

CONN_ID = "cdw-hive-sql"

with DAG(
    dag_id="cdw-hive-sql-test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["cwo-qe", "cdw", "hive"],
    default_args={"retries": 1},
) as dag:

    @task
    def make_names() -> dict:
        """Random db + table names, published via XCom for downstream tasks."""
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

    # Hive doesn't support UPDATE on non-ACID tables; INSERT OVERWRITE PARTITION
    # is the idiomatic rewrite path.
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
