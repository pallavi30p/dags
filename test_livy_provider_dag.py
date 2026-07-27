"""
DAG: test_provider_livy

Prerequisites
-------------
Create an Airflow connection with:

    Connection ID:   livy_default
    Connection Type: HTTP
    Host:            ccycloud.spark-livy.root.comops.site
    Port:            8998

The connection must be able to reach the Livy server at:

    http://xyz:8998

The Livy server is running and is configured to
submit jobs to the Spark Standalone cluster.

Spark/Livy job files are stored on the VM under:

    /opt/spark/jobs/

Example job:

    /opt/spark/jobs/pi.py

The Livy server must be running before this DAG executes.

Network requirement
-------------------
The Airflow worker/scheduler must be able to connect to the YCloud VM
on TCP port 8998.

Example connectivity test from the Airflow environment:

    curl -i http://xyz:8998/

Expected response:

    HTTP/1.1 302 Found
    Location: http://xyz:8998/ui
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.apache.livy.operators.livy import LivyOperator


with DAG(
    dag_id="test_provider_livy",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    livy_python_task = LivyOperator(
        task_id="pi_python_task",
        file="file:///opt/spark/jobs/pi.py",
        args=["10"],
        name="airflow-pi-test",
        polling_interval=10,
        livy_conn_id="livy_default",
    )
