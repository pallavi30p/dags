"""
CWO - AWS Glue External Catalog + Glue Job Integration Test
=============================================================

Purpose
-------
This DAG validates CWO/Airflow integration with AWS Glue as an external
cloud metadata store and workload execution platform.

The DAG performs:

1. AWS Glue Data Catalog CRUD:
   - Create a database with a dynamic suffix
   - Get the database
   - Create a table
   - Get the table
   - Update the table
   - Verify the update
   - Delete the table
   - Delete the database

2. AWS Glue external workload execution:
   - Trigger an already-created AWS Glue Job
   - Monitor the Glue Job until it reaches a terminal state
   - Fail the Airflow task if the Glue Job fails

Architecture
------------

                 CWO / Airflow
                       |
                aws_glue_conn
                       |
             +---------+---------+
             |                   |
             v                   v
      AWS Glue Catalog      AWS Glue Job
             |                   |
       Database CRUD        Start + Monitor
             |                   |
          Table CRUD         S3 Python script
                                 |
                                 v
                 s3://qe-s3-bucket-weekly/cwo/cwo/
                       glue-test/cwo_glue_test.py


Airflow Connection
------------------
Create this manually in the Airflow/CWO environment:

Connection ID:
    aws_glue_conn

Connection Type:
    Amazon Web Services

Login:
    <AWS_ACCESS_KEY_ID>

Password:
    <AWS_SECRET_ACCESS_KEY>

Extra:
    {
        "region_name": "us-west-2"
    }

The AWS credentials used by this connection must be able to perform
the Glue Catalog CRUD operations and start/monitor the Glue Job.

AWS Glue Job
------------
The following Glue Job must already exist:

    cwo-glue-external-job

It should point to:

    s3://qe-s3-bucket-weekly/cwo/cwo/glue-test/cwo_glue_test.py

Glue execution role:

    AmazonSageMakerServiceCatalogProductsGlueRole

The execution role must be able to read objects from:

    s3://qe-s3-bucket-weekly/*

Airflow Variables
-----------------
Optional:

    cwo_glue_job_name
        Default:
            cwo-glue-external-job

The database name is generated dynamically by the DAG, so multiple
runs do not collide with an existing database.

Provider Requirement
--------------------
Requires apache-airflow-providers-amazon with:

    airflow.providers.amazon.aws.hooks.glue.GlueCatalogHook
    airflow.providers.amazon.aws.operators.glue.GlueJobOperator
"""

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.glue_catalog import GlueCatalogHook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AWS_CONN_ID = "aws_glue_conn"
AWS_REGION = "us-west-2"

GLUE_JOB_NAME = Variable.get(
    "cwo_glue_job_name",
    default_var="cwo-glue-external-job",
)

# Base names. A timestamp is added at runtime.
DB_BASE_NAME = "cwo_glue_test_db"
TABLE_NAME = "cwo_test_table"


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def get_runtime_names(**context):
    """
    Generate unique database/table names for this DAG run.

    The database gets a timestamp suffix so repeated DAG runs do not
    conflict with previous runs.
    """
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    db_name = f"{DB_BASE_NAME}_{ts}"

    context["ti"].xcom_push(
        key="db_name",
        value=db_name,
    )

    context["ti"].xcom_push(
        key="table_name",
        value=TABLE_NAME,
    )

    print(f"Database: {db_name}")
    print(f"Table:    {TABLE_NAME}")


def create_database(**context):
    """
    Create an AWS Glue Data Catalog database.
    """
    hook = GlueCatalogHook(
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
    )

    db_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="db_name",
    )

    hook.create_database(
        database_name=db_name,
        description="CWO AWS Glue external catalog integration test",
    )

    print(f"Created Glue database: {db_name}")


def get_database(**context):
    """
    Read the database from AWS Glue Data Catalog.
    """
    hook = GlueCatalogHook(
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
    )

    db_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="db_name",
    )

    database = hook.get_database(
        database_name=db_name,
    )

    print(f"Retrieved Glue database: {database}")


def create_table(**context):
    """
    Create a table in the dynamically generated Glue database.
    """
    hook = GlueCatalogHook(
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
    )

    db_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="db_name",
    )

    table_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="table_name",
    )

    table_input = {
        "Name": table_name,
        "Description": "CWO AWS Glue CRUD integration test table",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "created_by": "cwo-airflow",
            "test": "aws-glue-crud",
        },
        "StorageDescriptor": {
            "Columns": [
                {
                    "Name": "id",
                    "Type": "int",
                },
                {
                    "Name": "name",
                    "Type": "string",
                },
            ],
            "Location": "s3://qe-s3-bucket-weekly/cwo/cwo/",
        },
    }

    hook.create_table(
        database_name=db_name,
        table_input=table_input,
    )

    print(f"Created Glue table: {db_name}.{table_name}")


def get_table(**context):
    """
    Retrieve the Glue table and verify that it exists.
    """
    hook = GlueCatalogHook(
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
    )

    db_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="db_name",
    )

    table_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="table_name",
    )

    table = hook.get_table(
        database_name=db_name,
        table_name=table_name,
    )

    print(f"Retrieved Glue table: {table}")


def update_table(**context):
    """
    Update table metadata in the Glue Data Catalog.
    """
    hook = GlueCatalogHook(
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
    )

    db_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="db_name",
    )

    table_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="table_name",
    )

    table_input = {
        "Name": table_name,
        "Description": "UPDATED by CWO AWS Glue integration test",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "created_by": "cwo-airflow",
            "test": "aws-glue-crud",
            "updated_by": "cwo-airflow",
            "update_test": "passed",
        },
        "StorageDescriptor": {
            "Columns": [
                {
                    "Name": "id",
                    "Type": "int",
                },
                {
                    "Name": "name",
                    "Type": "string",
                },
                {
                    "Name": "updated",
                    "Type": "string",
                },
            ],
            "Location": "s3://qe-s3-bucket-weekly/cwo/cwo/",
        },
    }

    hook.update_table(
        database_name=db_name,
        table_input=table_input,
    )

    print(f"Updated Glue table: {db_name}.{table_name}")


def verify_updated_table(**context):
    """
    Verify that the table update is visible through the Glue Catalog.
    """
    hook = GlueCatalogHook(
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
    )

    db_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="db_name",
    )

    table_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="table_name",
    )

    table = hook.get_table(
        database_name=db_name,
        table_name=table_name,
    )

    print(f"Updated table metadata: {table}")

    parameters = table.get("Parameters", {})

    if parameters.get("update_test") != "passed":
        raise RuntimeError(
            "Glue table update verification failed. "
            f"Parameters received: {parameters}"
        )

    print("Glue table update verified successfully.")


def delete_table(**context):
    """
    Delete the test table.
    """
    hook = GlueCatalogHook(
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
    )

    db_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="db_name",
    )

    table_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="table_name",
    )

    hook.delete_table(
        database_name=db_name,
        table_name=table_name,
    )

    print(f"Deleted Glue table: {db_name}.{table_name}")


def delete_database(**context):
    """
    Delete the dynamically generated Glue database.
    """
    hook = GlueCatalogHook(
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
    )

    db_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="db_name",
    )

    hook.delete_database(
        database_name=db_name,
    )

    print(f"Deleted Glue database: {db_name}")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="cwo_aws_glue_external_catalog_and_job",
    description=(
        "CWO AWS Glue Data Catalog CRUD plus external Glue Job "
        "trigger and monitoring test"
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "cwo",
        "aws",
        "glue",
        "external-catalog",
        "external-job",
    ],
) as dag:

    generate_runtime_names = PythonOperator(
        task_id="generate_runtime_names",
        python_callable=get_runtime_names,
    )

    create_db = PythonOperator(
        task_id="create_database",
        python_callable=create_database,
    )

    get_db = PythonOperator(
        task_id="get_database",
        python_callable=get_database,
    )

    create_tbl = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    get_tbl = PythonOperator(
        task_id="get_table",
        python_callable=get_table,
    )

    update_tbl = PythonOperator(
        task_id="update_table",
        python_callable=update_table,
    )

    verify_update = PythonOperator(
        task_id="verify_updated_table",
        python_callable=verify_updated_table,
    )

    # -----------------------------------------------------------------------
    # Trigger the existing AWS Glue Job.
    #
    # The Glue Job itself already points to:
    #
    # s3://qe-s3-bucket-weekly/cwo/cwo/glue-test/cwo_glue_test.py
    #
    # Airflow starts the job and waits until Glue reports a terminal state.
    # -----------------------------------------------------------------------

    run_external_glue_job = GlueJobOperator(
        task_id="run_external_glue_job",
        job_name=GLUE_JOB_NAME,
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
        verbose=True,
    )

    delete_tbl = PythonOperator(
        task_id="delete_table",
        python_callable=delete_table,
    )

    delete_db = PythonOperator(
        task_id="delete_database",
        python_callable=delete_database,
    )

    # -----------------------------------------------------------------------
    # Execution order
    # -----------------------------------------------------------------------

    (
        generate_runtime_names
        >> create_db
        >> get_db
        >> create_tbl
        >> get_tbl
        >> update_tbl
        >> verify_update
        >> run_external_glue_job
        >> delete_tbl
        >> delete_db
    )
