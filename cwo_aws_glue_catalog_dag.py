
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
   - List tables
   - Update the table
   - Verify the update
   - Delete the table
   - Delete the database

2. AWS Glue external workload execution:
   - Trigger an already-created AWS Glue Job
   - Monitor the AWS Glue Job until it reaches a terminal state
   - Fail the Airflow task if the Glue Job fails

3. Cleanup:
   - Cleanup runs even when the external Glue Job fails.
   - This prevents failed test runs from leaving temporary Glue
     databases/tables behind.

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

    airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook
    airflow.providers.amazon.aws.operators.glue.GlueJobOperator
"""

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AWS_CONN_ID = "aws_glue_conn"
AWS_REGION = "us-west-2"

GLUE_JOB_NAME = Variable.get(
    "cwo_glue_job_name",
    default_var="cwo-glue-external-job",
)

# Base names. A timestamp is added at runtime to the database.
DB_BASE_NAME = "cwo_glue_test_db"
TABLE_NAME = "cwo_test_table"


# ---------------------------------------------------------------------------
# AWS Glue client
# ---------------------------------------------------------------------------

def get_glue_client():
    """
    Return a boto3 Glue client using the Airflow AWS connection.

    This intentionally uses AwsBaseHook because this is the implementation
    used by the previously working CWO AWS Glue CRUD DAG.
    """
    hook = AwsBaseHook(
        aws_conn_id=AWS_CONN_ID,
        client_type="glue",
        region_name=AWS_REGION,
    )

    return hook.get_conn()


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def get_runtime_names(**context):
    """
    Generate unique database/table names for this DAG run.

    The database gets a timestamp suffix so repeated DAG runs do not
    conflict with existing databases.
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
    glue = get_glue_client()

    db_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="db_name",
    )

    glue.create_database(
        DatabaseInput={
            "Name": db_name,
            "Description": "CWO AWS Glue external catalog integration test",
        }
    )

    print(f"Created Glue database: {db_name}")


def get_database(**context):
    """
    Read the database from AWS Glue Data Catalog.
    """
    glue = get_glue_client()

    db_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="db_name",
    )

    database = glue.get_database(
        Name=db_name,
    )

    print(f"Retrieved Glue database: {database}")


def create_table(**context):
    """
    Create a table in the dynamically generated Glue database.
    """
    glue = get_glue_client()

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

    glue.create_table(
        DatabaseName=db_name,
        TableInput=table_input,
    )

    print(f"Created Glue table: {db_name}.{table_name}")


def get_table(**context):
    """
    Retrieve the Glue table and verify that it exists.
    """
    glue = get_glue_client()

    db_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="db_name",
    )

    table_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="table_name",
    )

    table = glue.get_table(
        DatabaseName=db_name,
        Name=table_name,
    )

    print(f"Retrieved Glue table: {table}")


def list_tables(**context):
    """
    List tables in the dynamically generated Glue database.

    This provides additional evidence that the table is visible through
    the Glue Catalog API.
    """
    glue = get_glue_client()

    db_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="db_name",
    )

    table_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="table_name",
    )

    response = glue.get_tables(
        DatabaseName=db_name,
    )

    tables = response.get("TableList", [])

    print(f"Tables in {db_name}: {tables}")

    if not any(
        table.get("Name") == table_name
        for table in tables
    ):
        raise RuntimeError(
            f"Expected table {table_name} was not found in {db_name}"
        )


def update_table(**context):
    """
    Update table metadata in the Glue Data Catalog.
    """
    glue = get_glue_client()

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
            "version": "2",
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

    glue.update_table(
        DatabaseName=db_name,
        TableInput=table_input,
    )

    print(f"Updated Glue table: {db_name}.{table_name}")


def verify_updated_table(**context):
    """
    Verify that the table update is visible through the Glue Catalog.
    """
    glue = get_glue_client()

    db_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="db_name",
    )

    table_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="table_name",
    )

    table = glue.get_table(
        DatabaseName=db_name,
        Name=table_name,
    )["Table"]

    print(f"Updated table metadata: {table}")

    parameters = table.get("Parameters", {})

    if parameters.get("update_test") != "passed":
        raise RuntimeError(
            "Glue table update verification failed. "
            f"Parameters received: {parameters}"
        )

    if parameters.get("version") != "2":
        raise RuntimeError(
            "Glue table version update verification failed. "
            f"Parameters received: {parameters}"
        )

    columns = table.get("StorageDescriptor", {}).get("Columns", [])

    if not any(
        column.get("Name") == "updated"
        for column in columns
    ):
        raise RuntimeError(
            "Glue table update verification failed: "
            "'updated' column was not found."
        )

    print("Glue table update verified successfully.")


# ---------------------------------------------------------------------------
# Cleanup functions
# ---------------------------------------------------------------------------

def delete_table(**context):
    """
    Delete the test table.

    This task uses ALL_DONE so it runs even when the external Glue Job
    fails.
    """
    glue = get_glue_client()

    db_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="db_name",
    )

    table_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="table_name",
    )

    try:
        glue.delete_table(
            DatabaseName=db_name,
            Name=table_name,
        )

        print(f"Deleted Glue table: {db_name}.{table_name}")

    except glue.exceptions.EntityNotFoundException:
        print(
            f"Glue table {db_name}.{table_name} does not exist. "
            "Nothing to delete."
        )


def delete_database(**context):
    """
    Delete the dynamically generated Glue database.

    This task runs after delete_table regardless of the external Glue
    Job result.
    """
    glue = get_glue_client()

    db_name = context["ti"].xcom_pull(
        task_ids="generate_runtime_names",
        key="db_name",
    )

    try:
        glue.delete_database(
            Name=db_name,
        )

        print(f"Deleted Glue database: {db_name}")

    except glue.exceptions.EntityNotFoundException:
        print(
            f"Glue database {db_name} does not exist. "
            "Nothing to delete."
        )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="cwo_aws_glue_external_catalog_and_job",
    description=(
        "CWO AWS Glue Data Catalog CRUD plus external Glue Job "
        "trigger, monitoring, and cleanup test"
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

    generate_names = PythonOperator(
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

    list_tbls = PythonOperator(
        task_id="list_tables",
        python_callable=list_tables,
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
    # Trigger and monitor the existing AWS Glue Job.
    #
    # The Glue Job already points to:
    #
    # s3://qe-s3-bucket-weekly/cwo/cwo/glue-test/cwo_glue_test.py
    #
    # wait_for_completion=True means Airflow waits for the Glue Job to
    # reach a terminal state before continuing.
    # -----------------------------------------------------------------------

    run_external_glue_job = GlueJobOperator(
        task_id="run_external_glue_job",
        job_name=GLUE_JOB_NAME,
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
        verbose=True,
    )

    # -----------------------------------------------------------------------
    # Cleanup
    #
    # ALL_DONE means cleanup executes whether the Glue Job:
    #
    #   - succeeds
    #   - fails
    #   - is otherwise completed
    #
    # This prevents temporary Glue resources from being left behind when
    # the external workload fails.
    # -----------------------------------------------------------------------

    delete_tbl = PythonOperator(
        task_id="delete_table",
        python_callable=delete_table,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_db = PythonOperator(
        task_id="delete_database",
        python_callable=delete_database,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # -----------------------------------------------------------------------
    # Execution order
    # -----------------------------------------------------------------------

    (
        generate_names
        >> create_db
        >> get_db
        >> create_tbl
        >> get_tbl
        >> list_tbls
        >> update_tbl
        >> verify_update
        >> run_external_glue_job
        >> delete_tbl
        >> delete_db
    )

