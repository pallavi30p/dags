"""
CWO - AWS Glue Data Catalog CRUD Integration Test
==================================================

Purpose
-------
Validates CWO/Airflow integration with AWS Glue Data Catalog using
an Airflow-managed AWS Connection.

The DAG performs:

    1. Create database
    2. Read database
    3. Create table
    4. Read table
    5. List tables
    6. Update table
    7. Verify update
    8. Delete table
    9. Delete database

Dynamic resource naming
-----------------------
A unique suffix is generated for every DAG run.

Example:

    cwo_test_db_20260727_064512
    cwo_test_table_20260727_064512

This prevents collisions when:
    - the DAG is manually triggered multiple times
    - multiple DAG runs execute
    - a previous run did not clean up successfully

Airflow Connection
------------------
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

AWS permissions required
------------------------
    glue:CreateDatabase
    glue:GetDatabase
    glue:DeleteDatabase

    glue:CreateTable
    glue:GetTable
    glue:GetTables
    glue:UpdateTable
    glue:DeleteTable

AWS region
----------
    us-west-2

Important
---------
AWS credentials are NOT stored in this DAG.
They are retrieved from the Airflow Connection.

The DAG only operates on dynamically generated test resources.
Existing resources such as "iceberg_data" are not modified.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AWS_CONN_ID = "aws_glue_conn"
AWS_REGION = "us-west-2"


# ---------------------------------------------------------------------------
# Dynamic resource names
# ---------------------------------------------------------------------------

def get_resource_names(**context):
    """
    Generate unique Glue database/table names for this DAG run.

    The execution timestamp is used instead of the full Airflow run_id
    because Glue resource names have naming restrictions and we want
    predictable, short names.
    """

    logical_date = context["logical_date"]

    suffix = logical_date.strftime("%Y%m%d_%H%M%S")

    database_name = f"cwo_test_db_{suffix}"
    table_name = f"cwo_test_table_{suffix}"

    print(f"Glue database: {database_name}")
    print(f"Glue table:    {table_name}")

    return {
        "database": database_name,
        "table": table_name,
    }


# ---------------------------------------------------------------------------
# AWS Glue client
# ---------------------------------------------------------------------------

def get_glue_client():
    """
    Create a boto3 Glue client using the Airflow AWS Connection.

    AWS credentials are retrieved from:
        aws_glue_conn
    """

    hook = AwsBaseHook(
        aws_conn_id=AWS_CONN_ID,
        client_type="glue",
        region_name=AWS_REGION,
    )

    return hook.get_conn()


# ---------------------------------------------------------------------------
# Create database
# ---------------------------------------------------------------------------

def create_database(**context):
    """Create a dynamically named Glue database."""

    glue = get_glue_client()

    names = context["ti"].xcom_pull(
        task_ids="generate_resource_names"
    )

    database_name = names["database"]

    print(f"Creating Glue database: {database_name}")

    glue.create_database(
        DatabaseInput={
            "Name": database_name,
            "Description": (
                "CWO AWS Glue CRUD integration test"
            ),
            "Parameters": {
                "created_by": "cwo-airflow",
                "test": "true",
            },
        }
    )

    print(f"Successfully created: {database_name}")


# ---------------------------------------------------------------------------
# Get database
# ---------------------------------------------------------------------------

def get_database(**context):
    """Read and verify the dynamically created Glue database."""

    glue = get_glue_client()

    names = context["ti"].xcom_pull(
        task_ids="generate_resource_names"
    )

    database_name = names["database"]

    response = glue.get_database(
        Name=database_name
    )

    database = response["Database"]

    print("Successfully retrieved Glue database")
    print(f"Name:        {database['Name']}")
    print(f"Description: {database.get('Description')}")
    print(f"Catalog ID:  {database.get('CatalogId')}")

    assert database["Name"] == database_name


# ---------------------------------------------------------------------------
# Create table
# ---------------------------------------------------------------------------

def create_table(**context):
    """Create a dynamically named Glue table."""

    glue = get_glue_client()

    names = context["ti"].xcom_pull(
        task_ids="generate_resource_names"
    )

    database_name = names["database"]
    table_name = names["table"]

    print(
        f"Creating Glue table: "
        f"{database_name}.{table_name}"
    )

    glue.create_table(
        DatabaseName=database_name,
        TableInput={
            "Name": table_name,
            "Description": (
                "CWO AWS Glue CRUD integration test table"
            ),
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "created_by": "cwo-airflow",
                "test": "true",
                "version": "1",
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
                "Location": "s3://cwo-glue-test-placeholder/",
            },
        },
    )

    print(
        f"Successfully created: "
        f"{database_name}.{table_name}"
    )


# ---------------------------------------------------------------------------
# Get table
# ---------------------------------------------------------------------------

def get_table(**context):
    """Read and verify the Glue table."""

    glue = get_glue_client()

    names = context["ti"].xcom_pull(
        task_ids="generate_resource_names"
    )

    database_name = names["database"]
    table_name = names["table"]

    response = glue.get_table(
        DatabaseName=database_name,
        Name=table_name,
    )

    table = response["Table"]

    print("Successfully retrieved Glue table")
    print(f"Database:    {table['DatabaseName']}")
    print(f"Table:       {table['Name']}")
    print(f"Description: {table.get('Description')}")
    print(f"Parameters:  {table.get('Parameters')}")
    print(
        f"Columns:     "
        f"{table['StorageDescriptor']['Columns']}"
    )

    assert table["Name"] == table_name
    assert table["DatabaseName"] == database_name


# ---------------------------------------------------------------------------
# Get tables
# ---------------------------------------------------------------------------

def get_tables(**context):
    """List tables in the dynamically created database."""

    glue = get_glue_client()

    names = context["ti"].xcom_pull(
        task_ids="generate_resource_names"
    )

    database_name = names["database"]
    table_name = names["table"]

    response = glue.get_tables(
        DatabaseName=database_name
    )

    tables = response.get("TableList", [])

    table_names = [
        table["Name"]
        for table in tables
    ]

    print(
        f"Tables in {database_name}: "
        f"{table_names}"
    )

    assert table_name in table_names


# ---------------------------------------------------------------------------
# Update table
# ---------------------------------------------------------------------------

def update_table(**context):
    """Update the dynamically created Glue table."""

    glue = get_glue_client()

    names = context["ti"].xcom_pull(
        task_ids="generate_resource_names"
    )

    database_name = names["database"]
    table_name = names["table"]

    print(
        f"Updating Glue table: "
        f"{database_name}.{table_name}"
    )

    glue.update_table(
        DatabaseName=database_name,
        TableInput={
            "Name": table_name,
            "Description": (
                "UPDATED by CWO AWS Glue CRUD integration test"
            ),
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "created_by": "cwo-airflow",
                "test": "true",
                "version": "2",
                "updated": "true",
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
                        "Name": "email",
                        "Type": "string",
                    },
                ],
                "Location": "s3://cwo-glue-test-placeholder/",
            },
        },
    )

    print(
        f"Successfully updated: "
        f"{database_name}.{table_name}"
    )


# ---------------------------------------------------------------------------
# Verify update
# ---------------------------------------------------------------------------

def verify_updated_table(**context):
    """Verify the table update."""

    glue = get_glue_client()

    names = context["ti"].xcom_pull(
        task_ids="generate_resource_names"
    )

    database_name = names["database"]
    table_name = names["table"]

    response = glue.get_table(
        DatabaseName=database_name,
        Name=table_name,
    )

    table = response["Table"]

    print(f"Description: {table.get('Description')}")
    print(f"Parameters:  {table.get('Parameters')}")

    columns = table["StorageDescriptor"]["Columns"]

    print(f"Columns: {columns}")

    assert (
        table["Description"]
        == "UPDATED by CWO AWS Glue CRUD integration test"
    )

    assert table["Parameters"]["version"] == "2"
    assert table["Parameters"]["updated"] == "true"

    column_names = [
        column["Name"]
        for column in columns
    ]

    assert "email" in column_names

    print("Table update verification successful")


# ---------------------------------------------------------------------------
# Delete table
# ---------------------------------------------------------------------------

def delete_table(**context):
    """Delete the dynamically created Glue table."""

    glue = get_glue_client()

    names = context["ti"].xcom_pull(
        task_ids="generate_resource_names"
    )

    database_name = names["database"]
    table_name = names["table"]

    print(
        f"Deleting Glue table: "
        f"{database_name}.{table_name}"
    )

    glue.delete_table(
        DatabaseName=database_name,
        Name=table_name,
    )

    print(
        f"Successfully deleted: "
        f"{database_name}.{table_name}"
    )


# ---------------------------------------------------------------------------
# Delete database
# ---------------------------------------------------------------------------

def delete_database(**context):
    """Delete the dynamically created Glue database."""

    glue = get_glue_client()

    names = context["ti"].xcom_pull(
        task_ids="generate_resource_names"
    )

    database_name = names["database"]

    print(f"Deleting Glue database: {database_name}")

    glue.delete_database(
        Name=database_name
    )

    print(
        f"Successfully deleted: {database_name}"
    )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="cwo_aws_glue_catalog_crud",
    description=(
        "CWO AWS Glue Data Catalog CRUD integration test "
        "with dynamically generated resources"
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=[
        "cwo",
        "aws",
        "glue",
        "metadata-store",
        "crud",
        "external-platform",
    ],
) as dag:

    generate_names = PythonOperator(
        task_id="generate_resource_names",
        python_callable=get_resource_names,
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
        task_id="get_tables",
        python_callable=get_tables,
    )

    update_tbl = PythonOperator(
        task_id="update_table",
        python_callable=update_table,
    )

    verify_update = PythonOperator(
        task_id="verify_updated_table",
        python_callable=verify_updated_table,
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
    # DAG execution order
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
        >> delete_tbl
        >> delete_db
    )
