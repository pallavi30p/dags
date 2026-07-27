"""
CWO - AWS Glue Data Catalog CRUD Integration Test
==================================================

Purpose
-------
This DAG validates CWO/Airflow integration with the AWS Glue Data Catalog
using an Airflow-managed AWS Connection.

The DAG performs the complete lifecycle of temporary Glue metadata:

    1. Create database
    2. Read database
    3. Create table
    4. Read table
    5. List tables
    6. Update table
    7. Read table and verify update
    8. Delete table
    9. Delete database

AWS credentials are NOT stored in this DAG.

Airflow Connection
------------------
Create this connection manually in:

    Airflow UI
      -> Admin
      -> Connections
      -> Add Connection

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
The AWS identity used by aws_glue_conn needs:

    glue:CreateDatabase
    glue:GetDatabase
    glue:DeleteDatabase

    glue:CreateTable
    glue:GetTable
    glue:GetTables
    glue:UpdateTable
    glue:DeleteTable

Test resources
--------------
Database:
    cwo_test_db

Table:
    cwo_test_table

IMPORTANT
---------
The DAG deletes the test database and table at the end.

It only operates on the resources created by this DAG.
It does NOT touch existing databases such as "iceberg_data".
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

GLUE_DATABASE = "cwo_test_db"
GLUE_TABLE = "cwo_test_table"


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def get_glue_client():
    """
    Return a boto3 Glue client using the Airflow AWS Connection.

    Credentials come from:
        aws_glue_conn

    They are intentionally not hardcoded in the DAG.
    """

    hook = AwsBaseHook(
        aws_conn_id=AWS_CONN_ID,
        client_type="glue",
        region_name=AWS_REGION,
    )

    return hook.get_conn()


# ---------------------------------------------------------------------------
# Database CRUD
# ---------------------------------------------------------------------------

def create_database():
    """Create the temporary Glue database."""

    glue = get_glue_client()

    print(f"Creating Glue database: {GLUE_DATABASE}")

    glue.create_database(
        DatabaseInput={
            "Name": GLUE_DATABASE,
            "Description": "CWO AWS Glue CRUD integration test",
            "Parameters": {
                "created_by": "cwo-airflow",
                "test": "true",
            },
        }
    )

    print(f"Successfully created database: {GLUE_DATABASE}")


def get_database():
    """Read the Glue database and verify that it exists."""

    glue = get_glue_client()

    response = glue.get_database(
        Name=GLUE_DATABASE
    )

    database = response["Database"]

    print("Successfully retrieved Glue database")
    print(f"Database name: {database['Name']}")
    print(f"Description: {database.get('Description')}")
    print(f"Catalog ID: {database.get('CatalogId')}")

    assert database["Name"] == GLUE_DATABASE


# ---------------------------------------------------------------------------
# Table CRUD
# ---------------------------------------------------------------------------

def create_table():
    """Create a temporary Glue table."""

    glue = get_glue_client()

    print(
        f"Creating Glue table: "
        f"{GLUE_DATABASE}.{GLUE_TABLE}"
    )

    glue.create_table(
        DatabaseName=GLUE_DATABASE,
        TableInput={
            "Name": GLUE_TABLE,
            "Description": "CWO AWS Glue CRUD integration test table",
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
        f"Successfully created table: "
        f"{GLUE_DATABASE}.{GLUE_TABLE}"
    )


def get_table():
    """Read the Glue table and verify metadata."""

    glue = get_glue_client()

    response = glue.get_table(
        DatabaseName=GLUE_DATABASE,
        Name=GLUE_TABLE,
    )

    table = response["Table"]

    print("Successfully retrieved Glue table")
    print(f"Database: {table['DatabaseName']}")
    print(f"Table: {table['Name']}")
    print(f"Description: {table.get('Description')}")
    print(f"Table type: {table.get('TableType')}")
    print(
        f"Columns: "
        f"{table['StorageDescriptor']['Columns']}"
    )
    print(f"Parameters: {table.get('Parameters')}")

    assert table["Name"] == GLUE_TABLE
    assert table["DatabaseName"] == GLUE_DATABASE


def get_tables():
    """List all tables in the test database."""

    glue = get_glue_client()

    response = glue.get_tables(
        DatabaseName=GLUE_DATABASE
    )

    tables = response.get("TableList", [])

    table_names = [
        table["Name"]
        for table in tables
    ]

    print(
        f"Tables in database {GLUE_DATABASE}: "
        f"{table_names}"
    )

    assert GLUE_TABLE in table_names


def update_table():
    """Update the Glue table metadata."""

    glue = get_glue_client()

    print(
        f"Updating Glue table: "
        f"{GLUE_DATABASE}.{GLUE_TABLE}"
    )

    glue.update_table(
        DatabaseName=GLUE_DATABASE,
        TableInput={
            "Name": GLUE_TABLE,
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
        f"Successfully updated table: "
        f"{GLUE_DATABASE}.{GLUE_TABLE}"
    )


def verify_updated_table():
    """Verify that the table update was applied."""

    glue = get_glue_client()

    response = glue.get_table(
        DatabaseName=GLUE_DATABASE,
        Name=GLUE_TABLE,
    )

    table = response["Table"]

    print("Verifying updated Glue table")

    print(f"Description: {table.get('Description')}")
    print(f"Parameters: {table.get('Parameters')}")
    print(
        f"Columns: "
        f"{table['StorageDescriptor']['Columns']}"
    )

    assert (
        table["Description"]
        == "UPDATED by CWO AWS Glue CRUD integration test"
    )

    assert table["Parameters"]["version"] == "2"
    assert table["Parameters"]["updated"] == "true"

    columns = table["StorageDescriptor"]["Columns"]

    column_names = [
        column["Name"]
        for column in columns
    ]

    assert "email" in column_names

    print("Table update verification successful")


# ---------------------------------------------------------------------------
# Delete operations
# ---------------------------------------------------------------------------

def delete_table():
    """Delete the temporary Glue table."""

    glue = get_glue_client()

    print(
        f"Deleting Glue table: "
        f"{GLUE_DATABASE}.{GLUE_TABLE}"
    )

    glue.delete_table(
        DatabaseName=GLUE_DATABASE,
        Name=GLUE_TABLE,
    )

    print(
        f"Successfully deleted table: "
        f"{GLUE_DATABASE}.{GLUE_TABLE}"
    )


def delete_database():
    """Delete the temporary Glue database."""

    glue = get_glue_client()

    print(
        f"Deleting Glue database: {GLUE_DATABASE}"
    )

    glue.delete_database(
        Name=GLUE_DATABASE
    )

    print(
        f"Successfully deleted database: "
        f"{GLUE_DATABASE}"
    )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="cwo_aws_glue_catalog_crud",
    description=(
        "CWO integration test for AWS Glue Data Catalog "
        "CRUD operations using a manually configured "
        "Airflow AWS Connection"
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

    # Database
    create_db = PythonOperator(
        task_id="create_database",
        python_callable=create_database,
    )

    get_db = PythonOperator(
        task_id="get_database",
        python_callable=get_database,
    )

    # Table
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

    # Cleanup
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
        create_db
        >> get_db
        >> create_tbl
        >> get_tbl
        >> list_tbls
        >> update_tbl
        >> verify_update
        >> delete_tbl
        >> delete_db
    )
