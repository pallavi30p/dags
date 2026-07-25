"""
===============================================================================
DAG: spark_yarn_download_client_config
===============================================================================

Purpose
-------
Submit a Spark application to a CDP Base Spark-on-YARN cluster without storing
Hadoop/YARN client configuration files in the DAG repository.

At runtime this DAG:

1. Downloads the latest YARN Client Configuration ZIP from Cloudera Manager.
2. Extracts it into a temporary directory.
3. Sets HADOOP_CONF_DIR and YARN_CONF_DIR for Spark.
4. Submits a Spark application using SparkSubmitOperator.
5. Cleans up temporary files after execution.

This approach ensures the DAG always uses the latest client configuration from
Cloudera Manager
===============================================================================
Airflow Connection #1 (Required)
===============================================================================

Connection ID
    cm_yarn
Connection Type
    HTTP
Host
    https://<cm-host>:7183/cmf/services/<yarn-service-id>
Example
    https://ccycloud-1.demo1.root.comops.site:7183/cmf/services/1546335666
Login
    <cloudera-manager-username>
Password
    <cloudera-manager-password>
Extra (Optional)
{
    "ca_cert_base64": "<BASE64_ENCODED_CA_CERTIFICATE>"
}
Notes

• Host should point to the YARN service (NOT the CM home page).
• The DAG downloads the latest client configuration using:
      GET <host>/client-config
• If the Cloudera Manager certificate is publicly trusted,
  ca_cert_base64 is not required.
  
===============================================================================
Airflow Connection #2 (Required)
===============================================================================

Connection ID
    spark_yarn
Connection Type
    Spark
Host
    yarn
Extra
{
    "deploy-mode": "cluster",
    "spark_binary": "/opt/spark/bin/spark-submit"
}
Example
{
    "deploy-mode": "cluster",
    "spark_binary": "/opt/spark-3.5.4/bin/spark-submit"
}

===============================================================================
Repository Layout
===============================================================================

dags/
│
├── spark_yarn_download_client_config.py
└── pi.py

No Hadoop/YARN configuration files are required in the repository.

===============================================================================
Execution Flow
===============================================================================

download_client_config
        │
        ▼
extract ZIP to /tmp/yarn-conf-xxxx/conf
        │
        ▼
SparkSubmitOperator
        │
        ▼
cleanup temporary directory

===============================================================================
"""

import base64
import os
import shutil
import tempfile
import zipfile
from datetime import datetime

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)


@task
def download_client_config():
    """
    Download and extract the latest Hadoop/YARN client configuration.

    This task:

    1. Reads the Cloudera Manager connection (cm_yarn).
    2. Downloads the latest YARN client configuration ZIP.
    3. Optionally decodes the Base64 CA certificate from the Airflow
       connection Extra field.
    4. Extracts the ZIP into a temporary directory.
    5. Returns the extracted configuration directory via XCom.

    Returns
    -------
    str
        Absolute path of the extracted Hadoop configuration directory.

    Example return value

        /tmp/yarn-conf-abcd1234/conf
    """

    conn = BaseHook.get_connection("cm_yarn")

    tmp_dir = tempfile.mkdtemp(prefix="yarn-conf-")
    zip_path = os.path.join(tmp_dir, "client-config.zip")

    verify = True

    extra = conn.extra_dejson

    #
    # Optional CA certificate supplied as Base64
    #
    if "ca_cert_base64" in extra:

        cert_path = os.path.join(tmp_dir, "cm-ca.pem")

        with open(cert_path, "wb") as fp:
            fp.write(base64.b64decode(extra["ca_cert_base64"]))

        verify = cert_path

    url = f"{conn.host.rstrip('/')}/client-config"

    response = requests.get(
        url,
        auth=(conn.login, conn.password),
        verify=verify,
        timeout=120,
    )

    response.raise_for_status()

    with open(zip_path, "wb") as fp:
        fp.write(response.content)

    conf_dir = os.path.join(tmp_dir, "conf")
    os.makedirs(conf_dir)

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(conf_dir)

    print(f"Downloaded Hadoop configuration to {conf_dir}")

    return conf_dir


@task(trigger_rule="all_done")
def cleanup(conf_dir: str):
    """
    Delete the temporary Hadoop configuration directory.

    Parameters
    ----------
    conf_dir : str
        Directory returned by download_client_config().

    Notes
    -----
    This task ignores cleanup failures so that they do not mask the
    success/failure of the Spark job itself.
    """

    shutil.rmtree(os.path.dirname(conf_dir), ignore_errors=True)

    print(f"Cleaned up {os.path.dirname(conf_dir)}")


with DAG(
    dag_id="spark_yarn_download_client_config",
    description="Submit Spark jobs to CDP Base Spark-on-YARN using runtime-downloaded client configuration",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "yarn", "cdp", "cloudera"],
) as dag:

    #
    # Download the latest Hadoop/YARN client configuration from
    # Cloudera Manager.
    #
    conf_dir = download_client_config()

    #
    # Submit the Spark application.
    #
    # SparkSubmitOperator uses the Hadoop configuration downloaded
    # in the previous task via HADOOP_CONF_DIR and YARN_CONF_DIR.
    # No Hadoop configuration is stored in Git.
    #
    spark_submit = SparkSubmitOperator(
        task_id="spark_submit",

        # Spark application to execute
        application="/usr/local/airflow/dags/pi.py",

        # Airflow Spark connection
        conn_id="spark_yarn",

        deploy_mode="cluster",

        env_vars={
            "HADOOP_CONF_DIR": "{{ ti.xcom_pull(task_ids='download_client_config') }}",
            "YARN_CONF_DIR": "{{ ti.xcom_pull(task_ids='download_client_config') }}",
        },

        conf={
            "spark.master": "yarn",
        },

        verbose=True,
    )

    cleanup_task = cleanup(conf_dir)

    conf_dir >> spark_submit >> cleanup_task
