"""
===============================================================================
DAG: spark_on_yarn_and_download_client_config
===============================================================================

Purpose
-------
Submit a Spark application to a CDP Base Spark-on-YARN cluster without storing
Hadoop/YARN client configuration files or Spark scripts in the DAG repository.

At runtime this DAG:

1. Downloads the latest YARN Client Configuration ZIP from Cloudera Manager using
   the official REST API auto-discovery mechanism.
2. Extracts it into a temporary directory.
3. Dynamically generates the PySpark application script (pi.py) in a temp location.
4. Sets HADOOP_CONF_DIR and YARN_CONF_DIR for Spark.
5. Submits a Spark application using SparkSubmitOperator.
6. Cleans up temporary files and directories after execution.

This approach ensures the DAG always uses the latest client configuration from
Cloudera Manager and remains completely self-contained in a single file.
===============================================================================
Airflow Connection #1 (Required)
===============================================================================

Connection ID
    cm_yarn
Connection Type
    HTTP
Host
    https://<cm-host>:7183
Example
    https://ccycloud-1.demo1.root.comops.site:7183
Login
    <cloudera-manager-username>
Password
    <cloudera-manager-password>
Extra (Optional)
{
    "ca_cert_base64": "<BASE64_ENCODED_CA_CERTIFICATE>"
}
Notes

• Host can point to either the CM base URL or the YARN UI page (auto-discovery
  will extract the base host automatically).
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
└── spark_on_yarn_dag.py

No Hadoop/YARN configuration files or external PySpark scripts are required 
in the repository.

===============================================================================
Execution Flow
===============================================================================

download_client_config          generate_spark_script
          │                               │
          │  (Extracts to /tmp)           │  (Writes /tmp/spark-app-xxx/pi.py)
          └───────────────┬───────────────┘
                          │
                          ▼
                 SparkSubmitOperator
                          │
                          ▼
             cleanup temporary resources

===============================================================================
"""

import base64
import os
import shutil
import tempfile
import zipfile
from datetime import datetime
from urllib.parse import quote, urlparse

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)


@task
def generate_spark_script() -> str:
    """
    Dynamically generate the PySpark application script (pi.py) at runtime.

    Returns
    -------
    str
        Absolute path of the generated PySpark script.
    """
    tmp_dir = tempfile.mkdtemp(prefix="spark-app-")
    script_path = os.path.join(tmp_dir, "pi.py")

    script_content = """
import sys
from random import random
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PythonPi").getOrCreate()
    partitions = 2
    n = 100000 * partitions

    def f(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(lambda a, b: a + b)
    print(f"Pi is roughly {4.0 * count / n}")
    spark.stop()
"""
    with open(script_path, "w") as fp:
        fp.write(script_content.strip())

    print(f"Generated PySpark script at {script_path}")

    return script_path


@task
def download_client_config() -> str:
    """
    Download and extract the latest Hadoop/YARN client configuration.

    This task:
    1. Reads the Cloudera Manager connection (cm_yarn).
    2. Auto-detects the highest supported CM REST API version.
    3. Auto-discovers the cluster name and YARN service name.
    4. Downloads the client config ZIP via the official REST API endpoint.
    5. Extracts the ZIP into a temporary directory and returns its path.

    Returns
    -------
    str
        Absolute path of the extracted Hadoop configuration directory.
    """

    conn = BaseHook.get_connection("cm_yarn")

    tmp_dir = tempfile.mkdtemp(prefix="yarn-conf-")
    zip_path = os.path.join(tmp_dir, "client-config.zip")

    verify = True
    extra = conn.extra_dejson

    # Handle Base64 CA Certificate if provided in connection Extra
    if "ca_cert_base64" in extra:
        cert_path = os.path.join(tmp_dir, "cm-ca.pem")
        with open(cert_path, "wb") as fp:
            fp.write(base64.b64decode(extra["ca_cert_base64"]))
        verify = cert_path

    # Extract base URL (e.g. https://ccycloud-1.demo1.root.comops.site:7183)
    parsed = urlparse(conn.host)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    session = requests.Session()
    session.auth = (conn.login, conn.password)

    # 1. Determine highest supported API version
    version_url = f"{base_url}/api/version"
    try:
        v_resp = session.get(version_url, verify=verify, timeout=30)
        v_resp.raise_for_status()
        api_version = v_resp.text.strip().strip('"')
        if not api_version.startswith("v"):
            api_version = f"v{api_version}"
    except Exception as err:
        print(f"Could not auto-detect CM API version ({err}), falling back to v41")
        api_version = "v41"

    print(f"Using Cloudera Manager REST API version: {api_version}")

    # 2. Discover Clusters & YARN Service Name
    clusters_url = f"{base_url}/api/{api_version}/clusters"
    c_resp = session.get(clusters_url, verify=verify, timeout=30)
    c_resp.raise_for_status()
    clusters = c_resp.json().get("items", [])

    client_config_url = None

    for cluster in clusters:
        cluster_name = cluster.get("name")
        encoded_cluster = quote(cluster_name)
        services_url = f"{base_url}/api/{api_version}/clusters/{encoded_cluster}/services"
        s_resp = session.get(services_url, verify=verify, timeout=30)
        
        if s_resp.status_code != 200:
            continue

        services = s_resp.json().get("items", [])
        for svc in services:
            if svc.get("type") == "YARN":
                svc_name = quote(svc.get("name"))
                client_config_url = (
                    f"{base_url}/api/{api_version}/clusters/{encoded_cluster}/services/{svc_name}/clientConfig"
                )
                print(f"Discovered YARN service '{svc.get('name')}' in cluster '{cluster_name}'")
                break
        if client_config_url:
            break

    # Fallback to direct URL if auto-discovery fails
    if not client_config_url:
        print("Auto-discovery could not find YARN service via REST API. Attempting fallback URL...")
        client_config_url = conn.host if "clientConfig" in conn.host else f"{conn.host.rstrip('/')}/clientConfig"

    print(f"Downloading client configuration from: {client_config_url}")
    response = session.get(client_config_url, verify=verify, timeout=120)
    response.raise_for_status()

    with open(zip_path, "wb") as fp:
        fp.write(response.content)

    conf_dir = os.path.join(tmp_dir, "conf")
    os.makedirs(conf_dir)

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(conf_dir)

    print(f"Successfully downloaded and extracted Hadoop configuration to {conf_dir}")

    return conf_dir


@task(trigger_rule="all_done")
def cleanup(conf_dir: str, script_path: str):
    """
    Delete temporary Hadoop configuration and Spark script directories.

    Parameters
    ----------
    conf_dir : str
        Directory returned by download_client_config().
    script_path : str
        Path returned by generate_spark_script().

    Notes
    -----
    This task ignores cleanup failures so that they do not mask the
    success/failure of the Spark job itself.
    """

    if conf_dir:
        shutil.rmtree(os.path.dirname(conf_dir), ignore_errors=True)
        print(f"Cleaned up {os.path.dirname(conf_dir)}")

    if script_path and os.path.exists(script_path):
        shutil.rmtree(os.path.dirname(script_path), ignore_errors=True)
        print(f"Cleaned up {os.path.dirname(script_path)}")


with DAG(
    dag_id="spark_on_yarn_dag",
    description="Submit Spark jobs to CDP Base Spark-on-YARN using runtime-downloaded client configuration",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "yarn", "cdp", "cloudera"],
) as dag:

    #
    # Download the latest Hadoop/YARN client configuration from
    # Cloudera Manager using REST API auto-discovery.
    #
    conf_dir = download_client_config()

    #
    # Dynamically generate the Spark application script on the Airflow worker.
    #
    script_path = generate_spark_script()

    #
    # Submit the Spark application.
    #
    # SparkSubmitOperator uses the Hadoop configuration downloaded
    # in the previous task via HADOOP_CONF_DIR and YARN_CONF_DIR.
    # Neither Hadoop configuration nor Spark script files are stored in Git.
    #
    spark_submit = SparkSubmitOperator(
        task_id="spark_submit",

        # Spark application to execute (dynamically generated at runtime)
        application=script_path,

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

    cleanup_task = cleanup(conf_dir, script_path)

    [conf_dir, script_path] >> spark_submit >> cleanup_task
