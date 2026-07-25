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
"""
===============================================================================
DAG: spark_on_yarn_and_download_client_config
===============================================================================

Purpose
-------
Submit a Spark application to a CDP Base Spark-on-YARN cluster without storing
Hadoop/YARN client configuration files or Spark scripts in the DAG repository.

At runtime this single-task DAG:

1. Downloads the latest YARN Client Configuration ZIP from Cloudera Manager using
   REST API auto-discovery.
2. Extracts it into a temporary directory in the local container.
3. Dynamically generates the PySpark application script (pi.py) locally.
4. Executes SparkSubmitHook in the same container with local HADOOP_CONF_DIR.
5. Cleans up all temporary files and directories upon completion.
===============================================================================
"""

import base64
import os
import shutil
import stat
import tempfile
import zipfile
from datetime import datetime
from urllib.parse import quote, urlparse

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook


def _download_client_config() -> str:
    """Download and extract YARN client configuration to local container /tmp."""
    conn = BaseHook.get_connection("cm_yarn")
    tmp_dir = tempfile.mkdtemp(prefix="yarn-conf-")
    zip_path = os.path.join(tmp_dir, "client-config.zip")

    verify = True
    extra = conn.extra_dejson

    if "ca_cert_base64" in extra:
        cert_path = os.path.join(tmp_dir, "cm-ca.pem")
        with open(cert_path, "wb") as fp:
            fp.write(base64.b64decode(extra["ca_cert_base64"]))
        verify = cert_path

    parsed = urlparse(conn.host)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    session = requests.Session()
    session.auth = (conn.login, conn.password)

    # 1. Determine API version
    version_url = f"{base_url}/api/version"
    try:
        v_resp = session.get(version_url, verify=verify, timeout=30)
        v_resp.raise_for_status()
        api_version = v_resp.text.strip().strip('"')
        if not api_version.startswith("v"):
            api_version = f"v{api_version}"
    except Exception:
        api_version = "v57"

    # 2. Discover YARN service
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

        for svc in s_resp.json().get("items", []):
            if svc.get("type") == "YARN":
                svc_name = quote(svc.get("name"))
                client_config_url = (
                    f"{base_url}/api/{api_version}/clusters/{encoded_cluster}/services/{svc_name}/clientConfig"
                )
                print(f"Discovered YARN service '{svc.get('name')}' in cluster '{cluster_name}'")
                break
        if client_config_url:
            break

    if not client_config_url:
        client_config_url = f"{conn.host.rstrip('/')}/clientConfig"

    print(f"Downloading client configuration from: {client_config_url}")
    response = session.get(client_config_url, verify=verify, timeout=120)
    response.raise_for_status()

    with open(zip_path, "wb") as fp:
        fp.write(response.content)

    conf_dir = os.path.join(tmp_dir, "conf")
    os.makedirs(conf_dir)

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(conf_dir)

    print(f"Extracted Hadoop configuration to {conf_dir}")
    return conf_dir


def _generate_spark_script() -> str:
    """Generate PySpark script in local container /tmp."""
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
def run_spark_on_yarn():
    """
    Unified task that downloads configuration, generates script, and runs 
    SparkSubmitHook within the exact same container context.
    """
    conf_dir = None
    script_path = None

    try:
        # Step 1: Download client config
        conf_dir = _download_client_config()

        # Step 2: Generate Spark script
        script_path = _generate_spark_script()

        # Step 3: Resolve Spark binary path & permissions
        spark_conn = BaseHook.get_connection("spark_yarn")
        spark_extra = spark_conn.extra_dejson or {}
        spark_binary = spark_extra.get("spark_binary", "spark-submit")

        if os.path.isabs(spark_binary) and os.path.exists(spark_binary):
            if not os.access(spark_binary, os.X_OK):
                try:
                    st = os.stat(spark_binary)
                    os.chmod(spark_binary, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
                    print(f"Granted execute permissions to {spark_binary}")
                except Exception as e:
                    print(f"Warning: Could not grant execution permission on {spark_binary}: {e}")

        # Step 4: Execute Spark submission using SparkSubmitHook
        print("Submitting Spark job to YARN...")
        spark_hook = SparkSubmitHook(
            conn_id="spark_yarn",
            deploy_mode="cluster",
            env_vars={
                "HADOOP_CONF_DIR": conf_dir,
                "YARN_CONF_DIR": conf_dir,
            },
            conf={
                "spark.master": "yarn",
            },
            spark_binary=spark_binary,
            verbose=True,
        )

        spark_hook.submit(application=script_path)

    finally:
        # Step 5: Cleanup local files
        if conf_dir:
            shutil.rmtree(os.path.dirname(conf_dir), ignore_errors=True)
            print("Cleaned up Hadoop configuration directory.")
        if script_path and os.path.exists(script_path):
            shutil.rmtree(os.path.dirname(script_path), ignore_errors=True)
            print("Cleaned up Spark script directory.")


with DAG(
    dag_id="spark_on_yarn_dag",
    description="Submit Spark jobs to CDP Base Spark-on-YARN using runtime-downloaded client configuration",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "yarn", "cdp", "cloudera"],
) as dag:

    run_spark_on_yarn()
