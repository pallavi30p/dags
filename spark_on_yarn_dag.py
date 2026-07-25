"""
===============================================================================
DAG: spark_on_yarn_and_download_client_config
===============================================================================

Purpose
-------
Submit a Spark application to a CDP Base Spark-on-YARN cluster without storing
Hadoop/YARN client configuration files, Spark binaries, or Spark scripts 
in the DAG repository or Airflow worker image.

At runtime this single-task DAG:

1. Downloads the latest YARN Client Configuration ZIP from Cloudera Manager using
   REST API auto-discovery.
2. Auto-downloads Apache Spark 3.5.4 client binaries tarball to /tmp if 'spark-submit' 
   is not present in the local container image.
3. Dynamically generates the PySpark application script (pi.py) locally in /tmp.
4. Executes SparkSubmitHook in the local container with HADOOP_CONF_DIR pointing
   to the downloaded Cloudera configuration.
5. Cleans up temporary configuration and script files upon completion.
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
    "deploy-mode": "cluster"
}

===============================================================================
Repository Layout
===============================================================================

dags/
│
└── spark_on_yarn_dag.py

No Hadoop/YARN configuration files, Spark binaries, or external PySpark 
scripts are required in the repository.

===============================================================================
Execution Flow
===============================================================================

                 run_spark_on_yarn (Single Task)
                                │
   ┌────────────────────────────┼────────────────────────────┐
   ▼                            ▼                            ▼
Download CM          Ensure Spark Binaries           Generate Local 
Client Config           (Download to /tmp)           PySpark Script
   │                            │                            │
   └────────────────────────────┼────────────────────────────┘
                                │
                                ▼
                        SparkSubmitHook
                                │
                                ▼
                   Cleanup Temporary Resources

===============================================================================
"""
"""
===============================================================================
DAG: spark_on_yarn_and_download_client_config
===============================================================================

Purpose
-------
Submit a Spark application to a CDP Base Spark-on-YARN cluster without storing
Hadoop/YARN client configuration files, Spark binaries, or Spark scripts 
in the DAG repository or Airflow worker image.

At runtime this single-task DAG:

1. Downloads the latest YARN Client Configuration ZIP from Cloudera Manager using
   REST API auto-discovery.
2. Extracts and scans for the exact folder containing 'yarn-site.xml'.
3. Auto-downloads Apache Spark 3.5.4 client binaries tarball to /tmp if 'spark-submit' 
   is not present in the local container image.
4. Dynamically generates the PySpark application script (pi.py) locally in /tmp.
5. Executes SparkSubmitHook in the local container with HADOOP_CONF_DIR pointing
   directly to the active YARN configuration folder.
6. Cleans up temporary configuration and script files upon completion.
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
    """Download, extract, and locate YARN client configuration directory."""
    conn = BaseHook.get_connection("cm_yarn")
    tmp_dir = tempfile.mkdtemp(prefix="yarn-conf-")
    zip_path = os.path.join(tmp_dir, "client-config.zip")

    verify = True
    extra = conn.extra_dejson or {}

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

    extract_dir = os.path.join(tmp_dir, "conf")
    os.makedirs(extract_dir)

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)

    # 3. Locate exact folder containing yarn-site.xml / core-site.xml
    target_conf_dir = extract_dir
    for root, _, files in os.walk(extract_dir):
        if "yarn-site.xml" in files or "core-site.xml" in files:
            target_conf_dir = root
            print(f"Discovered active configuration files in: {target_conf_dir}")
            break

    return target_conf_dir


def _ensure_spark_binary() -> str:
    """Check for system spark-submit; download Spark tarball to /tmp if missing."""
    for cmd in ["spark-submit", "spark3-submit"]:
        path_binary = shutil.which(cmd)
        if path_binary:
            print(f"Found system Spark binary at: {path_binary}")
            return path_binary

    for path in ["/opt/spark/bin/spark-submit", "/usr/bin/spark-submit"]:
        if os.path.exists(path):
            print(f"Found static Spark binary at: {path}")
            return path

    tmp_spark_dir = "/tmp/spark_client"
    spark_submit_path = os.path.join(tmp_spark_dir, "spark-3.5.4-bin-hadoop3", "bin", "spark-submit")

    if os.path.exists(spark_submit_path):
        print(f"Using previously cached Spark binary at: {spark_submit_path}")
        return spark_submit_path

    print("spark-submit not found in container image. Downloading Apache Spark 3.5.4 binaries to /tmp...")
    os.makedirs(tmp_spark_dir, exist_ok=True)
    tgz_path = os.path.join(tmp_spark_dir, "spark.tgz")

    url = "https://archive.apache.org/dist/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz"
    response = requests.get(url, stream=True, timeout=300)
    response.raise_for_status()

    with open(tgz_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print("Unpacking Spark client archive...")
    shutil.unpack_archive(tgz_path, tmp_spark_dir)
    os.remove(tgz_path)

    st = os.stat(spark_submit_path)
    os.chmod(spark_submit_path, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    print(f"Successfully prepared Spark binary at {spark_submit_path}")
    return spark_submit_path


def _generate_spark_script() -> str:
    """Generate PySpark application script in local container /tmp."""
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
    Unified task that downloads configuration, downloads Spark binaries if needed, 
    generates script, and runs SparkSubmitHook in the local container context.
    """
    conf_dir = None
    script_path = None

    try:
        # Step 1: Download client config & locate yarn-site.xml directory
        conf_dir = _download_client_config()

        # Step 2: Ensure spark-submit is available
        spark_binary = _ensure_spark_binary()

        # Step 3: Generate PySpark application script
        script_path = _generate_spark_script()

        # Step 4: Export Hadoop & YARN environment variables explicitly
        os.environ["HADOOP_CONF_DIR"] = conf_dir
        os.environ["YARN_CONF_DIR"] = conf_dir

        print(f"Using HADOOP_CONF_DIR: {conf_dir}")
        print(f"Submitting Spark job to YARN using binary: {spark_binary}")

        # Step 5: Execute Spark submission
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
        # Step 6: Cleanup local config & application script
        if conf_dir:
            shutil.rmtree(os.path.dirname(conf_dir), ignore_errors=True)
            print("Cleaned up Hadoop configuration directory.")
        if script_path and os.path.exists(script_path):
            shutil.rmtree(os.path.dirname(script_path), ignore_errors=True)
            print("Cleaned up Spark script directory.")


with DAG(
    dag_id="spark_on_yarn_dag",
    description="Submit Spark jobs to CDP Base Spark-on-YARN using runtime-downloaded client configuration and binaries",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "yarn", "cdp", "cloudera"],
) as dag:

    run_spark_on_yarn()
