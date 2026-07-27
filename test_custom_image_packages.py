import importlib
from datetime import datetime

from airflow.decorators import dag, task


default_args = {
    "owner": "pallavip",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 0,
}


@dag(
    dag_id="verify_custom_cwo_image",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["validation", "custom_image", "cwo"],
    doc_md="""
    ### CWO Custom Image Package Verification

    Validates that newly added custom libraries and providers included in the
    custom CWO Airflow image are successfully installed and importable on
    Airflow worker pods.

    The validation covers:
    - Custom Python libraries added through `custom-requirements.txt`
    - Custom Airflow providers added to the image
    - Standard providers expected to be available in the base Cloudera CWO image

    The DAG fails if any required package or provider cannot be imported.
    """,
)
def custom_image_verification():
    """Check custom providers and libraries added as part of the CWO custom Airflow image."""

    @task
    def check_custom_packages():
        """
        Verify custom libraries and providers added as part of the custom CWO image.

        These dependencies are installed separately from the base Airflow image
        using the packages defined in custom-requirements.txt.
        """

        custom_modules = [
            ("DuckDB", "duckdb"),
            ("PyMuPDF (Fitz)", "fitz"),
            ("Great Expectations", "great_expectations"),
            ("Polars", "polars"),
            ("MS Fabric Provider", "airflow.providers.microsoft.fabric"),
            ("Tencent Cloud SDK", "tencentcloud"),
        ]

        results = []
        failed = False

        print("\n=== CHECKING CUSTOM PACKAGES ===")

        for name, module_name in custom_modules:
            try:
                mod = importlib.import_module(module_name)

                version = getattr(mod, "__version__", "Present")

                msg = (
                    f"✅ SUCCESS: {name} ({module_name}) "
                    f"-> Version: {version}"
                )

                print(msg)
                results.append(msg)

            except ImportError as e:
                failed = True

                msg = (
                    f"❌ FAILED: {name} ({module_name}) "
                    f"-> Error: {str(e)}"
                )

                print(msg)
                results.append(msg)

        if failed:
            raise ImportError(
                "One or more custom packages failed to load. "
                "Check task logs."
            )

    @task
    def check_base_providers():
        """
        Verify standard providers expected to be available in the base
        CWO image.

        The Cloudera provider is validated by importing the actual
        CdeRunJobOperator class rather than assuming the PyPI distribution
        name is also the Python import namespace.
        """

        base_modules = [
            (
                "Amazon Provider",
                "airflow.providers.amazon",
            ),
            (
                "Impala Provider",
                "airflow.providers.apache.impala",
            ),
            (
                "Kubernetes Provider",
                "airflow.providers.cncf.kubernetes",
            ),
        ]

        failed = False

        print("\n=== CHECKING BASE PROVIDERS ===")

        # ---------------------------------------------------------
        # Cloudera CDE Provider
        # ---------------------------------------------------------
        try:
            from cloudera.airflow.providers.operators.cde import (
                CdeRunJobOperator,
            )

            msg = (
                "✅ SUCCESS: Cloudera CDE Provider "
                "(cloudera.airflow.providers.operators.cde) "
                f"-> CdeRunJobOperator: {CdeRunJobOperator}"
            )

            print(msg)

        except ImportError as e:
            failed = True

            msg = (
                "❌ FAILED: Cloudera CDE Provider "
                "(cloudera.airflow.providers.operators.cde) "
                f"-> Error: {str(e)}"
            )

            print(msg)

        # ---------------------------------------------------------
        # Other base providers
        # ---------------------------------------------------------
        for name, module_name in base_modules:
            try:
                mod = importlib.import_module(module_name)

                version = getattr(mod, "__version__", "Present")

                msg = (
                    f"✅ SUCCESS: {name} ({module_name}) "
                    f"-> Version: {version}"
                )

                print(msg)

            except ImportError as e:
                failed = True

                msg = (
                    f"❌ FAILED: {name} ({module_name}) "
                    f"-> Error: {str(e)}"
                )

                print(msg)

        if failed:
            raise ImportError(
                "One or more base image providers are missing. "
                "Check task logs."
            )

    # Execute validation tasks sequentially.
    check_custom_packages() >> check_base_providers()


verify_dag = custom_image_verification()
