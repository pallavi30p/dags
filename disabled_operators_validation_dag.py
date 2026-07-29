"""
DAG: disabled_operators_validation

Purpose:
    Validate that ``KubernetesPatchJobOperator`` is correctly disabled
    via the CWO ``disabledOperators`` setting.

Configuration Under Test:

    disabledOperators=KubernetesPatchJobOperator

Expected Result:

    This DAG should fail to parse (or appear in Import Errors) whenever
    ``KubernetesPatchJobOperator`` is present in the disabled operators
    configuration.

    If the DAG imports successfully, then the disabled-operator
    enforcement is not working as expected.

Notes:

    This DAG is intended only for validation of the disabled operators
    feature and should never be scheduled in production.
"""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.job import (
    KubernetesPatchJobOperator,
)


with DAG(
    dag_id="disabled_operators_validation",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["validation", "disabled-operators"],
    doc_md="""
# Disabled Operators Validation

## Objective

Validate that `KubernetesPatchJobOperator` cannot be used when it is
listed in `disabledOperators`.

## Expected Outcome

This DAG should **fail during DAG parsing** if operator disabling is
functioning correctly.

If this DAG imports successfully, the validation has failed.
""",
) as dag:

    validate_patch_job = KubernetesPatchJobOperator(
        task_id="validate_patch_job",
        name="validation-job",
        namespace="default",
        body={"metadata": {"labels": {"cwo-qe": "disabled-op-check"}}},
    )
