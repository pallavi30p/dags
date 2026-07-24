"""
DAG: disabled_operators_validation

Purpose:
    Validate that operators configured in the CWO
    `disabledOperators` setting are correctly disabled.

Configuration Under Test:

    disabledOperators=
        KubernetesStartKueueJobOperator,
        BranchPythonVirtualenvOperator,
        _PythonVirtualenvDecoratedOperator,
        _BranchPythonVirtualenvDecoratedOperator

Expected Result:

    This DAG should fail to parse (or appear in Import Errors)
    whenever the above operators are present in the disabled
    operators configuration.

    If the DAG imports successfully, then the disabled operator
    enforcement is not working as expected.

Notes:

    This DAG is intended only for validation of the disabled
    operators feature and should never be scheduled in production.
"""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.providers.standard.operators.python import BranchPythonVirtualenvOperator
from airflow.providers.cncf.kubernetes.operators.kueue import (
    KubernetesStartKueueJobOperator,
)


def choose_branch():
    """Dummy branch callable."""
    return "done"


with DAG(
    dag_id="disabled_operators_validation",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["validation", "disabled-operators"],
    doc_md="""
# Disabled Operators Validation

## Objective

Validate that operators configured in
`disabledOperators` cannot be used.

## Expected Outcome

This DAG should **fail during DAG parsing** if operator
disabling is functioning correctly.

If this DAG imports successfully, the validation has failed.
""",
) as dag:

    validate_branch_virtualenv = BranchPythonVirtualenvOperator(
        task_id="validate_branch_virtualenv",
        python_callable=choose_branch,
        requirements=[],
    )

    validate_kueue = KubernetesStartKueueJobOperator(
        task_id="validate_kueue",
        queue_name="test-queue",
        namespace="default",
        image="busybox:1.36",
        cmds=["/bin/sh", "-c"],
        arguments=["echo validation"],
    )
