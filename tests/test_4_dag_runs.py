"""
Layer 4 — Full DAG integration tests  (`airflow dags test`).

Runs every task in the DAG sequentially in-process.  Catches XCom wiring
errors and dependency-order bugs that task-level tests miss.

DAGs that require Kubernetes or a live webserver are marked skip.
"""
import subprocess
import sys

import pytest

EXECUTION_DATE = "2024-01-01"

pytestmark = pytest.mark.skipif(
    subprocess.run(
        [sys.executable, "-m", "airflow", "version"],
        capture_output=True,
    ).returncode != 0,
    reason="airflow CLI not available",
)


def _run_dag(dag_id: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            sys.executable, "-m", "airflow",
            "dags", "test",
            dag_id, EXECUTION_DATE,
        ],
        capture_output=True,
        text=True,
    )


def test_dag3_full_xcom_pipeline():
    """All 4 tasks run; task_4 receives merged XCom from task_1 and task_3."""
    r = _run_dag("3_xcom_multi_operator_demo")
    assert r.returncode == 0, r.stderr
    assert "Combined XCom results" in r.stdout or "Combined XCom results" in r.stderr


def test_dag5_full_variable_print():
    r = _run_dag("5_print_a_variable")
    assert r.returncode == 0, r.stderr


@pytest.mark.skip(reason="requires a running Kubernetes cluster")
def test_dag1_k8s_xcom():
    r = _run_dag("1_k8s_job_xcom_bash_demo")
    assert r.returncode == 0, r.stderr


@pytest.mark.skip(reason="requires a prior DAG run with a known run_id in the metastore")
def test_dag2_monitor_dag():
    r = _run_dag("2_monitor_dag")
    assert r.returncode == 0, r.stderr


@pytest.mark.skip(reason="requires a running Airflow webserver at localhost:8080")
def test_dag4_print_all_variables():
    r = _run_dag("4_print_all_variables")
    assert r.returncode == 0, r.stderr
