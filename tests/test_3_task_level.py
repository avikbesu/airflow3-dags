"""
Layer 3 — Task-level CLI tests  (`airflow tasks test`).

Runs each task in-process via subprocess.  No scheduler, no XCom
persistence — output is verified through stdout.  Skipped automatically
when the `airflow` CLI is unavailable (CI without Airflow installed).
"""
import subprocess
import sys
from datetime import date

import pytest

EXECUTION_DATE = "2024-01-01"

pytestmark = pytest.mark.skipif(
    subprocess.run(
        [sys.executable, "-m", "airflow", "version"],
        capture_output=True,
    ).returncode != 0,
    reason="airflow CLI not available",
)


def _run_task(dag_id: str, task_id: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            sys.executable, "-m", "airflow",
            "tasks", "test",
            dag_id, task_id, EXECUTION_DATE,
        ],
        capture_output=True,
        text=True,
    )


# ── DAG 3 ─────────────────────────────────────────────────────────────────────

def test_dag3_task1_bash_runs():
    r = _run_task("3_xcom_multi_operator_demo", "task_1")
    assert r.returncode == 0, r.stderr


def test_dag3_task3_python_runs():
    r = _run_task("3_xcom_multi_operator_demo", "task_3")
    assert r.returncode == 0, r.stderr


# ── DAG 5 ─────────────────────────────────────────────────────────────────────

def test_dag5_print_variables_runs():
    r = _run_task("5_print_a_variable", "print_variables")
    assert r.returncode == 0, r.stderr


# ── DAG 4 (needs live API — skip if no server) ────────────────────────────────

@pytest.mark.skip(reason="requires a running Airflow webserver at localhost:8080")
def test_dag4_fetch_variables_runs():
    r = _run_task("4_print_all_variables", "fetch_variables")
    assert r.returncode == 0, r.stderr
