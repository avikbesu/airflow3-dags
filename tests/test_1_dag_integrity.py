"""
Layer 1 — Parse / import tests.

Verifies every DAG file loads without syntax errors, missing imports,
or bad DAG configuration.  Runs in milliseconds; no scheduler needed.
"""
import pytest
from airflow.models import DagBag

EXPECTED_DAG_IDS = {
    "1_k8s_job_xcom_bash_demo",
    "2_monitor_dag",
    "3_xcom_multi_operator_demo",
    "4_print_all_variables",
    "5_print_a_variable",
}


@pytest.fixture(scope="module")
def dagbag(dag_folder):
    return DagBag(dag_folder=dag_folder, include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, (
        f"DAG import errors:\n"
        + "\n".join(f"  {f}: {e}" for f, e in dagbag.import_errors.items())
    )


def test_expected_dags_present(dagbag):
    missing = EXPECTED_DAG_IDS - set(dagbag.dag_ids)
    assert not missing, f"Missing DAGs: {missing}"


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_dag_has_tags(dagbag, dag_id):
    dag = dagbag.dags.get(dag_id)
    assert dag is not None
    assert dag.tags, f"{dag_id} has no tags"


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_dag_catchup_disabled(dagbag, dag_id):
    dag = dagbag.dags.get(dag_id)
    assert dag is not None
    assert dag.catchup is False, f"{dag_id} has catchup=True"


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_dag_has_tasks(dagbag, dag_id):
    dag = dagbag.dags.get(dag_id)
    assert dag is not None
    assert len(dag.tasks) > 0, f"{dag_id} has no tasks"
