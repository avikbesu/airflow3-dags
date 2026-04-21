"""
Layer 1 — Parse / import tests.

Verifies every DAG file loads without syntax errors, missing imports,
or bad DAG configuration.  Runs in milliseconds; no scheduler needed.
"""
import os

import pytest
from airflow.models import DagBag

# Build DagBag at collection time so parametrize can use the discovered IDs.
# conftest sets AIRFLOW__CORE__DAGS_FOLDER, but that env var is for the CLI;
# here we resolve the path the same way conftest does.
_DAGS_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dags"))
_DAGBAG = DagBag(dag_folder=_DAGS_FOLDER, include_examples=False)
_ALL_DAG_IDS = sorted(_DAGBAG.dag_ids)


@pytest.fixture(scope="module")
def dagbag():
    return _DAGBAG


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, (
        f"DAG import errors:\n"
        + "\n".join(f"  {f}: {e}" for f, e in dagbag.import_errors.items())
    )


def test_at_least_one_dag_loaded(dagbag):
    assert len(dagbag.dag_ids) > 0, "DagBag loaded no DAGs — check dags folder path"


@pytest.mark.parametrize("dag_id", _ALL_DAG_IDS)
def test_dag_has_tags(dagbag, dag_id):
    dag = dagbag.dags.get(dag_id)
    assert dag is not None
    assert dag.tags, f"{dag_id} has no tags"


@pytest.mark.parametrize("dag_id", _ALL_DAG_IDS)
def test_dag_catchup_disabled(dagbag, dag_id):
    dag = dagbag.dags.get(dag_id)
    assert dag is not None
    assert dag.catchup is False, f"{dag_id} has catchup=True"


@pytest.mark.parametrize("dag_id", _ALL_DAG_IDS)
def test_dag_has_tasks(dagbag, dag_id):
    dag = dagbag.dags.get(dag_id)
    assert dag is not None
    assert len(dag.tasks) > 0, f"{dag_id} has no tasks"
