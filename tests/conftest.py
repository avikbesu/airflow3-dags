"""
Shared fixtures for all test layers.

Sets AIRFLOW_HOME to a temp dir and initialises a fresh SQLite metastore
so every pytest run is hermetic and requires no running Airflow instance.
"""
import os
import subprocess
import sys
import tempfile

import pytest

# Absolute path to the repo's dags/ directory
_DAGS_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dags"))


# ── point Airflow at a throw-away home before any airflow import ──────────────
@pytest.fixture(scope="session", autouse=True)
def airflow_home(tmp_path_factory):
    home = tmp_path_factory.mktemp("airflow_home")
    os.environ["AIRFLOW_HOME"] = str(home)
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{home}/airflow.db"
    # Airflow 3 CLI resolves DAGs via the 'dags-folder' bundle whose path
    # defaults to $AIRFLOW_HOME/dags.  Override it so subprocess CLI calls
    # find the repo's actual dags/ directory.
    os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = _DAGS_FOLDER

    # initialise DB (needed for DagBag and integration tests)
    subprocess.run(
        [sys.executable, "-m", "airflow", "db", "migrate"],
        check=True,
        capture_output=True,
    )
    return home


# ── DAG folder path usable in all tests ──────────────────────────────────────
@pytest.fixture(scope="session")
def dag_folder():
    return _DAGS_FOLDER
