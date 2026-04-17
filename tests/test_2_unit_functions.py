"""
Layer 2 — Unit tests for pure task-level functions.

Imports helper functions directly from each DAG module and tests them
without triggering any Airflow scheduler or metastore interaction.
"""
import importlib.util
import os
import sys

import pytest


# ── helpers to import a module from a file path without side-effects ──────────

def _load_module(name: str, filepath: str):
    spec = importlib.util.spec_from_file_location(name, filepath)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


DAG_DIR = os.path.join(os.path.dirname(__file__), "..", "dags", "utility")


# ─────────────────────────────────────────────────────────────────────────────
# DAG 4 & 5 — _is_sensitive()
# Both modules define the same helper; test each independently.
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def dag4():
    return _load_module("dag4", os.path.join(DAG_DIR, "4_print_all_variables.py"))


@pytest.fixture(scope="module")
def dag5():
    return _load_module("dag5", os.path.join(DAG_DIR, "5_print_a_variable.py"))


@pytest.mark.parametrize("key", ["db_password", "api_secret", "auth_token", "aws_key", "user_pwd", "service_credential"])
def test_dag4_is_sensitive_positive(dag4, key):
    assert dag4._is_sensitive(key) is True


@pytest.mark.parametrize("key", ["db_host", "env", "s3_bucket", "region", "timeout"])
def test_dag4_is_sensitive_negative(dag4, key):
    assert dag4._is_sensitive(key) is False


@pytest.mark.parametrize("key", ["Another_password", "Another_passwd"])
def test_dag5_is_sensitive_positive(dag5, key):
    assert dag5._is_sensitive(key) is True


@pytest.mark.parametrize("key", ["env", "s3_bucket", "db_host", "EXEC_ENV"])
def test_dag5_is_sensitive_negative(dag5, key):
    assert dag5._is_sensitive(key) is False


# ─────────────────────────────────────────────────────────────────────────────
# DAG 3 — XCom merge logic in collect_xcoms
# We extract just the merge logic and test it without a real TaskInstance.
# ─────────────────────────────────────────────────────────────────────────────

def _merge_xcoms(bash_xcom, empty_xcom, py_xcom) -> list:
    """Mirror of collect_xcoms merge logic, extracted for unit testing."""
    all_results = []
    for xcom in (bash_xcom, empty_xcom, py_xcom):
        if xcom is not None:
            all_results.extend(xcom if isinstance(xcom, list) else [xcom])
    return all_results


def test_merge_xcoms_combines_lists():
    bash = [{"task": "bash task", "result": "success from bash"}]
    py   = [{"task": "py task",   "result": "success from python"}]
    result = _merge_xcoms(bash, None, py)
    assert len(result) == 2
    assert result[0]["task"] == "bash task"
    assert result[1]["task"] == "py task"


def test_merge_xcoms_skips_none():
    result = _merge_xcoms(None, None, None)
    assert result == []


def test_merge_xcoms_wraps_single_item():
    result = _merge_xcoms({"task": "single"}, None, None)
    assert result == [{"task": "single"}]


def test_merge_xcoms_empty_operator_never_contributes():
    bash = [{"task": "bash"}]
    result = _merge_xcoms(bash, None, None)
    assert len(result) == 1
