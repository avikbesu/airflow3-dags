"""
DAG: 11_xcom_api_sdk_demo

Demonstrates two ways to push / pull XComs in Airflow 3:

  * SDK path  — `ti.xcom_push(...)` and `ti.xcom_pull(...)` from inside a task.
  * REST API  — `GET / POST /api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/
                 {task_id}/xcomEntries/{key}` to an Airflow API server.

Task flow:
    sdk_push  ──►  sdk_pull
         │            │
         ▼            ▼
      api_pull      api_push  ──►  api_verify_pull

Run config (optional — overrides API host for this run):
    { "api_base_url": "http://webserver:8080" }

Auth: same pattern as DAG 4 (Airflow Connection `airflow_api` or env vars
AIRFLOW_API_BASE_URL / AIRFLOW_API_USER / AIRFLOW_API_PASSWORD).
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.sdk import dag, get_current_context, task

_CONN_ID = "airflow_api"
_DEFAULT_API_URL = "http://localhost:8080"


def _resolve_auth(override_url: str | None = None):
    from requests.auth import HTTPBasicAuth
    if override_url:
        base_url = override_url.rstrip("/")
        auth = HTTPBasicAuth(
            os.getenv("AIRFLOW_API_USER", "admin"),
            os.getenv("AIRFLOW_API_PASSWORD", "admin"),
        )
        return base_url, auth
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(_CONN_ID)
        base_url = (conn.host or _DEFAULT_API_URL).rstrip("/")
        auth = HTTPBasicAuth(conn.login or "", conn.password or "")
    except Exception:
        base_url = os.getenv("AIRFLOW_API_BASE_URL", _DEFAULT_API_URL).rstrip("/")
        auth = HTTPBasicAuth(
            os.getenv("AIRFLOW_API_USER", "admin"),
            os.getenv("AIRFLOW_API_PASSWORD", "admin"),
        )
    return base_url, auth


@dag(
    dag_id="11_xcom_api_sdk_demo",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=[
        "type=utility",
        "exec=compose", "exec=kube",
        "subtype=xcom",
        "intent=utility",
    ],
    doc_md=__doc__,
    params={"api_base_url": ""},
)
def xcom_api_sdk_demo():

    @task(task_id="sdk_push")
    def sdk_push(ti=None) -> dict:
        """Pushes both via return value (auto-XCom) and an explicit key."""
        payload = {"source": "sdk", "numbers": [1, 2, 3]}
        ti.xcom_push(key="extra_sdk_key", value={"note": "pushed via SDK"})
        print(f"[SDK push] return={payload}, extra_sdk_key=pushed")
        return payload

    @task(task_id="sdk_pull")
    def sdk_pull(ti=None) -> dict:
        """Pulls back what sdk_push wrote — pure Task SDK."""
        ret = ti.xcom_pull(task_ids="sdk_push")
        extra = ti.xcom_pull(task_ids="sdk_push", key="extra_sdk_key")
        print(f"[SDK pull] return_value={ret}")
        print(f"[SDK pull] extra_sdk_key={extra}")
        return {"return_value": ret, "extra_sdk_key": extra}

    @task(task_id="api_pull")
    def api_pull() -> dict:
        """Pulls sdk_push's XCom via the REST API v2."""
        import requests

        ctx = get_current_context()
        dag_run = ctx["dag_run"]
        run_id = dag_run.run_id
        dag_id = ctx["dag"].dag_id
        conf = dag_run.conf or {}

        base_url, auth = _resolve_auth(conf.get("api_base_url") or None)
        url = (
            f"{base_url}/api/v2/dags/{dag_id}/dagRuns/{run_id}"
            f"/taskInstances/sdk_push/xcomEntries/return_value"
        )
        r = requests.get(url, auth=auth, params={"deserialize": "true"}, timeout=30)
        r.raise_for_status()
        payload = r.json()
        print(f"[API pull] {url}\n  -> {payload}")
        return payload

    @task(task_id="api_push")
    def api_push() -> dict:
        """
        Pushes a new XCom entry via POST
        /api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries
        The task_id targets THIS task (api_push) so the entry is attached to
        this task instance.
        """
        import requests

        ctx = get_current_context()
        dag_run = ctx["dag_run"]
        run_id = dag_run.run_id
        dag_id = ctx["dag"].dag_id
        ti = ctx["ti"]
        conf = dag_run.conf or {}

        base_url, auth = _resolve_auth(conf.get("api_base_url") or None)
        url = (
            f"{base_url}/api/v2/dags/{dag_id}/dagRuns/{run_id}"
            f"/taskInstances/{ti.task_id}/xcomEntries"
        )
        body = {
            "key": "pushed_via_api",
            "value": {"source": "api", "when": datetime.utcnow().isoformat() + "Z"},
        }
        r = requests.post(url, auth=auth, json=body, timeout=30)
        if r.status_code not in (200, 201):
            print(f"[API push] FAIL {r.status_code} {r.text}")
            r.raise_for_status()
        print(f"[API push] {url}\n  -> {body}")
        return body

    @task(task_id="api_verify_pull")
    def api_verify_pull() -> dict:
        """Reads back the API-pushed key to prove the round-trip works."""
        import requests

        ctx = get_current_context()
        dag_run = ctx["dag_run"]
        run_id = dag_run.run_id
        dag_id = ctx["dag"].dag_id
        conf = dag_run.conf or {}

        base_url, auth = _resolve_auth(conf.get("api_base_url") or None)
        url = (
            f"{base_url}/api/v2/dags/{dag_id}/dagRuns/{run_id}"
            f"/taskInstances/api_push/xcomEntries/pushed_via_api"
        )
        r = requests.get(url, auth=auth, params={"deserialize": "true"}, timeout=30)
        r.raise_for_status()
        payload = r.json()
        print(f"[API verify] {url}\n  -> {payload}")
        return payload

    pushed = sdk_push()
    pulled_sdk = sdk_pull()
    pulled_api = api_pull()
    posted_api = api_push()
    verified = api_verify_pull()

    pushed >> [pulled_sdk, pulled_api]
    posted_api >> verified


xcom_api_sdk_demo()
