"""
DAG: print_all_variables
Purpose: Fetches and prints all Airflow Variables via the REST API v2.

Why REST API?
  Airflow 3.0 blocks direct ORM / create_session() access inside tasks.
  All metastore access must go through the Task SDK communication layer.
  Variable.get(key) works for known keys, but listing all variables
  requires the public REST API: GET /api/v2/variables

Auth setup (pick one):
  Option A — Airflow Connection (recommended):
    Create a connection in the UI / CLI:
      conn_id : airflow_api
      conn_type: HTTP
      host     : http://localhost:8080   (your webserver URL)
      login    : <airflow admin user>
      password : <airflow admin password>

  Option B — Environment variables (quick local dev):
    AIRFLOW_API_BASE_URL  = http://localhost:8080
    AIRFLOW_API_USER      = admin
    AIRFLOW_API_PASSWORD  = admin
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.sdk import dag, task

_SENSITIVE_PATTERNS = ("password", "secret", "token", "key", "api", "pwd", "credential")
_DEFAULT_API_URL = "http://localhost:8080"
_CONN_ID = "airflow_api"  # Change if your connection has a different id


def _is_sensitive(var_key: str) -> bool:
    return any(p in var_key.lower() for p in _SENSITIVE_PATTERNS)


@dag(
    dag_id="4_print_all_variables",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=[
        "type=demo",
        "exec=kube", "exec=compose",
        "subtype=variables",
        "intent=utility"
    ],
    doc_md="""
    ## Print All Airflow Variables

    Lists every Variable currently stored in the Airflow metastore and prints
    each key/value pair to the task log using the REST API v2.

    **Sensitive values** (keys containing: `password`, `secret`, `token`, `key`, `api`)
    are automatically masked.

    **Auth setup** — create an Airflow Connection:
    ```
    conn_id  : airflow_api
    conn_type: HTTP
    host     : http://localhost:8080
    login    : <admin user>
    password : <admin password>
    ```

    Or set env vars: `AIRFLOW_API_BASE_URL`, `AIRFLOW_API_USER`, `AIRFLOW_API_PASSWORD`.

    Trigger manually:
    ```bash
    airflow dags trigger print_all_variables
    ```
    """,
)
def print_all_variables():

    @task(task_id="fetch_variables")
    def fetch_variables() -> dict[str, str]:
        """
        Calls GET /api/v2/variables (paginated) and returns
        {key: value_or_masked} for all variables.

        Airflow 3 REST API paginates at 100 items by default.
        We loop until all pages are exhausted.
        """
        import requests
        from requests.auth import HTTPBasicAuth

        # ── Resolve base URL ──────────────────────────────────────────────
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

        # ── Paginated fetch ───────────────────────────────────────────────
        all_vars: list[dict] = []
        limit = 100
        offset = 0

        while True:
            resp = requests.get(
                f"{base_url}/api/v2/variables",
                auth=auth,
                params={"limit": limit, "offset": offset},
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()

            batch = payload.get("variables", [])
            all_vars.extend(batch)

            total = payload.get("total_entries", len(all_vars))
            offset += len(batch)

            if offset >= total or not batch:
                break

        # ── Mask sensitive values ─────────────────────────────────────────
        result: dict[str, str] = {}
        for var in all_vars:
            k = var.get("key", "")
            v = "***MASKED***" if _is_sensitive(k) else str(var.get("value", ""))
            result[k] = v

        return result

    @task(task_id="print_variables")
    def print_variables(variables: dict[str, str]) -> None:
        """Pretty-prints all variables to the task log."""
        if not variables:
            print("=" * 60)
            print("  No Airflow Variables found.")
            print("=" * 60)
            return

        total = len(variables)
        col_width = max((len(k) for k in variables), default=20)
        col_width = max(col_width, 20)
        width = col_width + 48

        print("=" * width)
        print(f"  {'AIRFLOW VARIABLES':^{width - 4}}")
        print(f"  Total: {total} variable(s)")
        print("=" * width)
        print(f"  {'KEY':<{col_width}}  VALUE")
        print("-" * width)

        for key in sorted(variables):
            value = variables[key]
            display = value if len(value) <= 80 else value[:77] + "..."
            print(f"  {key:<{col_width}}  {display}")

        print("=" * width)
        print(f"  Done. {total} variable(s) printed.")
        print("=" * width)

    vars_dict = fetch_variables()
    print_variables(vars_dict)


print_all_variables()