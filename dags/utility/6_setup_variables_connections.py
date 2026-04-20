"""
DAG: 6_setup_variables_connections

Idempotently seeds Airflow Variables and Connections from a YAML input.

Behaviour:
  * For every variable in the YAML, check if it already exists.
    If missing -> create via REST API v2.  If present -> skip.
  * For every connection, same check-then-create logic.
  * Never overwrites existing values (idempotent / safe to re-run).

YAML input sources (first match wins):
  1. DAG run conf:   { "yaml_path": "/path/to/file.yaml" }
                     or { "yaml_inline": "<yaml string>" }
  2. Airflow Variable:  `setup_config_yaml_path`
  3. Default path:      $AIRFLOW_HOME/config/setup_config.yaml

Expected YAML shape:

    variables:
      - key: env
        value: prod
        description: Target environment
      - key: s3_bucket
        value: my-bucket
    connections:
      - conn_id: my_postgres
        conn_type: postgres
        host: db.internal
        login: airflow
        password: airflow
        schema: airflow
        port: 5432
        extra: '{"sslmode": "require"}'

Auth: same pattern as DAG 4 (Airflow Connection `airflow_api` or env vars
AIRFLOW_API_BASE_URL / AIRFLOW_API_USER / AIRFLOW_API_PASSWORD).
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.sdk import dag, get_current_context, task

_CONN_ID = "airflow_api"
_DEFAULT_API_URL = "http://localhost:8080"
_DEFAULT_YAML_VAR = "setup_config_yaml_path"


def _resolve_auth():
    from requests.auth import HTTPBasicAuth
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
    dag_id="6_setup_variables_connections",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=[
        "type=utility",
        "exec=compose", "exec=kube",
        "subtype=bootstrap",
        "intent=utility",
    ],
    doc_md=__doc__,
    params={
        "yaml_path": "",
        "yaml_inline": "",
    },
)
def setup_variables_connections():

    @task(task_id="load_yaml")
    def load_yaml() -> dict:
        """Reads YAML from run conf, Variable, or default path."""
        import yaml
        from airflow.sdk import Variable

        ctx = get_current_context()
        conf = (ctx.get("params") or {}) | (ctx.get("dag_run").conf or {} if ctx.get("dag_run") else {})

        inline = conf.get("yaml_inline")
        if inline:
            print("[INFO] Loading YAML from run conf (inline).")
            return yaml.safe_load(inline) or {}

        path = conf.get("yaml_path") or Variable.get(_DEFAULT_YAML_VAR, default="")
        if not path:
            path = os.path.join(
                os.environ.get("AIRFLOW_HOME", "/opt/airflow"),
                "config", "setup_config.yaml",
            )

        print(f"[INFO] Loading YAML from path: {path}")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Setup YAML not found: {path}")

        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        return data

    @task(task_id="seed_variables")
    def seed_variables(config: dict) -> dict:
        """For each variable in YAML: skip if exists, else POST to REST API."""
        import requests

        base_url, auth = _resolve_auth()
        created, skipped, failed = [], [], []

        for entry in config.get("variables") or []:
            key = entry.get("key")
            if not key:
                continue

            resp = requests.get(
                f"{base_url}/api/v2/variables/{key}", auth=auth, timeout=30,
            )
            if resp.status_code == 200:
                print(f"[SKIP] variable already exists: {key}")
                skipped.append(key)
                continue

            payload = {
                "key": key,
                "value": str(entry.get("value", "")),
                "description": entry.get("description", ""),
            }
            create = requests.post(
                f"{base_url}/api/v2/variables", auth=auth, json=payload, timeout=30,
            )
            if create.status_code in (200, 201):
                print(f"[CREATE] variable: {key}")
                created.append(key)
            else:
                print(f"[FAIL] variable {key}: {create.status_code} {create.text}")
                failed.append(key)

        return {"created": created, "skipped": skipped, "failed": failed}

    @task(task_id="seed_connections")
    def seed_connections(config: dict) -> dict:
        """For each connection in YAML: skip if exists, else POST to REST API."""
        import requests

        base_url, auth = _resolve_auth()
        created, skipped, failed = [], [], []

        for entry in config.get("connections") or []:
            conn_id = entry.get("conn_id")
            if not conn_id:
                continue

            resp = requests.get(
                f"{base_url}/api/v2/connections/{conn_id}", auth=auth, timeout=30,
            )
            if resp.status_code == 200:
                print(f"[SKIP] connection already exists: {conn_id}")
                skipped.append(conn_id)
                continue

            payload = {
                "connection_id": conn_id,
                "conn_type": entry.get("conn_type"),
                "host": entry.get("host"),
                "login": entry.get("login"),
                "password": entry.get("password"),
                "schema": entry.get("schema"),
                "port": entry.get("port"),
                "extra": entry.get("extra"),
                "description": entry.get("description"),
            }
            payload = {k: v for k, v in payload.items() if v is not None}

            create = requests.post(
                f"{base_url}/api/v2/connections", auth=auth, json=payload, timeout=30,
            )
            if create.status_code in (200, 201):
                print(f"[CREATE] connection: {conn_id}")
                created.append(conn_id)
            else:
                print(f"[FAIL] connection {conn_id}: {create.status_code} {create.text}")
                failed.append(conn_id)

        return {"created": created, "skipped": skipped, "failed": failed}

    @task(task_id="summarise")
    def summarise(vars_result: dict, conn_result: dict) -> dict:
        print("=" * 60)
        print(" SETUP SUMMARY")
        print("=" * 60)
        print(f"  variables   created={len(vars_result['created'])}  "
              f"skipped={len(vars_result['skipped'])}  failed={len(vars_result['failed'])}")
        print(f"  connections created={len(conn_result['created'])}  "
              f"skipped={len(conn_result['skipped'])}  failed={len(conn_result['failed'])}")
        if vars_result["failed"] or conn_result["failed"]:
            raise RuntimeError(
                f"Setup had failures — variables={vars_result['failed']} "
                f"connections={conn_result['failed']}"
            )
        return {"variables": vars_result, "connections": conn_result}

    cfg = load_yaml()
    v = seed_variables(cfg)
    c = seed_connections(cfg)
    summarise(v, c)


setup_variables_connections()
