"""
DAG: 19_connection_health_checker

Pings every Airflow Connection and reports which are healthy, broken, or
untestable.  Catches stale / rotated credentials before they cause task
failures at runtime.

Health-check strategy per conn_type
-------------------------------------
Airflow's hook layer exposes a test_connection() → (bool, str) method on most
built-in providers.  This DAG delegates to that method and falls back to a
lightweight type-specific probe when test_connection() is unavailable:

  postgres / mysql / mssql / sqlite  → SELECT 1
  http / https                        → HEAD request to the configured host
  ssh                                 → TCP connect to host:port
  *                                   → attempt hook.get_conn()

Connections whose provider package is not installed are marked "skip (import
error)" rather than "failed", so missing extras don't pollute the report.

Auth setup (pick one):
  Option A — Airflow Connection (recommended):
    conn_id  : airflow_api
    conn_type: HTTP
    host     : http://localhost:8080
    login    : <airflow admin user>
    password  : <airflow admin password>

  Option B — Environment variables:
    AIRFLOW_API_BASE_URL = http://localhost:8080
    AIRFLOW_API_USER     = admin
    AIRFLOW_API_PASSWORD = admin

Run config
----------
    {
      "exclude_conn_ids": ["airflow_db", "airflow_api"],
      "timeout_seconds": 10,
      "fail_on_broken": false
    }

Set fail_on_broken=true to mark the DAG run as FAILED when any connection
check fails (useful for alerting via on_failure_callback).
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, get_current_context, task

from utility.airflow_api_client import get_session

_DEFAULT_TIMEOUT = 10


def _probe_db(conn, timeout: int) -> tuple[bool, str]:
    """Generic SELECT 1 probe for relational DB connections."""
    try:
        hook = conn.get_hook()
        db_conn = hook.get_conn()
        cursor = db_conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        db_conn.close()
        return True, "SELECT 1 succeeded"
    except Exception as exc:
        return False, str(exc)


def _probe_http(conn, timeout: int) -> tuple[bool, str]:
    """HEAD request to the connection's host endpoint."""
    import requests

    schema = conn.schema or "http"
    host = conn.host or "localhost"
    port = f":{conn.port}" if conn.port else ""
    url = f"{schema}://{host}{port}/"
    try:
        resp = requests.head(url, timeout=timeout, allow_redirects=True)
        return True, f"HTTP {resp.status_code} from {url}"
    except Exception as exc:
        return False, str(exc)


def _probe_tcp(conn, timeout: int) -> tuple[bool, str]:
    """TCP connect probe — used for SSH and other socket-based connections."""
    import socket

    host = conn.host or "localhost"
    port = conn.port or 22
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            pass
        return True, f"TCP connect to {host}:{port} succeeded"
    except Exception as exc:
        return False, str(exc)


def _probe_generic(conn, timeout: int) -> tuple[bool, str]:
    """Try hook.get_conn() as a last resort."""
    try:
        hook = conn.get_hook()
        hook.get_conn()
        return True, f"{type(hook).__name__}.get_conn() succeeded"
    except Exception as exc:
        return False, str(exc)


def _check_one(conn_id: str, conn_type: str, timeout: int) -> dict:
    """Return a health-check result dict for a single connection."""
    from airflow.hooks.base import BaseHook

    result: dict = {
        "conn_id": conn_id,
        "conn_type": conn_type,
        "status": "unknown",
        "message": "",
    }

    try:
        conn = BaseHook.get_connection(conn_id)
    except Exception as exc:
        result["status"] = "error"
        result["message"] = f"Could not retrieve connection: {exc}"
        return result

    # Prefer the built-in test_connection() when the hook implements it.
    try:
        hook = conn.get_hook()
        if hasattr(hook, "test_connection"):
            ok, msg = hook.test_connection()
            result["status"] = "healthy" if ok else "failed"
            result["message"] = msg
            return result
    except ImportError as exc:
        result["status"] = "skip"
        result["message"] = f"Provider not installed: {exc}"
        return result
    except Exception:
        pass  # fall through to type-specific probes

    # Type-specific fallback probes.
    ct = (conn_type or "").lower()
    try:
        if ct in {"postgres", "postgresql", "mysql", "mssql", "sqlite", "redshift"}:
            ok, msg = _probe_db(conn, timeout)
        elif ct in {"http", "https"}:
            ok, msg = _probe_http(conn, timeout)
        elif ct in {"ssh"}:
            ok, msg = _probe_tcp(conn, timeout)
        else:
            ok, msg = _probe_generic(conn, timeout)
        result["status"] = "healthy" if ok else "failed"
        result["message"] = msg
    except ImportError as exc:
        result["status"] = "skip"
        result["message"] = f"Provider not installed: {exc}"
    except Exception as exc:
        result["status"] = "error"
        result["message"] = str(exc)

    return result


@dag(
    dag_id="19_connection_health_checker",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=[
        "type=utility",
        "exec=compose",
        "subtype=maintenance",
        "intent=operational",
    ],
    doc_md=__doc__,
    params={
        "exclude_conn_ids": [],
        "timeout_seconds": _DEFAULT_TIMEOUT,
        "fail_on_broken": False,
    },
)
def connection_health_checker():

    @task(task_id="resolve_config")
    def resolve_config() -> dict:
        ctx = get_current_context()
        params = ctx.get("params") or {}
        conf = (ctx["dag_run"].conf or {}) if ctx.get("dag_run") else {}
        merged = {**params, **conf}
        timeout = int(merged.get("timeout_seconds", _DEFAULT_TIMEOUT))
        if timeout < 1:
            raise ValueError(f"timeout_seconds must be >= 1, got {timeout}")
        return {
            "exclude_conn_ids": list(merged.get("exclude_conn_ids") or []),
            "timeout_seconds": timeout,
            "fail_on_broken": bool(merged.get("fail_on_broken", False)),
        }

    @task(task_id="list_connections")
    def list_connections(cfg: dict) -> list[dict]:
        """Fetch all connection IDs and types from the Airflow REST API."""
        base_url, session = get_session()
        excluded = set(cfg["exclude_conn_ids"])
        connections: list[dict] = []
        limit, offset = 100, 0

        while True:
            resp = session.get(
                f"{base_url}/api/v2/connections",
                params={"limit": limit, "offset": offset},
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()
            batch = payload.get("connections", [])
            for c in batch:
                if c["connection_id"] not in excluded:
                    connections.append(
                        {
                            "conn_id": c["connection_id"],
                            "conn_type": c.get("conn_type", ""),
                        }
                    )
            total = payload.get("total_entries", 0)
            offset += len(batch)
            if offset >= total or not batch:
                break

        print(
            f"[INFO] {len(connections)} connection(s) to check "
            f"({len(excluded)} excluded by config)."
        )
        return connections

    @task(task_id="health_check_connections")
    def health_check_connections(cfg: dict, connections: list[dict]) -> list[dict]:
        """Run a health check on each connection and collect results."""
        results: list[dict] = []
        timeout = cfg["timeout_seconds"]
        icons = {"healthy": "[OK]", "failed": "[FAIL]", "skip": "[SKIP]", "error": "[ERR]"}

        for c in connections:
            result = _check_one(c["conn_id"], c["conn_type"], timeout)
            icon = icons.get(result["status"], "[?]")
            print(
                f"{icon} {result['conn_id']:<35} type={result['conn_type']:<18} "
                f"{result['message'][:70]}"
            )
            results.append(result)

        return results

    @task(task_id="print_report")
    def print_report(cfg: dict, results: list[dict]) -> None:
        from collections import Counter

        counts = Counter(r["status"] for r in results)
        width = 95

        print("=" * width)
        print(f"  {'CONNECTION HEALTH CHECKER REPORT':^{width - 4}}")
        print("=" * width)
        print(
            f"  healthy={counts.get('healthy', 0)}  "
            f"failed={counts.get('failed', 0)}  "
            f"error={counts.get('error', 0)}  "
            f"skip={counts.get('skip', 0)}  "
            f"total={len(results)}"
        )
        print(f"  timeout_seconds={cfg['timeout_seconds']}  "
              f"fail_on_broken={cfg['fail_on_broken']}")

        sections = [
            ("FAILED / BROKEN", "failed"),
            ("ERROR (unexpected)", "error"),
            ("HEALTHY", "healthy"),
            ("SKIPPED (provider not installed)", "skip"),
        ]
        for label, key in sections:
            subset = [r for r in results if r["status"] == key]
            if not subset:
                continue
            print(f"\n  [{label}]  ({len(subset)} connection(s))")
            print(f"  {'Connection ID':<35} {'Type':<22} Message")
            print(f"  {'-'*35} {'-'*22} {'-'*32}")
            for r in subset:
                print(
                    f"  {r['conn_id']:<35} {r['conn_type']:<22} "
                    f"{r['message'][:60]}"
                )

        print("=" * width)

        if cfg["fail_on_broken"]:
            broken = [r for r in results if r["status"] in {"failed", "error"}]
            if broken:
                ids = ", ".join(r["conn_id"] for r in broken)
                raise RuntimeError(
                    f"{len(broken)} connection(s) failed health check: {ids}"
                )

    cfg = resolve_config()
    connections = list_connections(cfg)
    results = health_check_connections(cfg, connections)
    print_report(cfg, results)


connection_health_checker()
