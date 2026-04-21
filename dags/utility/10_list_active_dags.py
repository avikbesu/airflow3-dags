"""
DAG: 10_list_active_dags

Lists every active (unpaused) DAG and, for each one, prints its most recent
DAG run:
  * run_id
  * state
  * conf (run configuration)
  * duration (seconds, end_date - start_date)

Uses the Airflow 3 REST API v2 — ORM access is blocked inside tasks.

Auth: Airflow 3.x uses JWT Bearer tokens. Credentials are resolved from an
Airflow Connection (`airflow_api`) or env vars
AIRFLOW_API_BASE_URL / AIRFLOW_API_USER / AIRFLOW_API_PASSWORD.
A JWT is obtained via POST /auth/token before any API call is made.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task

from utility.airflow_api_client import get_session


def _duration_seconds(start: str | None, end: str | None) -> float | None:
    if not start or not end:
        return None
    try:
        s = datetime.fromisoformat(start.replace("Z", "+00:00"))
        e = datetime.fromisoformat(end.replace("Z", "+00:00"))
    except ValueError:
        return None
    return (e - s).total_seconds()


@dag(
    dag_id="10_list_active_dags",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=[
        "type=utility",
        "exec=compose", "exec=kube",
        "subtype=dag-status",
        "intent=utility",
    ],
    doc_md=__doc__,
)
def list_active_dags():

    @task(task_id="fetch_active_dags")
    def fetch_active_dags() -> list:
        """GET /api/v2/dags?paused=false — paginated."""
        base_url, session = get_session()
        out: list = []
        limit, offset = 100, 0
        while True:
            r = session.get(
                f"{base_url}/api/v2/dags",
                params={"limit": limit, "offset": offset, "paused": "false"},
                timeout=30,
            )
            r.raise_for_status()
            payload = r.json()
            batch = payload.get("dags", [])
            out.extend(batch)
            total = payload.get("total_entries", len(out))
            offset += len(batch)
            if offset >= total or not batch:
                break
        return out

    @task(task_id="fetch_latest_runs")
    def fetch_latest_runs(dags: list) -> list:
        """
        For each DAG, fetch its latest run via
        GET /api/v2/dags/{dag_id}/dagRuns?order_by=-start_date&limit=1
        """
        base_url, session = get_session()
        rows: list = []

        for d in dags:
            dag_id = d.get("dag_id")
            r = session.get(
                f"{base_url}/api/v2/dags/{dag_id}/dagRuns",
                params={"limit": 1, "order_by": "-start_date"},
                timeout=30,
            )
            if r.status_code != 200:
                print(f"[WARN] {dag_id}: could not fetch runs ({r.status_code})")
                rows.append({
                    "dag_id": dag_id,
                    "run_id": None,
                    "state": None,
                    "conf": None,
                    "duration_seconds": None,
                })
                continue

            runs = r.json().get("dag_runs", [])
            if not runs:
                rows.append({
                    "dag_id": dag_id,
                    "run_id": None,
                    "state": "no-runs",
                    "conf": None,
                    "duration_seconds": None,
                })
                continue

            run = runs[0]
            rows.append({
                "dag_id": dag_id,
                "run_id": run.get("dag_run_id") or run.get("run_id"),
                "state": run.get("state"),
                "conf": run.get("conf"),
                "duration_seconds": _duration_seconds(
                    run.get("start_date"), run.get("end_date"),
                ),
                "start_date": run.get("start_date"),
                "end_date": run.get("end_date"),
            })
        return rows

    @task(task_id="print_table")
    def print_table(rows: list) -> list:
        if not rows:
            print("No active DAGs found.")
            return rows

        dag_col = max(len(r["dag_id"]) for r in rows)
        run_col = max((len(r["run_id"] or "-") for r in rows), default=10)
        state_col = max((len(r["state"] or "-") for r in rows), default=8)

        header = (
            f"  {'DAG_ID':<{dag_col}}  {'RUN_ID':<{run_col}}  "
            f"{'STATE':<{state_col}}  {'DURATION(s)':>12}  CONF"
        )
        width = len(header) + 20
        print("=" * width)
        print(f"  ACTIVE DAGS  (total={len(rows)})")
        print("=" * width)
        print(header)
        print("-" * width)

        for r in sorted(rows, key=lambda x: x["dag_id"]):
            dur = r["duration_seconds"]
            dur_s = f"{dur:>12.2f}" if isinstance(dur, (int, float)) else f"{'-':>12}"
            print(
                f"  {r['dag_id']:<{dag_col}}  "
                f"{(r['run_id'] or '-'):<{run_col}}  "
                f"{(r['state'] or '-'):<{state_col}}  "
                f"{dur_s}  {r['conf']}"
            )
        print("=" * width)
        return rows

    dags = fetch_active_dags()
    runs = fetch_latest_runs(dags)
    print_table(runs)


list_active_dags()
