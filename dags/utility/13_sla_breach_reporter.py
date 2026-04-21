"""
DAG: sla_breach_reporter
Purpose: Query DAG run history via REST API, flag runs exceeding a configurable
SLA threshold, correlate with the slowest tasks per run, and export results
as JSON or CSV.

Tune the constants below to adjust lookback window, SLA, and output format.

Auth setup (pick one):
  Option A — Airflow Connection (recommended):
    conn_id  : airflow_api
    conn_type: HTTP
    host     : http://localhost:8080
    login    : <airflow admin user>
    password : <airflow admin password>

  Option B — Environment variables:
    AIRFLOW_API_BASE_URL = http://localhost:8080
    AIRFLOW_API_USER     = admin
    AIRFLOW_API_PASSWORD = admin
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

from utility.airflow_api_client import get_session

_LOOKBACK_DAYS = 7       # days of run history to scan
_SLA_MINUTES = 60        # SLA threshold — runs longer than this are flagged
_TOP_SLOW_TASKS = 5      # max slow tasks to attach per breaching run
_OUTPUT_FORMAT = "json"  # "json" or "csv"
_OUTPUT_DIR = "/tmp"     # directory for the exported report


def _duration_seconds(start: str | None, end: str | None) -> float | None:
    if not start or not end:
        return None
    try:
        s = datetime.fromisoformat(start.replace("Z", "+00:00"))
        e = datetime.fromisoformat(end.replace("Z", "+00:00"))
        return (e - s).total_seconds()
    except (ValueError, TypeError):
        return None


@dag(
    dag_id="13_sla_breach_reporter",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=[
        "type=utility",
        "exec=kube", "exec=compose",
        "subtype=monitoring",
        "intent=operational",
    ],
    doc_md="""
## SLA Breach Reporter

Queries DAG run history from the Airflow REST API, identifies runs that exceeded
a configurable SLA threshold, correlates them with the slowest tasks, and exports
a report as JSON or CSV.

**Tune the module-level constants** in the DAG file to adjust behaviour:

| Constant | Default | Description |
|----------|---------|-------------|
| `_LOOKBACK_DAYS` | 7 | Days of history to scan |
| `_SLA_MINUTES` | 60 | SLA threshold in minutes |
| `_OUTPUT_FORMAT` | json | Export format: `json` or `csv` |
| `_OUTPUT_DIR` | /tmp | Directory for the report file |

**Auth setup** — create an Airflow Connection:
```
conn_id  : airflow_api
conn_type: HTTP
host     : http://localhost:8080
login    : <admin user>
password : <admin password>
```

Trigger manually:
```bash
airflow dags trigger 13_sla_breach_reporter
```
""",
)
def sla_breach_reporter():

    @task(task_id="fetch_dag_runs")
    def fetch_dag_runs() -> list[dict]:
        """Fetch all completed DAG runs within _LOOKBACK_DAYS via paginated REST API."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        base_url, session = get_session()
        all_runs: list[dict] = []
        limit, offset = 100, 0

        while True:
            resp = session.get(
                f"{base_url}/api/v2/dags/~/dagRuns",
                params={
                    "limit": limit,
                    "offset": offset,
                    "state": "success,failed",
                    "updated_at_gte": cutoff,
                },
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()
            batch = payload.get("dag_runs", [])
            all_runs.extend(batch)
            total = payload.get("total_entries", len(all_runs))
            offset += len(batch)
            if offset >= total or not batch:
                break

        print(f"[INFO] Fetched {len(all_runs)} run(s) from the last {_LOOKBACK_DAYS} day(s).")
        return all_runs

    @task(task_id="identify_breaches")
    def identify_breaches(dag_runs: list[dict]) -> list[dict]:
        """Return runs whose wall-clock duration exceeded _SLA_MINUTES, sorted by overage."""
        sla_seconds = _SLA_MINUTES * 60
        breaches: list[dict] = []

        for run in dag_runs:
            duration = _duration_seconds(run.get("start_date"), run.get("end_date"))
            if duration is not None and duration > sla_seconds:
                breaches.append({
                    "dag_id": run.get("dag_id"),
                    "run_id": run.get("run_id"),
                    "state": run.get("state"),
                    "start_date": run.get("start_date"),
                    "end_date": run.get("end_date"),
                    "duration_minutes": round(duration / 60, 2),
                    "sla_exceeded_by_minutes": round((duration - sla_seconds) / 60, 2),
                })

        breaches.sort(key=lambda x: x["sla_exceeded_by_minutes"], reverse=True)
        print(f"[INFO] {len(breaches)} run(s) exceeded the {_SLA_MINUTES}-minute SLA.")
        return breaches

    @task(task_id="fetch_slow_tasks")
    def fetch_slow_tasks(breaches: list[dict]) -> list[dict]:
        """
        For each breaching run, attach the top _TOP_SLOW_TASKS slowest task instances
        fetched from GET /api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances.
        """
        if not breaches:
            print("[INFO] No breaches — skipping slow task fetch.")
            return []

        base_url, session = get_session()
        enriched: list[dict] = []

        for breach in breaches:
            dag_id = breach["dag_id"]
            run_id = breach["run_id"]
            try:
                resp = session.get(
                    f"{base_url}/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances",
                    timeout=30,
                )
                resp.raise_for_status()
                task_instances = resp.json().get("task_instances", [])

                task_durations = []
                for ti in task_instances:
                    d = _duration_seconds(ti.get("start_date"), ti.get("end_date"))
                    if d is not None:
                        task_durations.append({
                            "task_id": ti.get("task_id"),
                            "state": ti.get("state"),
                            "duration_seconds": round(d, 1),
                            "try_number": ti.get("try_number", 1),
                        })

                task_durations.sort(key=lambda x: x["duration_seconds"], reverse=True)
                breach["slowest_tasks"] = task_durations[:_TOP_SLOW_TASKS]

            except Exception as exc:
                print(f"[WARN] Could not fetch tasks for {dag_id}/{run_id}: {exc}")
                breach["slowest_tasks"] = []

            enriched.append(breach)

        return enriched

    @task(task_id="export_report")
    def export_report(enriched: list[dict]) -> str:
        """Write the breach report to disk and print a summary table to the task log."""
        import json
        import csv

        fmt = _OUTPUT_FORMAT.lower()
        output_path = os.path.join(_OUTPUT_DIR, f"sla_breach_report.{fmt}")

        if fmt == "csv":
            fieldnames = [
                "dag_id", "run_id", "state",
                "start_date", "end_date",
                "duration_minutes", "sla_exceeded_by_minutes",
            ]
            with open(output_path, "w", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(enriched)
        else:
            with open(output_path, "w") as fh:
                json.dump(enriched, fh, indent=2, default=str)

        width = 82
        print("=" * width)
        print(f"  {'SLA BREACH REPORT':^{width - 4}}")
        print(f"  SLA threshold : {_SLA_MINUTES} minutes")
        print(f"  Lookback      : {_LOOKBACK_DAYS} days")
        print(f"  Total breaches: {len(enriched)}")
        print("=" * width)

        if enriched:
            print(f"  {'DAG ID':<38} {'Exceeded By':>12}  {'Duration':>10}  State")
            print("-" * width)
            for b in enriched[:20]:
                dag_id = b["dag_id"][:36]
                exceeded = f"{b['sla_exceeded_by_minutes']}m"
                duration = f"{b['duration_minutes']}m"
                print(f"  {dag_id:<38} {exceeded:>12}  {duration:>10}  {b['state']}")
                for t in b.get("slowest_tasks", []):
                    print(f"    └─ {t['task_id']} ({t['duration_seconds']}s, try #{t['try_number']})")
            if len(enriched) > 20:
                print(f"  ... and {len(enriched) - 20} more — see {output_path}")

        print("=" * width)
        print(f"  Report written: {output_path}")
        print("=" * width)
        return output_path

    dag_runs = fetch_dag_runs()
    breaches = identify_breaches(dag_runs)
    enriched = fetch_slow_tasks(breaches)
    export_report(enriched)


sla_breach_reporter()
