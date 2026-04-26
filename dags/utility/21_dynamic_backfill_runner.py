"""
DAG: 21_dynamic_backfill_runner

Accept a date range and a target DAG id, then safely trigger one backfill
run per logical date using the Airflow REST API — with per-run exponential
back-off retry on transient failures. More predictable than running
``airflow dags backfill`` from the CLI, and safe to run from within the
scheduler because it drives triggers through the REST API rather than
bypassing it.

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

Trigger with conf:
  {
    "target_dag_id" : "my_etl_dag",
    "start_date"    : "2024-01-01",
    "end_date"      : "2024-01-07",
    "interval"      : "daily",
    "max_retries"   : 3,
    "base_delay_s"  : 5,
    "run_conf"      : {}
  }
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, get_current_context, task

from utility.airflow_api_client import get_session

_TERMINAL_STATES = {"success", "failed", "error"}
_POLL_INTERVAL_S = 15
_POLL_TIMEOUT_S = 600


def _parse_date(s: str) -> datetime:
    """Parse YYYY-MM-DD or ISO-8601 string into a UTC-aware datetime."""
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date string: {s!r}")


def _generate_dates(start: datetime, end: datetime, interval: str) -> list[str]:
    """Return ISO-8601 UTC strings for every step between start and end (inclusive)."""
    step = timedelta(hours=1) if interval == "hourly" else timedelta(days=1)
    dates: list[str] = []
    current = start
    while current <= end:
        dates.append(current.isoformat())
        current += step
    return dates


@dag(
    dag_id="21_dynamic_backfill_runner",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=[
        "type=utility",
        "exec=kube", "exec=compose",
        "subtype=orchestration",
        "intent=utility",
    ],
    doc_md="""
## Dynamic Backfill Runner

Triggers one DAG run per logical date over a given date range using the
Airflow REST API. Uses exponential back-off retry on transient trigger
failures. Safer than `airflow dags backfill` because it respects the live
scheduler, avoids locking issues, and produces a visible audit trail in the
Airflow UI.

**Trigger conf keys:**
| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `target_dag_id` | yes | — | DAG to backfill |
| `start_date` | yes | — | First logical date (`YYYY-MM-DD`) |
| `end_date` | yes | — | Last logical date (`YYYY-MM-DD`, inclusive) |
| `interval` | no | `daily` | `daily` or `hourly` |
| `max_retries` | no | `3` | Retries per date on trigger failure |
| `base_delay_s` | no | `5` | Base delay for exponential back-off (seconds) |
| `run_conf` | no | `{}` | Conf dict forwarded to every triggered run |

**Example trigger:**
```bash
airflow dags trigger 21_dynamic_backfill_runner --conf '{
  "target_dag_id": "my_etl_dag",
  "start_date": "2024-03-01",
  "end_date": "2024-03-07"
}'
```
""",
)
def dynamic_backfill_runner():

    @task(task_id="generate_date_range")
    def generate_date_range() -> dict:
        """Parse conf and produce the ordered list of logical dates to backfill."""
        context = get_current_context()
        conf = context["dag_run"].conf or {}

        target_dag_id = conf.get("target_dag_id", "")
        if not target_dag_id:
            raise ValueError("conf['target_dag_id'] is required.")

        start_str = conf.get("start_date", "")
        end_str = conf.get("end_date", "")
        if not start_str or not end_str:
            raise ValueError(
                "conf['start_date'] and conf['end_date'] are required (YYYY-MM-DD)."
            )

        start_dt = _parse_date(start_str)
        end_dt = _parse_date(end_str)
        if end_dt < start_dt:
            raise ValueError(
                f"end_date ({end_str}) must be >= start_date ({start_str})."
            )

        interval = conf.get("interval", "daily")
        if interval not in ("daily", "hourly"):
            raise ValueError("conf['interval'] must be 'daily' or 'hourly'.")

        dates = _generate_dates(start_dt, end_dt, interval)
        print(f"[INFO] Backfill target: '{target_dag_id}'  interval={interval}")
        print(f"[INFO] Date range: {start_str} → {end_str}  ({len(dates)} run(s) queued)")

        return {
            "target_dag_id": target_dag_id,
            "dates": dates,
            "max_retries": int(conf.get("max_retries", 3)),
            "base_delay_s": int(conf.get("base_delay_s", 5)),
            "run_conf": conf.get("run_conf", {}),
        }

    @task(task_id="trigger_backfill_runs")
    def trigger_backfill_runs(params: dict) -> list[dict]:
        """
        Trigger one DAG run per logical date with exponential back-off retry.
        409 Conflict (run already exists) is treated as success to make
        re-triggering this DAG idempotent.
        """
        target_dag_id = params["target_dag_id"]
        dates: list[str] = params["dates"]
        max_retries: int = params["max_retries"]
        base_delay: int = params["base_delay_s"]
        run_conf: dict = params["run_conf"]

        base_url, session = get_session()
        outcomes: list[dict] = []

        for logical_date in dates:
            # Build a short, filesystem-safe run_id slug from the date string
            date_slug = logical_date.replace(":", "").replace("+", "").replace("-", "")[:15]
            run_id = f"backfill__{date_slug}"
            triggered = False

            for attempt in range(max_retries + 1):
                resp = session.post(
                    f"{base_url}/api/v2/dags/{target_dag_id}/dagRuns",
                    json={
                        "dag_run_id": run_id,
                        "logical_date": logical_date,
                        "conf": run_conf,
                    },
                    timeout=30,
                )
                if resp.status_code in (200, 201):
                    actual_run_id = (
                        resp.json().get("dag_run_id") or resp.json().get("run_id") or run_id
                    )
                    print(
                        f"[OK] Triggered {logical_date} → run_id={actual_run_id} "
                        f"(attempt {attempt + 1})"
                    )
                    outcomes.append({
                        "logical_date": logical_date,
                        "run_id": actual_run_id,
                        "status": "triggered",
                    })
                    triggered = True
                    break
                elif resp.status_code == 409:
                    print(f"[SKIP] Run for {logical_date} already exists (409) — skipping.")
                    outcomes.append({
                        "logical_date": logical_date,
                        "run_id": run_id,
                        "status": "already_exists",
                    })
                    triggered = True
                    break
                else:
                    delay = base_delay * (2 ** attempt)
                    print(
                        f"[WARN] Trigger failed for {logical_date}: {resp.status_code} "
                        f"(attempt {attempt + 1}/{max_retries + 1}) — retrying in {delay}s…"
                    )
                    if attempt < max_retries:
                        time.sleep(delay)

            if not triggered:
                print(f"[ERROR] All retries exhausted for {logical_date}.")
                outcomes.append({
                    "logical_date": logical_date,
                    "run_id": run_id,
                    "status": "failed_to_trigger",
                })

        return outcomes

    @task(task_id="poll_backfill_runs")
    def poll_backfill_runs(params: dict, outcomes: list[dict]) -> list[dict]:
        """
        Poll each triggered run until it reaches a terminal state or the
        per-run timeout (_POLL_TIMEOUT_S) is exceeded.
        """
        target_dag_id = params["target_dag_id"]
        base_url, session = get_session()
        final_outcomes: list[dict] = []

        for item in outcomes:
            if item["status"] not in ("triggered", "already_exists"):
                final_outcomes.append({**item, "final_state": "skipped"})
                continue

            run_id = item["run_id"]
            deadline = time.monotonic() + _POLL_TIMEOUT_S
            final_state = "unknown"

            while time.monotonic() < deadline:
                resp = session.get(
                    f"{base_url}/api/v2/dags/{target_dag_id}/dagRuns/{run_id}",
                    timeout=30,
                )
                if resp.status_code == 200:
                    state = resp.json().get("state", "")
                    if state in _TERMINAL_STATES:
                        final_state = state
                        print(f"[DONE] {item['logical_date']} → {final_state}")
                        break
                    print(f"[POLL] {item['logical_date']}: state={state}")
                else:
                    print(f"[WARN] Poll {run_id}: {resp.status_code}")
                time.sleep(_POLL_INTERVAL_S)
            else:
                final_state = "timeout"
                print(f"[WARN] Timeout waiting for {run_id}.")

            final_outcomes.append({**item, "final_state": final_state})

        return final_outcomes

    @task(task_id="print_backfill_report")
    def print_backfill_report(params: dict, final_outcomes: list[dict]) -> None:
        """Print a structured backfill result table with per-date state."""
        target_dag_id = params["target_dag_id"]
        width = 80
        print("=" * width)
        print(f"  {'BACKFILL REPORT — ' + target_dag_id:^{width - 4}}")
        print("=" * width)

        counters: dict[str, int] = {}
        for item in final_outcomes:
            state = item.get("final_state", "unknown")
            counters[state] = counters.get(state, 0) + 1
            if state == "success":
                icon = "OK"
            elif state in ("already_exists", "skipped"):
                icon = "--"
            else:
                icon = "!!"
            print(
                f"  [{icon}] {item['logical_date']:<26}  "
                f"run_id={item.get('run_id', '-'):<35}  {state}"
            )

        print("-" * width)
        print("  Summary: " + "  ".join(f"{k}={v}" for k, v in sorted(counters.items())))
        print("=" * width)

    params = generate_date_range()
    outcomes = trigger_backfill_runs(params)
    final_outcomes = poll_backfill_runs(params, outcomes)
    print_backfill_report(params, final_outcomes)


dynamic_backfill_runner()
