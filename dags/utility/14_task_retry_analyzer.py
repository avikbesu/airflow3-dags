"""
DAG: task_retry_analyzer
Purpose: Identify tasks with high retry rates, group them by inferred failure
category (sensor timeout, resource constraint, transient error, unknown), and
surface tuning recommendations.

Queries task instances with try_number > 1 from the Airflow REST API over a
configurable lookback window.

Tune the constants below to adjust the analysis scope.

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

import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

_CONN_ID = "airflow_api"
_DEFAULT_API_URL = "http://localhost:8080"

_LOOKBACK_DAYS = 14   # days of task instance history to scan
_MIN_RETRIES = 1      # minimum retry count for a task to be included in the report
_TOP_N = 20           # number of high-retry tasks to surface


# Keyword-based heuristic to categorise failure reasons from operator/state names.
# Maps category label → list of keywords to match against task_id or operator name.
_FAILURE_CATEGORIES: dict[str, list[str]] = {
    "sensor_timeout": ["sensor", "wait", "poking", "poke", "deferrable"],
    "resource_constraint": ["pool", "slot", "quota", "limit", "memory", "cpu", "resource"],
    "transient_error": ["transient", "retry", "flaky", "network", "connection", "timeout", "http"],
}


def _resolve_auth() -> tuple[str, object]:
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


def _infer_category(task_id: str, operator: str) -> str:
    """Return the first matching failure category based on task_id / operator keywords."""
    combined = f"{task_id} {operator}".lower()
    for category, keywords in _FAILURE_CATEGORIES.items():
        if any(kw in combined for kw in keywords):
            return category
    return "unknown"


@dag(
    dag_id="14_task_retry_analyzer",
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
## Task Retry Analyzer

Scans task instance history for tasks with elevated retry counts, groups them by
inferred failure category, and prints tuning recommendations.

**Failure categories** (keyword-based heuristic):
- `sensor_timeout` — sensor/wait/poking tasks
- `resource_constraint` — pool/slot/quota/memory tasks
- `transient_error` — network/timeout/flaky tasks
- `unknown` — everything else

**Tune the module-level constants** in the DAG file:

| Constant | Default | Description |
|----------|---------|-------------|
| `_LOOKBACK_DAYS` | 14 | Days of task instance history to scan |
| `_MIN_RETRIES` | 1 | Minimum retry count to include a task |
| `_TOP_N` | 20 | Tasks to surface in the report |

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
airflow dags trigger 14_task_retry_analyzer
```
""",
)
def task_retry_analyzer():

    @task(task_id="fetch_retried_task_instances")
    def fetch_retried_task_instances() -> list[dict]:
        """
        Fetch task instances with try_number > 1 from the last _LOOKBACK_DAYS.
        Uses GET /api/v2/dags/~/dagRuns/~/taskInstances (wildcard endpoint).
        """
        import requests

        cutoff = (datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        base_url, auth = _resolve_auth()
        all_instances: list[dict] = []
        limit, offset = 100, 0

        while True:
            resp = requests.get(
                f"{base_url}/api/v2/dags/~/dagRuns/~/taskInstances",
                auth=auth,
                params={
                    "limit": limit,
                    "offset": offset,
                    "updated_at_gte": cutoff,
                },
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()
            batch = payload.get("task_instances", [])
            # Keep only instances that were retried at least once
            retried = [ti for ti in batch if (ti.get("try_number") or 1) > _MIN_RETRIES]
            all_instances.extend(retried)
            total = payload.get("total_entries", 0)
            offset += len(batch)
            if offset >= total or not batch:
                break

        print(f"[INFO] Found {len(all_instances)} retried task instance(s) in the last {_LOOKBACK_DAYS} day(s).")
        return all_instances

    @task(task_id="aggregate_retry_stats")
    def aggregate_retry_stats(instances: list[dict]) -> list[dict]:
        """
        Aggregate retry counts by (dag_id, task_id).
        Computes total_tries, max_try_number, run_count, and avg_tries_per_run.
        """
        from collections import defaultdict

        stats: dict[tuple, dict] = defaultdict(lambda: {
            "total_tries": 0,
            "max_try_number": 0,
            "run_count": 0,
            "operator": "",
            "states": [],
        })

        for ti in instances:
            key = (ti.get("dag_id", ""), ti.get("task_id", ""))
            entry = stats[key]
            try_num = ti.get("try_number") or 1
            entry["total_tries"] += try_num
            entry["max_try_number"] = max(entry["max_try_number"], try_num)
            entry["run_count"] += 1
            if not entry["operator"]:
                entry["operator"] = ti.get("operator", "")
            state = ti.get("state")
            if state:
                entry["states"].append(state)

        aggregated: list[dict] = []
        for (dag_id, task_id), data in stats.items():
            run_count = data["run_count"]
            total_tries = data["total_tries"]
            avg_tries = round(total_tries / run_count, 2) if run_count else 0
            retry_rate = round((total_tries - run_count) / run_count, 2) if run_count else 0
            aggregated.append({
                "dag_id": dag_id,
                "task_id": task_id,
                "operator": data["operator"],
                "run_count": run_count,
                "total_tries": total_tries,
                "max_try_number": data["max_try_number"],
                "avg_tries_per_run": avg_tries,
                "retry_rate": retry_rate,  # avg extra tries per run
                "failure_category": _infer_category(task_id, data["operator"]),
            })

        aggregated.sort(key=lambda x: x["retry_rate"], reverse=True)
        print(f"[INFO] Aggregated stats for {len(aggregated)} unique (dag_id, task_id) pair(s).")
        return aggregated

    @task(task_id="rank_offenders")
    def rank_offenders(aggregated: list[dict]) -> dict:
        """
        Return top _TOP_N retry offenders and group counts by failure_category.
        """
        from collections import Counter

        top = aggregated[:_TOP_N]
        category_counts = dict(Counter(r["failure_category"] for r in aggregated))
        by_category: dict[str, list[dict]] = {}
        for record in aggregated:
            cat = record["failure_category"]
            by_category.setdefault(cat, []).append(record)

        print(f"[INFO] Top {len(top)} offenders identified across {len(category_counts)} category(ies).")
        return {
            "top_offenders": top,
            "category_counts": category_counts,
            "by_category": by_category,
        }

    @task(task_id="print_recommendations")
    def print_recommendations(ranked: dict) -> None:
        """Print the retry analysis report and category-specific tuning recommendations."""
        recommendations = {
            "sensor_timeout": (
                "Consider increasing `poke_interval` and `timeout`, or switch to "
                "deferrable sensor mode to release worker slots while waiting."
            ),
            "resource_constraint": (
                "Review pool slot allocation and task concurrency settings. "
                "Consider adding more pool slots or splitting the DAG into priority tiers."
            ),
            "transient_error": (
                "Tune `retries` and `retry_delay` with exponential back-off. "
                "Investigate upstream service stability or add circuit-breaker logic."
            ),
            "unknown": (
                "Inspect task logs for recurring exception patterns. "
                "Extract and test the core logic independently to isolate the root cause."
            ),
        }

        top = ranked.get("top_offenders", [])
        category_counts = ranked.get("category_counts", {})
        by_category = ranked.get("by_category", {})

        width = 90
        print("=" * width)
        print(f"  {'TASK RETRY ANALYSIS REPORT':^{width - 4}}")
        print(f"  Lookback: {_LOOKBACK_DAYS} days  |  Min retries: {_MIN_RETRIES}  |  Top N: {_TOP_N}")
        print("=" * width)

        print(f"\n[SUMMARY BY CATEGORY]")
        for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
            print(f"  {cat:<25} {count:>5} task(s)")

        print(f"\n[TOP {len(top)} HIGH-RETRY TASKS]")
        print(f"  {'DAG ID':<30} {'Task ID':<28} {'Retry Rate':>10}  {'Max Try':>7}  Category")
        print("-" * width)
        for r in top:
            dag_id = r["dag_id"][:28]
            task_id = r["task_id"][:26]
            rate = f"{r['retry_rate']:.2f}"
            max_try = str(r["max_try_number"])
            cat = r["failure_category"]
            print(f"  {dag_id:<30} {task_id:<28} {rate:>10}  {max_try:>7}  {cat}")

        print(f"\n[TUNING RECOMMENDATIONS]")
        for cat in sorted(by_category.keys()):
            tasks_in_cat = by_category[cat]
            advice = recommendations.get(cat, recommendations["unknown"])
            print(f"\n  [{cat.upper()}] — {len(tasks_in_cat)} task(s)")
            print(f"  Advice: {advice}")
            print(f"  Top tasks:")
            for r in tasks_in_cat[:3]:
                print(f"    - {r['dag_id']}.{r['task_id']} (retry rate: {r['retry_rate']:.2f})")

        print("\n" + "=" * width)

    instances = fetch_retried_task_instances()
    aggregated = aggregate_retry_stats(instances)
    ranked = rank_offenders(aggregated)
    print_recommendations(ranked)


task_retry_analyzer()
