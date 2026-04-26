"""
DAG: 20_smart_dag_trigger_orchestrator

Smart conditional fan-out orchestrator. Checks whether an upstream DAG's
most recent run succeeded, collects its XCom output, then conditionally
triggers one or more downstream DAGs — forwarding the XCom payload as
run ``conf``.

Useful for complex fan-out pipelines where downstream DAGs should only
fire when upstream data is confirmed ready.

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
    "upstream_dag_id"   : "my_source_dag",
    "upstream_xcom_task": "produce_result",
    "upstream_xcom_key" : "return_value",
    "downstream_dag_ids": ["dag_a", "dag_b"],
    "extra_conf"        : {"env": "prod"},
    "poll_interval_s"   : 10,
    "poll_timeout_s"    : 300
  }
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

from airflow.sdk import dag, get_current_context, task

from utility.airflow_api_client import get_session

_TERMINAL_STATES = {"success", "failed", "error"}


@dag(
    dag_id="20_smart_dag_trigger_orchestrator",
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
## Smart DAG Trigger Orchestrator

Conditionally triggers downstream DAGs based on an upstream DAG's success,
forwarding the upstream XCom as run `conf`. Useful for fan-out pipelines.

**Trigger conf keys:**
| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `upstream_dag_id` | yes | — | DAG to inspect |
| `upstream_xcom_task` | no | `""` | Task that pushed the XCom |
| `upstream_xcom_key` | no | `return_value` | XCom key to retrieve |
| `downstream_dag_ids` | yes | — | List of DAGs to fan-out to |
| `extra_conf` | no | `{}` | Extra keys merged into every triggered run conf |
| `poll_interval_s` | no | `10` | Seconds between status polls |
| `poll_timeout_s` | no | `300` | Max seconds to wait per triggered run |

**Example trigger:**
```bash
airflow dags trigger 20_smart_dag_trigger_orchestrator --conf '{
  "upstream_dag_id": "my_etl_dag",
  "upstream_xcom_task": "transform",
  "downstream_dag_ids": ["report_dag", "notify_dag"],
  "extra_conf": {"env": "prod"}
}'
```
""",
)
def smart_dag_trigger_orchestrator():

    @task(task_id="check_upstream_success")
    def check_upstream_success() -> dict:
        """
        Fetch the most recent run of the upstream DAG via REST API.
        Raises RuntimeError if the last run is not in 'success' state.
        """
        context = get_current_context()
        conf = context["dag_run"].conf or {}
        upstream_dag_id = conf.get("upstream_dag_id", "")
        if not upstream_dag_id:
            raise ValueError("conf['upstream_dag_id'] is required.")

        base_url, session = get_session()
        resp = session.get(
            f"{base_url}/api/v2/dags/{upstream_dag_id}/dagRuns",
            params={"limit": 1, "order_by": "-start_date"},
            timeout=30,
        )
        resp.raise_for_status()
        runs = resp.json().get("dag_runs", [])
        if not runs:
            raise RuntimeError(f"No runs found for upstream DAG '{upstream_dag_id}'.")

        run = runs[0]
        run_id = run.get("dag_run_id") or run.get("run_id")
        state = run.get("state")
        print(f"[INFO] Upstream '{upstream_dag_id}' latest run: {run_id}  state={state}")

        if state != "success":
            raise RuntimeError(
                f"Upstream DAG '{upstream_dag_id}' last run '{run_id}' "
                f"is '{state}', not 'success'. Aborting fan-out."
            )

        return {
            "upstream_dag_id": upstream_dag_id,
            "upstream_run_id": run_id,
            "state": state,
            "start_date": run.get("start_date"),
            "end_date": run.get("end_date"),
        }

    @task(task_id="collect_upstream_xcom")
    def collect_upstream_xcom(upstream_info: dict) -> dict:
        """
        Retrieve XCom from the upstream run and assemble the run conf dict
        that will be forwarded to every downstream trigger.
        """
        context = get_current_context()
        conf = context["dag_run"].conf or {}
        xcom_task = conf.get("upstream_xcom_task", "")
        xcom_key = conf.get("upstream_xcom_key", "return_value")
        extra_conf = conf.get("extra_conf", {})

        dag_id = upstream_info["upstream_dag_id"]
        run_id = upstream_info["upstream_run_id"]

        xcom_value = None
        if xcom_task:
            base_url, session = get_session()
            url = (
                f"{base_url}/api/v2/dags/{dag_id}/dagRuns/{run_id}"
                f"/taskInstances/{xcom_task}/xcomEntries/{xcom_key}"
            )
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                xcom_value = resp.json().get("value")
                print(f"[INFO] Collected XCom '{xcom_key}' from '{xcom_task}': {xcom_value}")
            else:
                print(
                    f"[WARN] XCom fetch returned {resp.status_code} — "
                    "continuing without XCom payload."
                )
        else:
            print("[INFO] No upstream_xcom_task specified — skipping XCom collection.")

        run_conf: dict = {}
        if isinstance(extra_conf, dict):
            run_conf.update(extra_conf)
        run_conf["_upstream_dag_id"] = dag_id
        run_conf["_upstream_run_id"] = run_id
        if xcom_value is not None:
            run_conf["_upstream_xcom"] = xcom_value

        return run_conf

    @task(task_id="trigger_downstream_dags")
    def trigger_downstream_dags(run_conf: dict) -> list[dict]:
        """
        POST a new DAG run for each downstream DAG ID with the assembled conf.
        Returns list of {dag_id, run_id} records for the polling step.
        """
        context = get_current_context()
        conf = context["dag_run"].conf or {}
        downstream_dag_ids: list[str] = conf.get("downstream_dag_ids", [])
        if not downstream_dag_ids:
            raise ValueError("conf['downstream_dag_ids'] must be a non-empty list.")

        base_url, session = get_session()
        triggered: list[dict] = []

        for target_dag_id in downstream_dag_ids:
            now_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            run_id = f"orchestrator__{now_ts}__{target_dag_id}"
            resp = session.post(
                f"{base_url}/api/v2/dags/{target_dag_id}/dagRuns",
                json={"dag_run_id": run_id, "conf": run_conf},
                timeout=30,
            )
            if resp.status_code in (200, 201):
                actual_run_id = (
                    resp.json().get("dag_run_id") or resp.json().get("run_id") or run_id
                )
                print(f"[TRIGGERED] '{target_dag_id}': run_id={actual_run_id}")
                triggered.append({"dag_id": target_dag_id, "run_id": actual_run_id})
            else:
                print(
                    f"[WARN] Failed to trigger '{target_dag_id}': "
                    f"{resp.status_code} {resp.text[:200]}"
                )
                triggered.append({
                    "dag_id": target_dag_id,
                    "run_id": None,
                    "error": resp.text[:200],
                })

        return triggered

    @task(task_id="poll_and_report")
    def poll_and_report(triggered: list[dict]) -> dict:
        """
        Poll each triggered run until it reaches a terminal state or timeout.
        Prints a summary table and returns aggregated result counts.
        """
        context = get_current_context()
        conf = context["dag_run"].conf or {}
        poll_interval = int(conf.get("poll_interval_s", 10))
        poll_timeout = int(conf.get("poll_timeout_s", 300))

        base_url, session = get_session()
        results: list[dict] = []

        for item in triggered:
            dag_id = item["dag_id"]
            run_id = item.get("run_id")
            if not run_id:
                results.append({**item, "final_state": "NOT_TRIGGERED"})
                continue

            deadline = time.monotonic() + poll_timeout
            final_state = "unknown"
            while time.monotonic() < deadline:
                resp = session.get(
                    f"{base_url}/api/v2/dags/{dag_id}/dagRuns/{run_id}",
                    timeout=30,
                )
                if resp.status_code == 200:
                    state = resp.json().get("state", "")
                    if state in _TERMINAL_STATES:
                        final_state = state
                        break
                    print(f"[POLL] '{dag_id}/{run_id}' state={state} — waiting {poll_interval}s…")
                else:
                    print(f"[WARN] Poll failed for '{dag_id}/{run_id}': {resp.status_code}")
                time.sleep(poll_interval)
            else:
                final_state = "timeout"
                print(f"[WARN] Timed out waiting for '{dag_id}/{run_id}'.")

            results.append({**item, "final_state": final_state})

        width = 72
        success_count = sum(1 for r in results if r.get("final_state") == "success")
        print("\n" + "=" * width)
        print(f"  {'FAN-OUT TRIGGER SUMMARY':^{width - 4}}")
        print("=" * width)
        print(
            f"  Triggered: {len(results)}   "
            f"Succeeded: {success_count}   "
            f"Failed/Timeout: {len(results) - success_count}"
        )
        print("-" * width)
        for r in results:
            icon = "OK" if r.get("final_state") == "success" else "!!"
            print(f"  [{icon}] {r['dag_id']:<40}  run_id={r.get('run_id') or '-'}")
            print(f"       final_state={r.get('final_state', 'unknown')}")
        print("=" * width)

        return {
            "total": len(results),
            "succeeded": success_count,
            "results": results,
        }

    upstream_info = check_upstream_success()
    run_conf = collect_upstream_xcom(upstream_info)
    triggered = trigger_downstream_dags(run_conf)
    poll_and_report(triggered)


smart_dag_trigger_orchestrator()
