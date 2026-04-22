"""
DAG: 22_branching_dag_router

Route execution to one variant DAG from a routing table based on a
``route`` key in the run conf. All non-selected variants are paused; the
selected variant is unpaused and then optionally triggered. Useful for
A/B experiments, feature flags, and canary deployments where only one
pipeline branch should be active at a time.

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
    "route"           : "variant_b",
    "routing_table"   : {
      "variant_a": ["feature_dag_v1"],
      "variant_b": ["feature_dag_v2"],
      "control"  : ["feature_dag_stable"]
    },
    "trigger_selected": true,
    "trigger_conf"    : {"env": "prod"},
    "poll_interval_s" : 10,
    "poll_timeout_s"  : 300
  }
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

from airflow.sdk import dag, get_current_context, task

from utility.airflow_api_client import get_session

_TERMINAL_STATES = {"success", "failed", "error"}


@dag(
    dag_id="22_branching_dag_router",
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
## Branching DAG Router

Routes a workflow to one variant DAG from a routing table based on a
`route` key in the run conf. Non-selected variants are **paused**, the
selected variant is **unpaused**, then optionally **triggered**. Useful
for A/B testing pipelines, feature flags, and canary deployments.

**Trigger conf keys:**
| Key | Required | Default | Description |
|-----|----------|---------|-------------|
| `route` | yes | — | Which branch to activate (must be a key in `routing_table`) |
| `routing_table` | yes | — | `{route_name: [dag_id, ...]}` mapping |
| `trigger_selected` | no | `true` | Whether to trigger the selected DAG after unpausing |
| `trigger_conf` | no | `{}` | Conf dict forwarded to the triggered run |
| `poll_interval_s` | no | `10` | Seconds between status polls |
| `poll_timeout_s` | no | `300` | Max seconds to wait per triggered run |

**Example trigger:**
```bash
airflow dags trigger 22_branching_dag_router --conf '{
  "route": "variant_b",
  "routing_table": {
    "variant_a": ["feature_dag_v1"],
    "variant_b": ["feature_dag_v2"],
    "control":   ["feature_dag_stable"]
  }
}'
```
""",
)
def branching_dag_router():

    @task(task_id="resolve_route")
    def resolve_route() -> dict:
        """
        Parse conf and compute which DAGs to activate vs deactivate.
        Returns a routing decision dict consumed by downstream tasks.
        """
        context = get_current_context()
        conf = context["dag_run"].conf or {}

        route = conf.get("route", "")
        if not route:
            raise ValueError("conf['route'] is required.")

        routing_table: dict[str, list[str]] = conf.get("routing_table", {})
        if not routing_table:
            raise ValueError("conf['routing_table'] is required.")

        if route not in routing_table:
            raise ValueError(
                f"Route '{route}' not found in routing_table. "
                f"Available routes: {list(routing_table.keys())}"
            )

        selected_dags: list[str] = routing_table[route]

        # Deduplicated ordered list of every DAG mentioned in the table
        all_dag_ids: list[str] = []
        for dag_list in routing_table.values():
            for d in dag_list:
                if d not in all_dag_ids:
                    all_dag_ids.append(d)

        deselected_dags: list[str] = [d for d in all_dag_ids if d not in selected_dags]

        print(f"[INFO] Active route   : '{route}'")
        print(f"[INFO] Will UNPAUSE   : {selected_dags}")
        print(f"[INFO] Will PAUSE     : {deselected_dags}")

        return {
            "route": route,
            "selected_dags": selected_dags,
            "deselected_dags": deselected_dags,
            "trigger_selected": bool(conf.get("trigger_selected", True)),
            "trigger_conf": conf.get("trigger_conf", {}),
            "poll_interval_s": int(conf.get("poll_interval_s", 10)),
            "poll_timeout_s": int(conf.get("poll_timeout_s", 300)),
        }

    @task(task_id="apply_pause_resume")
    def apply_pause_resume(decision: dict) -> dict:
        """
        Pause all non-selected DAGs and unpause each selected DAG via
        PATCH /api/v2/dags/{dag_id}. Returns the decision dict enriched
        with a ``changes`` list for auditing.
        """
        base_url, session = get_session()
        changes: list[dict] = []

        for dag_id in decision["deselected_dags"]:
            resp = session.patch(
                f"{base_url}/api/v2/dags/{dag_id}",
                json={"is_paused": True},
                timeout=30,
            )
            result = "paused" if resp.status_code == 200 else f"error({resp.status_code})"
            print(f"[PAUSE]   {dag_id}: {result}")
            changes.append({"dag_id": dag_id, "action": "pause", "result": result})

        for dag_id in decision["selected_dags"]:
            resp = session.patch(
                f"{base_url}/api/v2/dags/{dag_id}",
                json={"is_paused": False},
                timeout=30,
            )
            result = "unpaused" if resp.status_code == 200 else f"error({resp.status_code})"
            print(f"[UNPAUSE] {dag_id}: {result}")
            changes.append({"dag_id": dag_id, "action": "unpause", "result": result})

        return {**decision, "changes": changes}

    @task(task_id="trigger_selected_variant")
    def trigger_selected_variant(state: dict) -> list[dict]:
        """
        If trigger_selected is True, POST a new run for each selected DAG.
        Returns a list of triggered run records for the polling step.
        """
        if not state.get("trigger_selected", True):
            print("[INFO] trigger_selected=false — skipping trigger step.")
            return []

        base_url, session = get_session()
        triggered: list[dict] = []

        for dag_id in state["selected_dags"]:
            now_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            run_id = f"router__{state['route']}__{now_ts}"
            resp = session.post(
                f"{base_url}/api/v2/dags/{dag_id}/dagRuns",
                json={"dag_run_id": run_id, "conf": state["trigger_conf"]},
                timeout=30,
            )
            if resp.status_code in (200, 201):
                actual_run_id = (
                    resp.json().get("dag_run_id") or resp.json().get("run_id") or run_id
                )
                print(f"[TRIGGERED] '{dag_id}': run_id={actual_run_id}")
                triggered.append({
                    "dag_id": dag_id,
                    "run_id": actual_run_id,
                    "route": state["route"],
                })
            else:
                print(
                    f"[WARN] Failed to trigger '{dag_id}': "
                    f"{resp.status_code} {resp.text[:200]}"
                )
                triggered.append({
                    "dag_id": dag_id,
                    "run_id": None,
                    "route": state["route"],
                    "error": resp.text[:200],
                })

        return triggered

    @task(task_id="poll_and_summarise")
    def poll_and_summarise(state: dict, triggered: list[dict]) -> None:
        """
        Poll each triggered run until terminal or timeout, then print a
        routing report covering pause/unpause actions and final run states.
        """
        poll_interval = state["poll_interval_s"]
        poll_timeout = state["poll_timeout_s"]
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
                    run_state = resp.json().get("state", "")
                    if run_state in _TERMINAL_STATES:
                        final_state = run_state
                        break
                    print(f"[POLL] '{dag_id}': state={run_state} — waiting {poll_interval}s…")
                else:
                    print(f"[WARN] Poll failed for '{dag_id}/{run_id}': {resp.status_code}")
                time.sleep(poll_interval)
            else:
                final_state = "timeout"
                print(f"[WARN] Timed out waiting for '{dag_id}/{run_id}'.")

            results.append({**item, "final_state": final_state})

        width = 72
        print("\n" + "=" * width)
        print(f"  {'BRANCHING DAG ROUTER — REPORT':^{width - 4}}")
        print("=" * width)
        print(f"  Active route : {state['route']}")
        print(f"  Unpaused     : {', '.join(state['selected_dags']) or 'none'}")
        print(f"  Paused       : {', '.join(state['deselected_dags']) or 'none'}")
        print("-" * width)
        if results:
            print("  Triggered runs:")
            for r in results:
                icon = "OK" if r.get("final_state") == "success" else "!!"
                print(
                    f"  [{icon}] {r['dag_id']:<40}  "
                    f"run_id={r.get('run_id') or '-'}"
                )
                print(f"       final_state={r.get('final_state', 'unknown')}")
        else:
            print("  No runs triggered (trigger_selected=false).")
        print("=" * width)

    decision = resolve_route()
    state = apply_pause_resume(decision)
    triggered = trigger_selected_variant(state)
    poll_and_summarise(state, triggered)


branching_dag_router()
