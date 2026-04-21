"""
DAG: dag_dependency_validator
Purpose: Crawl all DAG definitions, extract upstream/downstream dependencies,
detect cycles, missing upstream DAGs, and orphaned DAGs.

Useful for impact analysis before pausing or deleting a DAG.

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

from datetime import datetime

from airflow.sdk import dag, task

from utility.airflow_api_client import get_session


def _detect_cycles(graph: dict[str, list[str]]) -> list[list[str]]:
    """DFS-based cycle detection. Returns each cycle as an ordered list of dag_ids."""
    visited: set[str] = set()
    rec_stack: set[str] = set()
    cycles: list[list[str]] = []
    path: list[str] = []

    def _dfs(node: str) -> None:
        visited.add(node)
        rec_stack.add(node)
        path.append(node)
        for neighbor in graph.get(node, []):
            if neighbor not in visited:
                _dfs(neighbor)
            elif neighbor in rec_stack:
                cycle_start = path.index(neighbor)
                cycles.append(path[cycle_start:] + [neighbor])
        path.pop()
        rec_stack.discard(node)

    for node in list(graph.keys()):
        if node not in visited:
            _dfs(node)
    return cycles


@dag(
    dag_id="12_dag_dependency_validator",
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
## DAG Dependency Validator

Crawls all DAG definitions via the Airflow REST API, extracts upstream/downstream
dependency relationships, and detects structural issues:

- **Cycles**: DAGs that form circular trigger chains (A → B → A)
- **Missing upstream**: DAGs referenced as a dependency that no longer exist
- **Orphaned DAGs**: DAGs with no upstream or downstream relationships at all

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
airflow dags trigger 12_dag_dependency_validator
```
""",
)
def dag_dependency_validator():

    @task(task_id="fetch_all_dags")
    def fetch_all_dags() -> list[str]:
        """Return all dag_ids from the metastore via paginated REST API."""
        base_url, session = get_session()
        dag_ids: list[str] = []
        limit, offset = 100, 0

        while True:
            resp = session.get(
                f"{base_url}/api/v2/dags",
                params={"limit": limit, "offset": offset},
                timeout=30,
            )
            resp.raise_for_status()
            payload = resp.json()
            batch = payload.get("dags", [])
            dag_ids.extend(d["dag_id"] for d in batch)
            total = payload.get("total_entries", len(dag_ids))
            offset += len(batch)
            if offset >= total or not batch:
                break

        print(f"[INFO] Found {len(dag_ids)} DAG(s).")
        return dag_ids

    @task(task_id="fetch_dag_dependencies")
    def fetch_dag_dependencies(dag_ids: list[str]) -> dict:
        """
        Fetch the full dependency graph from GET /api/v2/dag_dependencies.
        Returns {dag_id: {upstream: [...], downstream: [...]}} for every known DAG.
        """
        base_url, session = get_session()
        resp = session.get(
            f"{base_url}/api/v2/dag_dependencies",
            timeout=30,
        )
        resp.raise_for_status()

        graph: dict[str, dict] = {
            dag_id: {"upstream": [], "downstream": []} for dag_id in dag_ids
        }

        for dep in resp.json().get("dag_dependencies", []):
            source = dep.get("source_dag_id", "")
            target = dep.get("target_dag_id", "")
            if not source or not target:
                continue
            if source in graph and target not in graph[source]["downstream"]:
                graph[source]["downstream"].append(target)
            if target in graph and source not in graph[target]["upstream"]:
                graph[target]["upstream"].append(source)

        print(f"[INFO] Dependency graph built for {len(graph)} DAG(s).")
        return graph

    @task(task_id="detect_issues")
    def detect_issues(dag_ids: list[str], graph: dict) -> dict:
        """
        Analyse the dependency graph for:
        - Cycles: circular trigger chains
        - Missing upstream: upstream dag_ids not present in the metastore
        - Orphaned: DAGs with zero upstream and zero downstream connections
        """
        known = set(dag_ids)

        downstream_graph: dict[str, list[str]] = {
            dag_id: info.get("downstream", []) for dag_id, info in graph.items()
        }
        cycles = _detect_cycles(downstream_graph)

        missing_upstream: dict[str, list[str]] = {}
        for dag_id, info in graph.items():
            absent = [u for u in info.get("upstream", []) if u not in known]
            if absent:
                missing_upstream[dag_id] = absent

        orphaned = [
            dag_id for dag_id, info in graph.items()
            if not info.get("upstream") and not info.get("downstream")
        ]

        return {
            "total_dags": len(dag_ids),
            "cycles": cycles,
            "missing_upstream": missing_upstream,
            "orphaned": orphaned,
        }

    @task(task_id="print_report")
    def print_report(issues: dict) -> None:
        """Print the dependency validation report to the task log."""
        width = 72
        print("=" * width)
        print(f"  {'DAG DEPENDENCY VALIDATION REPORT':^{width - 4}}")
        print(f"  Total DAGs analysed: {issues['total_dags']}")
        print("=" * width)

        cycles = issues.get("cycles", [])
        print(f"\n[CYCLES] Found: {len(cycles)}")
        if cycles:
            for i, cycle in enumerate(cycles, 1):
                print(f"  {i}. {' → '.join(cycle)}")
        else:
            print("  None detected.")

        missing = issues.get("missing_upstream", {})
        print(f"\n[MISSING UPSTREAM] Found: {len(missing)} DAG(s)")
        if missing:
            for dag_id, upstream_list in sorted(missing.items()):
                print(f"  {dag_id}")
                for u in upstream_list:
                    print(f"    └─ missing: {u}")
        else:
            print("  None detected.")

        orphaned = issues.get("orphaned", [])
        print(f"\n[ORPHANED DAGs] Found: {len(orphaned)}")
        if orphaned:
            for dag_id in sorted(orphaned):
                print(f"  - {dag_id}")
        else:
            print("  None detected.")

        print("\n" + "=" * width)
        clean = not cycles and not missing and not orphaned
        verdict = "PASSED — no dependency issues found." if clean else "ISSUES DETECTED — review above."
        print(f"  Result: {verdict}")
        print("=" * width)

    all_dags = fetch_all_dags()
    dep_graph = fetch_dag_dependencies(all_dags)
    issues = detect_issues(all_dags, dep_graph)
    print_report(issues)


dag_dependency_validator()
