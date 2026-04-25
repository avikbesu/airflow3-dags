# airflow3-dags

A collection of reference DAGs for **Apache Airflow 3**, covering XCom patterns, Kubernetes job execution, DAG introspection, variable access, and operational utilities via the Airflow 3 Task SDK.

## Repository layout

```
dags/
  utility/
    1_push_xcom_from_kube_pod_op.py   KubernetesJobOperator XCom demo
    2_log_other_dag_status.py         Monitor another DAG's run state
    3_xcom_multi_operator_demo.py     XCom across BashOperator + @task
    4_print_all_variables.py          List all Variables via REST API v2
    5_print_a_variable.py             Fetch specific Variables via Task SDK
    6_setup_variables_connections.py  Seed Variables & Connections from YAML
    7_validate_k8s_cluster_status.py  K8s cluster health report (per namespace)
    8_cleanup_k8s_jobs_pods.py        Delete finished K8s Jobs and their Pods
    9_manage_pvc.py                   Create or delete a PersistentVolumeClaim
    10_list_active_dags.py            List unpaused DAGs + latest run details
    11_xcom_api_sdk_demo.py           XCom push/pull via SDK and REST API
    12_dag_dependency_validator.py    Detect cycles & missing upstream DAGs
    13_sla_breach_reporter.py         Flag DAG runs that exceed an SLA threshold
    14_task_retry_analyzer.py         Surface high-retry tasks with root-cause hints
    15_dag_version_auditor.py         Track DAG file changes via hash baseline
    airflow_api_client.py             Shared JWT-auth helper for REST API v2
  demo/
    a1_airflow_basics_downtreams.py   cross_downstream / chain dependency demos
    a2_airflow_basics_trigger_rules.py  TriggerRule variants demo
tests/
  conftest.py                       Session-scoped Airflow home + SQLite DB setup
  test_1_dag_integrity.py           Layer 1 — DagBag parse/import tests
  test_2_unit_functions.py          Layer 2 — Unit tests for pure task functions
  test_3_task_level.py              Layer 3 — airflow tasks test (per-task CLI)
  test_4_dag_runs.py                Layer 4 — airflow dags test (full DAG run)
.pre-commit-config.yaml             Pre-commit hook (runs layers 1 & 2 on every commit)
pytest.ini                          Pytest configuration
requirements-test.txt               Test dependencies
docs/                               Additional documentation (WIP)
```

## DAGs

### 1 — k8s_job_xcom_bash_demo
Runs an alpine bash container via `KubernetesJobOperator`, writes a value to the XCom sidecar path, then reads it in a downstream `@task`.

**Tags:** `type=demo`, `exec=kube`, `subtype=xcom`

---

### 2 — monitor_dag
Queries the run state and run count of another DAG (`1_k8s_job_xcom_bash_demo`) using `ti.get_dagrun_state()` and `ti.get_dr_count()` — Airflow 3 Task SDK equivalents for metastore access.

**Tags:** `type=demo`, `exec=kube`, `subtype=dag-status`

---

### 3 — xcom_multi_operator_demo
Four-task XCom walkthrough:
- `task_1` — `BashOperator` pushes a JSON list via `output_processor`
- `task_2` — `EmptyOperator` (no XCom)
- `task_3` — `@task` pushes a list by returning it
- `task_4` — `@task` pulls from `task_1` + `task_3`, merges, and returns the combined list

**Tags:** `type=demo`, `exec=kube`, `subtype=xcom`

---

### 4 — print_all_variables
Lists every Airflow Variable by calling the REST API v2 (`GET /api/v2/variables`). Direct ORM access is blocked in Airflow 3 tasks, so this demonstrates the approved pattern.

**Auth setup (pick one):**

| Option | How |
|--------|-----|
| A — Airflow Connection (recommended) | Create conn `airflow_api` (type: HTTP, host: webserver URL, login/password) |
| B — Env vars (quick dev) | `AIRFLOW_API_BASE_URL`, `AIRFLOW_API_USER`, `AIRFLOW_API_PASSWORD` |

**Tags:** `type=demo`, `exec=kube`, `exec=compose`, `subtype=variables`

---

### 5 — print_a_variable
Fetches a known list of Variables via `Variable.get()` (Task SDK). Automatically masks values whose key contains `password`, `secret`, `token`, `key`, `passwd`, or `credential`.

**Tags:** `type=demo`, `exec=kube`, `exec=compose`, `subtype=variables`

---

### 6 — setup_variables_connections
Idempotently seeds Airflow Variables and Connections from a YAML definition. Checks existence before creating — never overwrites existing values, safe to re-run.

**YAML input sources (first match wins):**
1. DAG run conf: `{ "yaml_path": "/path/to/file.yaml" }` or `{ "yaml_inline": "<yaml>" }`
2. Airflow Variable: `setup_config_yaml_path`
3. Default path: `$AIRFLOW_HOME/config/setup_config.yaml`

**Tags:** `type=utility`, `exec=kube`, `exec=compose`, `subtype=bootstrap`, `intent=utility`

---

### 7 — validate_k8s_cluster_status
Validates the health of a Kubernetes cluster on a per-namespace basis and emits a JSON report as XCom and to the task log.

**Per-namespace checks:** pod phase counts, CrashLoopBackOff / ImagePullBackOff pods, deployments with unavailable replicas, unbound PVCs, NotReady nodes.

**Run config:**
```json
{ "namespaces": ["airflow", "default"], "in_cluster": true, "kube_config_path": null }
```

**Tags:** `type=utility`, `exec=kube`, `subtype=k8s-health`, `intent=utility`

---

### 8 — cleanup_k8s_jobs_pods
Deletes Kubernetes Jobs (and their Pods) that finished earlier than a configurable cutoff. Separate retention windows for successful vs. failed workloads. Supports `dry_run` mode.

**Defaults:** `success_older_than_hours=1`, `failure_older_than_hours=5`

**Tags:** `type=utility`, `exec=kube`, `subtype=k8s-cleanup`, `intent=utility`

---

### 9 — manage_pvc
Creates or deletes a Kubernetes PersistentVolumeClaim. Idempotent: create is skipped if the PVC already exists; delete is skipped if it does not.

**Defaults:** `action=create`, `name=airflow-default-pvc`, `namespace=airflow`, `storage=1Gi`

**Tags:** `type=utility`, `exec=kube`, `subtype=k8s-pvc`, `intent=utility`

---

### 10 — list_active_dags
Lists every active (unpaused) DAG and, for each one, prints its most recent run's `run_id`, `state`, `conf`, and duration. Uses REST API v2 with JWT auth.

**Tags:** `type=utility`, `exec=kube`, `exec=compose`, `subtype=dag-status`, `intent=utility`

---

### 11 — xcom_api_sdk_demo
Demonstrates both XCom paths in Airflow 3: `ti.xcom_push/pull()` (SDK) and `GET/POST /api/v2/…/xcomEntries/{key}` (REST API). Task flow: `sdk_push → sdk_pull → api_pull` and `api_push → api_verify_pull`.

**Tags:** `type=utility`, `exec=kube`, `exec=compose`, `subtype=xcom`, `intent=utility`

---

### 12 — dag_dependency_validator
Crawls all DAG definitions via REST API, builds an upstream/downstream dependency graph, detects cycles, flags missing upstream DAGs, and identifies orphaned DAGs. Useful for impact analysis before pausing or deleting a DAG.

**Tags:** `type=utility`, `exec=kube`, `exec=compose`, `subtype=monitoring`, `intent=operational`

---

### 13 — sla_breach_reporter
Queries DAG run history via REST API, flags runs that exceeded a configurable SLA threshold, correlates with the slowest tasks per run, and exports results as JSON or CSV.

**Tunable constants:** `_LOOKBACK_DAYS`, SLA threshold, output format.

**Tags:** `type=utility`, `exec=kube`, `exec=compose`, `subtype=monitoring`, `intent=operational`

---

### 14 — task_retry_analyzer
Identifies tasks with high retry rates, groups them by inferred failure category (sensor timeout, resource constraint, transient error, unknown), and surfaces tuning recommendations. Queries task instances with `try_number > 1` over a configurable lookback window.

**Tags:** `type=utility`, `exec=kube`, `exec=compose`, `subtype=monitoring`, `intent=operational`

---

### 15 — dag_version_auditor
Tracks DAG file changes by comparing current file hashes against a stored baseline (persisted as an Airflow Variable). Detects new, modified, and deleted DAG files. Optionally enriches output with `git log` metadata. Set `_UPDATE_BASELINE = True` on first run to establish the baseline.

**Tags:** `type=utility`, `exec=kube`, `exec=compose`, `subtype=monitoring`, `intent=operational`

---

## Shared utility

### `airflow_api_client.py`
Shared helper that exchanges credentials for a JWT and returns a pre-configured `requests.Session` for `/api/v2/` calls. Used by DAGs 4, 10, 11, 12, 13, and 14.

**Credential resolution order:**
1. Airflow Connection `airflow_api` (conn_type HTTP)
2. Env vars: `AIRFLOW_API_BASE_URL`, `AIRFLOW_API_USER`, `AIRFLOW_API_PASSWORD`

---

## Testing

### Install test dependencies

```bash
pip install -r requirements-test.txt
```

### Pre-commit hook

Layers 1 & 2 run automatically on every `git commit` via [pre-commit](https://pre-commit.com).

```bash
pre-commit install   # one-time setup per clone
```

After that, commits are blocked if any parse or unit test fails.

### Run manually

```bash
pytest                                 # all layers
pytest tests/test_1_dag_integrity.py  # parse only     — no Airflow needed (~15s)
pytest tests/test_2_unit_functions.py # unit only      — no Airflow needed (~1s)
pytest tests/test_3_task_level.py     # per-task CLI   — needs airflow CLI + SQLite
pytest tests/test_4_dag_runs.py       # full DAG runs  — needs airflow CLI + SQLite
```

### What each layer covers

| Layer | Tool | Needs Airflow running? | Catches |
|-------|------|----------------------|---------|
| 1 — Parse | `DagBag` | No | Syntax errors, missing imports, bad DAG config |
| 2 — Unit | `pytest` | No | Logic bugs in task helper functions |
| 3 — Task | `airflow tasks test` | CLI only (SQLite) | Task execution, XCom values, operator wiring |
| 4 — DAG | `airflow dags test` | CLI only (SQLite) | Full flow, dependency order, XCom propagation |

> DAGs requiring Kubernetes (`exec=kube`) or a live webserver are marked `@pytest.mark.skip` in layers 3 & 4.

---

## Requirements

- Apache Airflow 3.x
- `apache-airflow-providers-cncf-kubernetes` (for DAGs tagged `exec=kube`)
- A running Kubernetes cluster accessible from the Airflow workers (for DAGs tagged `exec=kube`)
- `requests` (for DAGs using the REST API client)

## Running locally (Docker Compose)

DAGs tagged `exec=compose` can run on a standard Airflow Docker Compose stack without Kubernetes.

```bash
# start Airflow
docker compose up -d

# unpause a DAG and trigger it
airflow dags unpause 5_print_a_variable
airflow dags trigger 5_print_a_variable
```

## Tag conventions

| Tag key | Values | Meaning |
|---------|--------|---------|
| `type` | `demo`, `utility` | Purpose of the DAG |
| `exec` | `kube`, `compose` | Required execution environment |
| `subtype` | `xcom`, `dag-status`, `variables`, `bootstrap`, `k8s-health`, `k8s-cleanup`, `k8s-pvc`, `monitoring` | Feature being demonstrated |
| `intent` | `utility`, `operational` | Operational intent |
