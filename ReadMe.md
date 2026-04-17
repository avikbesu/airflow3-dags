# airflow3-dags

A collection of reference DAGs for **Apache Airflow 3**, covering XCom patterns, Kubernetes job execution, DAG introspection, and variable access via the Airflow 3 Task SDK.

## Repository layout

```
dags/
  1_push_xcom_from_kube_pod_op.py   KubernetesJobOperator XCom demo
  2_log_other_dag_status.py         Monitor another DAG's run state
  3_xcom_multi_operator_demo.py     XCom across BashOperator + @task
  4_print_all_variables.py          List all Variables via REST API v2
  5_print_a_variable.py             Fetch specific Variables via Task SDK
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

## Testing

### Test layout

```
tests/
  conftest.py                  Session-scoped Airflow home + SQLite DB setup
  test_1_dag_integrity.py      Layer 1 — DagBag parse/import tests
  test_2_unit_functions.py     Layer 2 — Unit tests for pure task functions
  test_3_task_level.py         Layer 3 — airflow tasks test (per-task CLI)
  test_4_dag_runs.py           Layer 4 — airflow dags test (full DAG run)
```

### Install test dependencies

```bash
pip install -r requirements-test.txt
```

### Run all tests

```bash
pytest
```

### Run by layer

```bash
pytest tests/test_1_dag_integrity.py   # parse only — no Airflow running needed
pytest tests/test_2_unit_functions.py  # pure Python — no Airflow running needed
pytest tests/test_3_task_level.py      # needs airflow CLI + DB
pytest tests/test_4_dag_runs.py        # needs airflow CLI + DB
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
- `apache-airflow-providers-cncf-kubernetes` (for DAG 1)
- A running Kubernetes cluster accessible from the Airflow workers (for DAGs tagged `exec=kube`)

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
| `type` | `demo` | Purpose of the DAG |
| `exec` | `kube`, `compose` | Required execution environment |
| `subtype` | `xcom`, `dag-status`, `variables` | Feature being demonstrated |
| `intent` | `utility` | Operational intent |
