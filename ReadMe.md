# airflow3-dags

A collection of reference DAGs for **Apache Airflow 3**, covering XCom patterns, Kubernetes job execution, DAG introspection, variable access, and operational utilities via the Airflow 3 Task SDK.

## Repository layout

```
dags/
  kubernetes/
    k1_kubepod_operator.py            KubernetesPodOperator — 5 pod patterns
    k2_kubejob_advanced.py            KubernetesJobOperator — advanced Job patterns
    k3_dynamic_k8s_fan_out.py         Dynamic task mapping fan-out with K8s Jobs
    k4_task_kubernetes_decorator.py   @task.kubernetes — Python function in a K8s pod
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

---

## Kubernetes execution methods (`dags/kubernetes/`)

Four DAGs showing every major way to run Airflow workloads on Kubernetes.

### k1 — k1_kubepod_operator
Runs workloads as bare **Kubernetes Pods** via `KubernetesPodOperator`.  Airflow owns the full pod lifecycle; K8s does not provide Job-level retry.

Five patterns run in parallel:

| Task | Pattern |
|------|---------|
| `basic_pod` | Minimal alpine container — image, command, env vars |
| `env_from_config` | Import all keys from a ConfigMap + a single Secret key as env vars |
| `volume_mount_pod` | emptyDir volume — write and read within the same pod |
| `init_container_pod` | Init container seeds a shared emptyDir before the main container starts |
| `resource_profile_pod` | CPU/memory requests + nodeSelector + toleration for batch nodes |

**Tags:** `type=demo`, `exec=kube`, `subtype=k8s-pod`

---

### k2 — k2_kubejob_advanced
Advanced **Kubernetes Job** patterns via `KubernetesJobOperator`.  Creates a proper K8s `Job` resource with at-least-once guarantees, pod-level retry, and completion tracking.

Four patterns (run sequentially):

| Task | Pattern |
|------|---------|
| `ttl_backoff_job` | `ttlSecondsAfterFinished` + `backoffLimit` + `activeDeadlineSeconds` |
| `parallel_workers_job` | `completions=6 / parallelism=3` with Indexed completion mode (`JOB_COMPLETION_INDEX`) |
| `init_container_job` | Init container pre-fetches a CSV via shared emptyDir; main container processes it |
| `priority_annotated_job` | PriorityClass for scheduling precedence + Prometheus/Datadog pod annotations |

**Tags:** `type=demo`, `exec=kube`, `subtype=k8s-job`

---

### k3 — k3_dynamic_k8s_fan_out
**Dynamic task mapping** — generates a list of work items at runtime and spawns one `KubernetesJobOperator` per item with `.partial().expand()`.  No tasks are pre-defined at parse time.

```
generate_shards (@task)
        ↓
process_shard.expand(arguments=shard_args)   ← N K8s Jobs in parallel
        ↓
aggregate_results (@task)
```

Demonstrates `max_active_tis_per_dagrun` to cap concurrent K8s Jobs, `do_xcom_push=True` to collect per-shard results, and XCom list unpacking in the aggregator.

**Tags:** `type=demo`, `exec=kube`, `subtype=k8s-fan-out`

---

### k4 — k4_task_kubernetes_decorator
**`@task.kubernetes` decorator** — write a plain Python function and run it inside a K8s pod with no custom Dockerfile.  Airflow serialises the function, executes it in the pod, and pulls the return value back as XCom.

```
fetch_pipeline_config (@task — Airflow worker)
        ↓
run_feature_engineering (@task.kubernetes — K8s pod, 512Mi / 1 CPU)
        ↓
run_model_scoring       (@task.kubernetes — K8s pod, 512Mi / 2 CPU)
        ↓
notify_completion (@task — Airflow worker)
```

Each pod can have different resource budgets, node selectors, and env vars — useful for routing heavy ML workloads to GPU or compute-optimised nodes.

**Tags:** `type=demo`, `exec=kube`, `subtype=k8s-decorator`

---

## Airflow 3 Assets (`dags/assets/`)

Assets (formerly Datasets in Airflow 2.x) enable **data-aware scheduling**: a DAG
declares what data it produces (`outlets`) and what it consumes (`inlets`), and
Airflow automatically triggers downstream DAGs when their input assets are updated.

### Shared definitions — `asset_defs.py`
Central module defining all `Asset` and `AssetAlias` objects.  Both producer and
consumer DAGs import from here so the URI identity stays in one place.

| Symbol | URI | Tier |
|--------|-----|------|
| `asset_orders_raw` | `s3://my-data-lake/raw/orders.parquet` | bronze |
| `asset_customers_raw` | `s3://my-data-lake/raw/customers.parquet` | bronze |
| `asset_orders_clean` | `s3://my-data-lake/clean/orders.parquet` | silver |
| `asset_daily_report` | `s3://my-data-lake/reports/daily_summary.parquet` | gold |
| `alias_bronze_ingestion` | *(AssetAlias)* | — |

---

### a1 — `a1_asset_producers` — all production patterns
How a task declares that it produced data:

| Task | Pattern |
|------|---------|
| `ingest_orders_raw` | `@task(outlets=[asset])` — minimal, no metadata |
| `ingest_customers` | `BashOperator(outlets=[asset])` — any operator can produce assets |
| `backfill_all_bronze` | `outlets=[asset_a, asset_b]` — one task, multiple asset events |
| `clean_and_enrich` | `outlet_events[asset].extra = {...}` — attach rich metadata to the event |
| `build_daily_report` | `raise AirflowSkipException` — quality gate: suppresses the asset event |

**Tags:** `type=demo`, `exec=compose`, `exec=kube`, `subtype=assets`

---

### a2 — `a2_asset_consumers` — all scheduling patterns
Five DAGs, one per schedule variant:

| DAG | Schedule | Triggers when |
|-----|----------|---------------|
| `a2a_single_asset_schedule` | `schedule=asset_orders_clean` | that one asset updates |
| `a2b_and_all_assets` | `schedule=[asset_a, asset_b]` | ALL listed assets update |
| `a2c_or_any_asset` | `schedule=asset_a \| asset_b` | EITHER asset updates |
| `a2d_complex_asset_expr` | `schedule=(a & b) \| c` | boolean combination |
| `a2e_asset_or_time_schedule` | `AssetOrTimeSchedule(cron, assets)` | asset update OR cron, first wins |

All five tasks use `inlets=` to read the `extra=` metadata attached by the producer.

**Tags:** `type=demo`, `exec=compose`, `exec=kube`, `subtype=assets`

---

### a3 — `a3_asset_alias` — AssetAlias decoupling (2 DAGs)
`AssetAlias` lets producers publish to a *named alias* and consumers schedule on the
alias — completely decoupled from concrete URIs.

| DAG | Role |
|-----|------|
| `a3_alias_producer` | Resolves alias → multiple concrete assets at runtime; also shows dynamic URI from conf |
| `a3_alias_consumer` | `schedule=alias_bronze_ingestion` — fires on any asset published under the alias |

**Tags:** `type=demo`, `exec=compose`, `exec=kube`, `subtype=assets`

---

### a4 — `a4_asset_events_branching` — event metadata + decision making
Reads `inlet_events` metadata and routes execution via `@task.branch`:

```
inspect_incoming_event  (staleness SLA + multi-event best-pick)
         ↓
   route_by_quality  (@task.branch on quality_score)
         ↓
   ┌─ full_enrichment      (score ≥ 0.95 → gold → emits daily_report asset)
   ├─ light_cleanse        (score ≥ 0.80 → silver → no asset event)
   └─ quarantine_and_alert (score < 0.80 → alert → no asset event)
```

**Tags:** `type=demo`, `exec=compose`, `exec=kube`, `subtype=assets`

---

### a5 — `a5_asset_lifecycle_api` — full lifecycle via REST API
Manages assets through the Airflow v2 REST API:

| Task | API call | Purpose |
|------|----------|---------|
| `list_all_assets` | `GET /api/v2/assets` | Discover all assets, producers, consumers |
| `query_asset_events` | `GET /api/v2/assets/events` | Audit event history with metadata |
| `materialize_asset_event` | `POST /api/v2/assets/events` | External system signals data-ready |
| `archive_asset` | `DELETE /api/v2/assets/<id>` | Remove asset from metastore (data untouched) |

Supports `dry_run=true` (default) to preview without writes.

**Tags:** `type=utility`, `exec=compose`, `exec=kube`, `subtype=assets`

---

### Asset concept quick reference

| Concept | Code | Description |
|---------|------|-------------|
| Define an asset | `Asset(uri=..., name=..., extra={})` | Logical identifier; URI does not need to be reachable |
| Produce | `@task(outlets=[asset])` | Task emits event on success |
| Produce with metadata | `outlet_events[asset].extra = {...}` | Attach structured data to the event |
| Suppress event | `raise AirflowSkipException` | Task skipped = no event = no downstream trigger |
| Consume (schedule) | `schedule=asset` or `schedule=[a, b]` | Data-aware DAG trigger |
| OR schedule | `schedule=a \| b` | Either asset triggers |
| AND schedule | `schedule=a & b` or `schedule=[a, b]` | All assets must update |
| Time OR asset | `AssetOrTimeSchedule(timetable, assets)` | First condition wins |
| Read event data | `@task(inlets=[asset])` + `inlet_events[asset]` | Access extra= from producer |
| Alias (decouple) | `AssetAlias(name=...)` | Producer resolves URI at runtime |
| External trigger | `POST /api/v2/assets/events` | Signal from non-Airflow systems |
| Delete | `DELETE /api/v2/assets/<id>` | Remove from metastore; data unaffected |

---

## Kubernetes execution method comparison

| Method | K8s resource | Retry owner | Best for |
|--------|-------------|-------------|----------|
| `KubernetesPodOperator` | Pod | Airflow | Simple one-shot tasks, sidecar containers |
| `KubernetesJobOperator` | Job | Airflow + K8s | Batch jobs, parallel completions, audit trail |
| `.partial().expand()` + `KubernetesJobOperator` | Job × N | Airflow + K8s | Runtime fan-out over N shards/datasets |
| `@task.kubernetes` | Pod | Airflow | Python functions needing K8s isolation |
| `KubernetesExecutor` *(infra-level)* | Pod per task | Airflow | Every task runs in its own pod automatically |

> **KubernetesExecutor** is configured in `airflow.cfg` (`executor = KubernetesExecutor`), not in DAG code.  It makes every Airflow task spin up its own pod — no operator changes required.

---

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
