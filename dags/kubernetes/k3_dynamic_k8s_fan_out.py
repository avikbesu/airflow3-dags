"""
DAG: k3_dynamic_k8s_fan_out

Execution method 3 of 4 — Dynamic task mapping with KubernetesJobOperator
==========================================================================
Airflow's dynamic task mapping (.partial().expand()) generates one task
instance per element of a runtime list.  Combined with KubernetesJobOperator,
this creates an elastic fan-out: N K8s Jobs launch in parallel without
pre-defining N tasks at DAG-parse time.

Use-case: process N datasets / regions / date-shards where N is known only
at runtime (e.g. driven by an upstream API, a config file, or prior XCom).

DAG flow:
  1. generate_shards   — @task; returns list[dict] of work items
  2. process_shard     — KubernetesJobOperator mapped over the list;
                         one K8s Job per shard, up to `max_active_tis_per_dagrun`
                         running concurrently (set on the DAG)
  3. aggregate_results — @task; collects all XCom results and prints summary

Key Airflow 3 concepts used:
  * KubernetesJobOperator.partial(...).expand(arguments=<XComArg>)
  * do_xcom_push=True  — each job writes /airflow/xcom/return.json
  * max_active_tis_per_dagrun on the mapped task limits concurrency

Requirements:
  - apache-airflow-providers-cncf-kubernetes >= 8.0
  - RBAC: ServiceAccount must have jobs create/get/watch/delete
"""

from __future__ import annotations

import json
import pendulum
from airflow.sdk import DAG, task
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator


with DAG(
    dag_id="k3_dynamic_k8s_fan_out",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["type=demo", "exec=kube", "subtype=k8s-fan-out", "intent=demo"],
    doc_md=__doc__,
) as dag:

    # ------------------------------------------------------------------
    # Step 1 — Generate shard list at runtime.
    # In production this list typically comes from:
    #   * DAG run conf  (pass via API trigger)
    #   * An upstream sensor that discovers new files / partitions
    #   * A database query listing unprocessed records
    # ------------------------------------------------------------------
    @task
    def generate_shards() -> list[list[str]]:
        """
        Returns a list of argument-lists: one per K8s Job that will be spawned.
        Each inner list is passed verbatim as `arguments` to one job container.
        """
        shards = [
            {"shard_id": "0", "region": "us-east-1",      "table": "orders",    "rows": 42_000},
            {"shard_id": "1", "region": "us-west-2",      "table": "orders",    "rows": 38_500},
            {"shard_id": "2", "region": "eu-west-1",      "table": "orders",    "rows": 29_100},
            {"shard_id": "3", "region": "ap-southeast-1", "table": "orders",    "rows": 15_200},
            {"shard_id": "4", "region": "us-east-1",      "table": "customers", "rows": 8_800},
        ]
        # Each shard's work parameters are encoded as a single shell command
        # string so the container can parse them without extra tooling.
        return [
            [
                "sh", "-c",
                f"""
                set -e
                SHARD_ID={s['shard_id']}
                REGION={s['region']}
                TABLE={s['table']}
                ROWS={s['rows']}

                echo "==> Shard $SHARD_ID | region=$REGION | table=$TABLE"
                echo "    Processing $ROWS rows..."

                # Simulate variable processing time proportional to row count
                SLEEP=$(( ROWS / 15000 + 1 ))
                sleep $SLEEP

                echo "    Done in ~${{SLEEP}}s"

                # Write XCom result — Airflow reads /airflow/xcom/return.json
                mkdir -p /airflow/xcom
                cat <<EOF > /airflow/xcom/return.json
{{
  "shard_id":   "$SHARD_ID",
  "region":     "$REGION",
  "table":      "$TABLE",
  "rows_processed": $ROWS,
  "status":     "success"
}}
EOF
                echo "==> XCom written."
                """
            ]
            for s in shards
        ]

    shard_args = generate_shards()

    # ------------------------------------------------------------------
    # Step 2 — Fan out: one KubernetesJobOperator per shard.
    #
    # .partial()  — fixed parameters shared across all mapped instances
    # .expand()   — parameter that varies per instance (arguments)
    #
    # max_active_tis_per_dagrun=3: at most 3 K8s Jobs run concurrently;
    #   the rest queue inside Airflow until a slot opens.  Set to None
    #   to let all jobs run simultaneously.
    # ------------------------------------------------------------------
    process_shard = KubernetesJobOperator.partial(
        task_id="process_shard",
        name="af-fan-out-shard",       # K8s appends a unique suffix
        namespace="airflow",
        image="alpine:3.19",
        # K8s-level safety nets
        backoff_limit=1,
        active_deadline_seconds=300,
        ttl_seconds_after_finished=120,
        container_resources={
            "requests": {"memory": "128Mi", "cpu": "100m"},
            "limits":   {"memory": "256Mi", "cpu": "250m"},
        },
        labels={"managed-by": "airflow", "dag-id": "k3_dynamic_k8s_fan_out"},
        env_vars={"JOB_SOURCE": "airflow-fan-out"},
        # Airflow reads the container's /airflow/xcom/return.json
        do_xcom_push=True,
        wait_until_job_complete=True,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        # Limit simultaneous K8s Jobs to avoid overwhelming the cluster
        max_active_tis_per_dagrun=3,
    ).expand(
        # Each element of shard_args becomes the `arguments` for one job
        arguments=shard_args,
    )

    # ------------------------------------------------------------------
    # Step 3 — Aggregate all shard results.
    #
    # When a mapped task pushes XCom, pulling it returns a list — one
    # element per mapped instance, in index order.
    # ------------------------------------------------------------------
    @task
    def aggregate_results(shard_results: list) -> dict:
        print("=" * 60)
        print(" FAN-OUT AGGREGATION RESULTS")
        print("=" * 60)

        total_rows = 0
        shards_ok  = 0
        shards_fail = 0

        for raw in shard_results:
            # KubernetesJobOperator wraps pod XCom in a list for multi-pod jobs
            result = raw[0] if isinstance(raw, list) else raw
            if result is None:
                print("  [WARN] One shard returned no XCom (job may have skipped write)")
                shards_fail += 1
                continue

            if isinstance(result, str):
                result = json.loads(result)

            status = result.get("status", "unknown")
            rows   = result.get("rows_processed", 0)
            print(
                f"  shard={result.get('shard_id')}  "
                f"region={result.get('region'):<15}  "
                f"table={result.get('table'):<10}  "
                f"rows={rows:>7,}  status={status}"
            )
            if status == "success":
                total_rows += rows
                shards_ok  += 1
            else:
                shards_fail += 1

        summary = {
            "total_shards":    len(shard_results),
            "shards_ok":       shards_ok,
            "shards_failed":   shards_fail,
            "total_rows":      total_rows,
        }
        print("-" * 60)
        print(f"  Total rows processed : {total_rows:,}")
        print(f"  Shards OK / Failed   : {shards_ok} / {shards_fail}")
        return summary

    aggregate_results(process_shard.output)
