"""
DAG: a1_asset_producers

Airflow 3 Assets — all ways to PRODUCE (create/update) an Asset
================================================================
An "Asset event" is recorded in Airflow's metastore each time a task that
declares `outlets=[asset]` completes successfully.  Downstream DAGs that
are scheduled on that Asset are then queued automatically.

Five production patterns in this DAG:

  1. basic_outlet          — @task with outlets=[Asset]; minimal, no metadata
  2. operator_outlet       — BashOperator with outlets= (any operator works)
  3. multi_asset_outlet    — one @task emits events for MULTIPLE assets at once
  4. metadata_outlet       — @task attaches structured extra= to the event
                             via the outlet_events kwarg; consumers can read it
  5. conditional_outlet    — asset event is SUPPRESSED by raising
                             AirflowSkipException when a quality gate fails;
                             Airflow only emits the event on task success

Pipeline shape:
  ingest_orders_raw  ──┐
                        ├──► clean_and_enrich ──► build_daily_report
  ingest_customers   ──┘          (pattern 4)        (pattern 5)
       (pattern 2)
  backfill_all  (pattern 3) — standalone utility branch

Requirements:
  - apache-airflow >= 3.0
  - pip install apache-airflow  (Assets are built-in; no extra provider needed)
"""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG, task
from airflow.operators.bash import BashOperator

from assets.asset_defs import (
    asset_customers_raw,
    asset_daily_report,
    asset_orders_clean,
    asset_orders_raw,
)


with DAG(
    dag_id="a1_asset_producers",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["type=demo", "exec=compose", "exec=kube", "subtype=assets", "intent=demo"],
    doc_md=__doc__,
) as dag:

    # ------------------------------------------------------------------
    # Pattern 1 — @task with a single outlet (minimal form).
    # Airflow marks `asset_orders_raw` as updated after this task exits.
    # No metadata is attached; downstream consumers get an empty `.extra`.
    # ------------------------------------------------------------------
    @task(outlets=[asset_orders_raw])
    def ingest_orders_raw() -> int:
        """Simulate landing raw orders into the data lake."""
        import time
        print(f"Ingesting raw orders → {asset_orders_raw.uri}")
        time.sleep(1)
        row_count = 42_000
        print(f"Written {row_count:,} rows.")
        return row_count

    # ------------------------------------------------------------------
    # Pattern 2 — outlets= on a classic Operator.
    # Any Airflow operator that accepts `outlets` can declare asset
    # production.  The event fires when the operator exits without error.
    # ------------------------------------------------------------------
    ingest_customers = BashOperator(
        task_id="ingest_customers",
        bash_command=(
            "echo 'Ingesting customers...'; "
            "sleep 1; "
            "echo 'Written 8800 rows → s3://my-data-lake/raw/customers.parquet'"
        ),
        outlets=[asset_customers_raw],
    )

    # ------------------------------------------------------------------
    # Pattern 3 — One task emits events for MULTIPLE assets.
    # Both asset_orders_raw AND asset_customers_raw are marked updated
    # when this task succeeds.  Use for emergency backfills or bootstrap
    # jobs that refresh several datasets in a single pass.
    # ------------------------------------------------------------------
    @task(outlets=[asset_orders_raw, asset_customers_raw])
    def backfill_all_bronze() -> dict:
        """Backfill: rewrite both raw assets in one task."""
        print("Backfill mode — rewriting orders and customers simultaneously.")
        return {"orders": 42_000, "customers": 8_800}

    # ------------------------------------------------------------------
    # Pattern 4 — Attach structured metadata to the asset event.
    #
    # `outlet_events` is injected by Airflow when `outlets` is declared.
    # Setting outlet_events[asset].extra = {...} stores the dict in the
    # AssetEvent row.  Downstream consumers read it via inlet_events.
    #
    # This is how you pass lineage, quality scores, schema info, and
    # row counts from producer to consumer without a separate XCom chain.
    # ------------------------------------------------------------------
    @task(outlets=[asset_orders_clean])
    def clean_and_enrich(raw_count: int, *, outlet_events) -> int:
        """Deduplicate raw orders and attach quality metadata to the event."""
        print(f"Cleaning {raw_count:,} raw rows...")
        duplicates   = int(raw_count * 0.03)
        clean_count  = raw_count - duplicates
        quality_score = round(1 - duplicates / raw_count, 4)

        # Metadata stored in the AssetEvent; readable in consuming tasks
        outlet_events[asset_orders_clean].extra = {
            "row_count":          clean_count,
            "source_row_count":   raw_count,
            "duplicates_removed": duplicates,
            "quality_score":      quality_score,
            "schema":             ["order_id", "customer_id", "amount", "ts", "status"],
            "output_path":        asset_orders_clean.uri,
            "produced_by":        "a1_asset_producers.clean_and_enrich",
        }
        print(f"Wrote {clean_count:,} clean rows.  quality_score={quality_score}")
        return clean_count

    # ------------------------------------------------------------------
    # Pattern 5 — Conditional asset production (quality gate).
    #
    # Raising AirflowSkipException marks the task as SKIPPED — Airflow
    # does NOT emit an asset event for skipped tasks.  This prevents
    # downstream DAGs from triggering on bad data.
    #
    # Use case: a nightly report should only be "ready" if at least
    # MIN_ROWS rows were successfully cleaned.
    # ------------------------------------------------------------------
    @task(outlets=[asset_daily_report])
    def build_daily_report(clean_count: int, *, outlet_events) -> int:
        """Build the daily report — suppress asset event if data is too sparse."""
        from airflow.exceptions import AirflowSkipException

        MIN_ROWS = 1_000

        if clean_count < MIN_ROWS:
            # Skipping = no asset event → downstream DAGs are NOT triggered
            raise AirflowSkipException(
                f"Only {clean_count} rows available; minimum is {MIN_ROWS}. "
                "Skipping report build — asset event suppressed."
            )

        report_rows = clean_count // 10
        print(f"Building daily report: {clean_count:,} detail rows → {report_rows:,} summary rows")

        outlet_events[asset_daily_report].extra = {
            "report_rows":   report_rows,
            "source_rows":   clean_count,
            "generated_at":  pendulum.now("UTC").isoformat(),
            "format":        "parquet",
            "output_path":   asset_daily_report.uri,
        }
        print(f"Report written → {asset_daily_report.uri}")
        return report_rows

    # ------------------------------------------------------------------
    # Wire the main pipeline
    # ------------------------------------------------------------------
    raw_count   = ingest_orders_raw()
    clean_count = clean_and_enrich(raw_count)
    build_daily_report(clean_count)

    # ingest_customers and backfill_all_bronze run independently
    # (no dependency on the orders pipeline in this DAG)
