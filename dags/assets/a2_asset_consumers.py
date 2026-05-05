"""
Airflow 3 Assets — all scheduling / consumption patterns
=========================================================
This file defines FIVE separate DAGs, each showing a different way a DAG
can be scheduled based on Asset events.  Read them top to bottom as a
reference; only load the pattern(s) relevant to your use case.

Pattern overview:
  a2a_single_asset_schedule   — trigger when ONE specific asset updates
  a2b_and_all_assets          — trigger when ALL listed assets update (AND)
  a2c_or_any_asset            — trigger when ANY listed asset updates (OR)
  a2d_complex_asset_expr      — boolean combination of & and | operators
  a2e_asset_or_time_schedule  — trigger on a cron cadence OR on asset update

All five DAGs read the incoming asset event metadata via `inlet_events`
to show how producer-supplied extra= data flows through to the consumer.

Requirements:
  - apache-airflow >= 3.0
  - For AssetOrTimeSchedule: from airflow.timetables.assets import AssetOrTimeSchedule
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, dag, task
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

from assets.asset_defs import (
    asset_customers_raw,
    asset_daily_report,
    asset_orders_clean,
    asset_orders_raw,
)

# ===========================================================================
# Pattern 1 — Single asset schedule
#
# The simplest form: pass one Asset to `schedule`.
# This DAG is queued whenever `a1_asset_producers` successfully runs
# the task that declares `outlets=[asset_orders_clean]`.
# ===========================================================================
with DAG(
    dag_id="a2a_single_asset_schedule",
    schedule=asset_orders_clean,        # ← one Asset, no list needed
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["type=demo", "exec=compose", "exec=kube", "subtype=assets", "intent=demo"],
    doc_md="""
    ## a2a_single_asset_schedule
    Triggered automatically whenever `asset_orders_clean` is updated.

    Demonstrates reading `inlet_events` to access the extra metadata
    the producer attached to the asset event (row_count, quality_score, etc.).
    """,
) as dag_single:

    @task(inlets=[asset_orders_clean])
    def process_clean_orders(*, inlet_events) -> dict:
        """
        Runs when asset_orders_clean is updated.
        inlet_events[asset] holds the list of AssetEvent objects that
        triggered this DAG run — one per update since the last DAG run.
        """
        events = inlet_events[asset_orders_clean]
        print(f"Triggered by {len(events)} asset event(s) on {asset_orders_clean.name!r}")

        summary = []
        for ev in events:
            meta = ev.extra or {}
            print(
                f"  event  dag={ev.source_dag_id}  run={ev.source_run_id}  "
                f"ts={ev.timestamp}  rows={meta.get('row_count', 'n/a')}  "
                f"quality={meta.get('quality_score', 'n/a')}"
            )
            summary.append({
                "source_dag":    ev.source_dag_id,
                "source_run":    ev.source_run_id,
                "row_count":     meta.get("row_count"),
                "quality_score": meta.get("quality_score"),
            })

        return {"events_consumed": len(events), "summary": summary}

    process_clean_orders()


# ===========================================================================
# Pattern 2 — AND schedule: ALL listed assets must update
#
# Pass a list to `schedule`.  Airflow queues the DAG only after EVERY
# asset in the list has received at least one new event since the last
# DAG run.  The DAG acts as a natural join / barrier.
# ===========================================================================
with DAG(
    dag_id="a2b_and_all_assets",
    schedule=[asset_orders_clean, asset_customers_raw],   # AND — both required
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["type=demo", "exec=compose", "exec=kube", "subtype=assets", "intent=demo"],
    doc_md="""
    ## a2b_and_all_assets
    Triggered only when BOTH `asset_orders_clean` AND `asset_customers_raw`
    have been updated.  Use this to build a joined model that requires all
    input datasets to be fresh before it runs.
    """,
) as dag_and:

    @task(inlets=[asset_orders_clean, asset_customers_raw])
    def join_orders_and_customers(*, inlet_events) -> dict:
        """Runs only after both source assets are fresh."""
        order_events    = inlet_events[asset_orders_clean]
        customer_events = inlet_events[asset_customers_raw]

        print(f"orders events   : {len(order_events)}")
        print(f"customers events: {len(customer_events)}")

        order_rows    = (order_events[-1].extra or {}).get("row_count",  0) if order_events    else 0
        customer_rows = sum(1 for _ in customer_events)   # event count as proxy

        print(f"Building joined model: {order_rows:,} orders × customers dataset")
        return {"order_rows": order_rows, "customer_event_count": customer_rows}

    join_orders_and_customers()


# ===========================================================================
# Pattern 3 — OR schedule: ANY asset in the expression triggers the DAG
#
# Use the `|` operator on Asset objects to build an OR condition.
# The DAG fires when EITHER asset updates — it does not wait for both.
# ===========================================================================
with DAG(
    dag_id="a2c_or_any_asset",
    schedule=asset_orders_raw | asset_customers_raw,     # OR — either triggers
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["type=demo", "exec=compose", "exec=kube", "subtype=assets", "intent=demo"],
    doc_md="""
    ## a2c_or_any_asset
    Triggered when EITHER `asset_orders_raw` OR `asset_customers_raw` updates.
    Use for lightweight monitoring / alerting DAGs that should react to any
    change in a set of related assets.
    """,
) as dag_or:

    @task(inlets=[asset_orders_raw, asset_customers_raw])
    def react_to_any_bronze_update(*, inlet_events) -> dict:
        """Runs on the first update of either bronze asset."""
        triggered_assets = []
        for asset in [asset_orders_raw, asset_customers_raw]:
            events = inlet_events[asset]
            if events:
                triggered_assets.append({
                    "asset":      asset.name,
                    "event_count": len(events),
                    "latest_ts":  str(events[-1].timestamp),
                })

        print(f"Triggered by updates to: {[a['asset'] for a in triggered_assets]}")
        for info in triggered_assets:
            print(f"  {info['asset']}: {info['event_count']} new event(s)")

        return {"triggered_by": triggered_assets}

    react_to_any_bronze_update()


# ===========================================================================
# Pattern 4 — Complex boolean expression: mix & and |
#
# Combine multiple assets with & (AND) and | (OR) to express precise
# readiness conditions for complex pipelines.
#
# Expression: (orders_clean AND customers_raw) OR daily_report
# Meaning:    run if either
#               (a) both cleaned orders and raw customers are fresh, OR
#               (b) the daily report is available (fallback path)
# ===========================================================================
with DAG(
    dag_id="a2d_complex_asset_expr",
    schedule=(asset_orders_clean & asset_customers_raw) | asset_daily_report,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["type=demo", "exec=compose", "exec=kube", "subtype=assets", "intent=demo"],
    doc_md="""
    ## a2d_complex_asset_expr
    Schedule expression:  `(orders_clean AND customers_raw) OR daily_report`

    Supports pipelines with primary and fallback data paths.
    """,
) as dag_complex:

    @task(inlets=[asset_orders_clean, asset_customers_raw, asset_daily_report])
    def run_with_best_available_data(*, inlet_events) -> dict:
        """Choose execution path based on which assets are available."""
        has_orders    = bool(inlet_events[asset_orders_clean])
        has_customers = bool(inlet_events[asset_customers_raw])
        has_report    = bool(inlet_events[asset_daily_report])

        if has_orders and has_customers:
            path = "full-join"
            print("Path: FULL JOIN — using clean orders + fresh customers")
        elif has_report:
            path = "report-fallback"
            print("Path: REPORT FALLBACK — using pre-built daily report")
        else:
            path = "unknown"
            print("Path: UNKNOWN — check asset event routing")

        return {
            "execution_path": path,
            "has_orders":     has_orders,
            "has_customers":  has_customers,
            "has_report":     has_report,
        }

    run_with_best_available_data()


# ===========================================================================
# Pattern 5 — AssetOrTimeSchedule: asset update OR cron — whichever comes first
#
# Combines a time-based timetable with an asset trigger.  The DAG fires
# on the FIRST condition that becomes true:
#   • `asset_orders_clean` is updated  (data-driven, reactive)
#   • 06:00 UTC every day              (time-driven, guaranteed freshness)
#
# Use when: you want near-real-time processing when data arrives, but
# also need a hard daily SLA even if the upstream producer is delayed.
# ===========================================================================
with DAG(
    dag_id="a2e_asset_or_time_schedule",
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 6 * * *", timezone="UTC"),
        assets=[asset_orders_clean],
    ),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["type=demo", "exec=compose", "exec=kube", "subtype=assets", "intent=demo"],
    doc_md="""
    ## a2e_asset_or_time_schedule
    Fires when `asset_orders_clean` is updated **or** at 06:00 UTC daily,
    whichever happens first.  `inlet_events` may be empty when the cron
    fires without an asset event — the task handles both cases.
    """,
) as dag_hybrid:

    @task(inlets=[asset_orders_clean])
    def run_hybrid_trigger(*, inlet_events) -> dict:
        """Handles both asset-triggered and time-triggered execution."""
        events = inlet_events[asset_orders_clean]

        if events:
            trigger_type = "asset"
            latest_extra = events[-1].extra or {}
            row_count    = latest_extra.get("row_count", "unknown")
            print(f"Triggered by asset update — {len(events)} event(s), rows={row_count}")
        else:
            trigger_type = "cron"
            row_count    = "unknown"
            print("Triggered by cron schedule — no new asset events since last run")
            print("Running with whatever data is currently in the lake.")

        return {
            "trigger_type": trigger_type,
            "event_count":  len(events),
            "row_count":    row_count,
        }

    run_hybrid_trigger()
