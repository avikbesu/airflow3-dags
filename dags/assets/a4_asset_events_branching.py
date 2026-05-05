"""
DAG: a4_asset_events_branching

Airflow 3 Assets — reading event metadata and decision making
=============================================================
This DAG shows how to read the extra= metadata that producer DAGs attach
to asset events, and how to route execution based on that metadata.

Upstream producer: a1_asset_producers
  (attaches quality_score, row_count, schema, etc. to asset_orders_clean)

Decision tree:
  1. inspect_incoming_event     — reads inlet_events; extracts quality metrics;
                                  enforces a 48-hour staleness SLA
  2. route_by_quality           — @task.branch; routes to one of three paths
  3a. full_enrichment           — quality_score >= 0.95 → gold tier;
                                  emits asset_daily_report event for downstream
  3b. light_cleanse             — quality_score in [0.80, 0.95) → silver tier;
                                  produces cleaned data, no report event
  3c. quarantine_and_alert      — quality_score < 0.80 → quarantine;
                                  no asset event, sends an alert

Additional patterns shown:
  - Multi-event accumulation: pick the best event when multiple fired
  - Staleness SLA: raise ValueError if the asset is older than 48 h
  - Selective asset production: only the gold-tier branch emits an event
  - @task.branch returning a task_id string based on asset metadata
"""

from __future__ import annotations

from datetime import datetime, timezone

import pendulum
from airflow.sdk import DAG, task

from assets.asset_defs import asset_daily_report, asset_orders_clean


with DAG(
    dag_id="a4_asset_events_branching",
    schedule=asset_orders_clean,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["type=demo", "exec=compose", "exec=kube", "subtype=assets", "intent=demo"],
    doc_md=__doc__,
) as dag:

    # ------------------------------------------------------------------
    # Step 1 — Read all accumulated asset events; pick the best one.
    #
    # inlet_events[asset] returns ALL events since the last DAG run.
    # If the producer ran 3 times while this consumer was paused, we get
    # 3 events and choose the one with the highest quality_score.
    #
    # Also enforces a staleness SLA: fail if the newest event is > 48 h old.
    # ------------------------------------------------------------------
    @task(inlets=[asset_orders_clean])
    def inspect_incoming_event(*, inlet_events) -> dict:
        events = inlet_events[asset_orders_clean]

        if not events:
            raise ValueError(
                f"No asset events found for {asset_orders_clean.name!r}. "
                "This task should only run when the asset is updated."
            )

        print(f"Received {len(events)} asset event(s) for {asset_orders_clean.name!r}")

        # Staleness SLA: reject data older than 48 hours
        now          = datetime.now(timezone.utc)
        latest_event = events[-1]
        age_hours    = (
            now - latest_event.timestamp.replace(tzinfo=timezone.utc)
        ).total_seconds() / 3600

        if age_hours > 48:
            raise ValueError(
                f"Asset event is {age_hours:.1f}h old — exceeds 48 h SLA. "
                "Investigate the upstream producer DAG."
            )

        # When multiple events arrived, pick the one with the best quality
        best = max(events, key=lambda ev: (ev.extra or {}).get("quality_score", 0.0))
        meta = best.extra or {}

        summary = {
            "total_events":       len(events),
            "quality_score":      meta.get("quality_score",      0.0),
            "row_count":          meta.get("row_count",          0),
            "duplicates_removed": meta.get("duplicates_removed", 0),
            "schema":             meta.get("schema",             []),
            "output_path":        meta.get("output_path",        asset_orders_clean.uri),
            "source_dag":         best.source_dag_id,
            "source_run":         best.source_run_id,
            "event_ts":           str(best.timestamp),
            "age_hours":          round(age_hours, 2),
        }

        print(f"Best event: quality={summary['quality_score']}  "
              f"rows={summary['row_count']:,}  age={age_hours:.1f}h")
        return summary

    # ------------------------------------------------------------------
    # Step 2 — Branch routing based on quality_score from the event.
    # ------------------------------------------------------------------
    @task.branch
    def route_by_quality(summary: dict) -> str:
        score     = summary.get("quality_score", 0.0)
        row_count = summary.get("row_count",     0)

        print(f"Routing: quality_score={score}  row_count={row_count:,}")

        if score >= 0.95 and row_count >= 10_000:
            print("→ GOLD branch: full_enrichment")
            return "full_enrichment"
        elif score >= 0.80:
            print("→ SILVER branch: light_cleanse")
            return "light_cleanse"
        else:
            print("→ QUARANTINE branch: quarantine_and_alert")
            return "quarantine_and_alert"

    # ------------------------------------------------------------------
    # Step 3a — Gold path: full enrichment.
    # This is the ONLY branch that emits the daily_report asset event,
    # so downstream report-consumer DAGs only trigger on high-quality data.
    # ------------------------------------------------------------------
    @task(outlets=[asset_daily_report])
    def full_enrichment(summary: dict, *, outlet_events) -> None:
        rows  = summary["row_count"]
        score = summary["quality_score"]
        print(f"[GOLD] Full enrichment: {rows:,} rows  quality={score}")
        print("  1. Joining with customers dimension table")
        print("  2. Applying revenue model features")
        print("  3. Writing enriched parquet to gold layer")

        report_rows = rows // 10
        outlet_events[asset_daily_report].extra = {
            "report_rows":  report_rows,
            "source_rows":  rows,
            "quality_tier": "gold",
            "quality_score": score,
            "generated_at": pendulum.now("UTC").isoformat(),
            "source_dag":   summary["source_dag"],
        }
        print(f"daily_report asset event emitted: {report_rows:,} summary rows")

    # ------------------------------------------------------------------
    # Step 3b — Silver path: light cleanse only.
    # Produces cleaned data at the silver tier but does NOT emit the
    # daily_report event — downstream report DAGs are not triggered.
    # ------------------------------------------------------------------
    @task
    def light_cleanse(summary: dict) -> None:
        rows  = summary["row_count"]
        score = summary["quality_score"]
        print(f"[SILVER] Light cleanse: {rows:,} rows  quality={score}")
        print("  Applying null-fill rules and type coercions.")
        print("  Skipping join enrichment — quality below gold threshold.")
        print("  daily_report asset event NOT emitted.")

    # ------------------------------------------------------------------
    # Step 3c — Quarantine path: isolate bad data and alert.
    # No asset events are produced; no downstream consumers are triggered.
    # ------------------------------------------------------------------
    @task
    def quarantine_and_alert(summary: dict) -> None:
        rows  = summary["row_count"]
        score = summary["quality_score"]
        print(f"[QUARANTINE] Bad data: {rows:,} rows  quality={score}")
        print("  Moving data to quarantine partition.")
        print("  Sending alert to #data-quality Slack channel.")
        print("  NO asset events emitted — all downstream DAGs are held.")
        # Production: call Slack webhook, PagerDuty, or email hook here.

    # ------------------------------------------------------------------
    # Wire the DAG
    # ------------------------------------------------------------------
    summary = inspect_incoming_event()
    choice  = route_by_quality(summary)

    gold       = full_enrichment(summary)
    silver     = light_cleanse(summary)
    quarantine = quarantine_and_alert(summary)

    choice >> [gold, silver, quarantine]
