"""
Airflow 3 Assets — AssetAlias patterns (2 DAGs)
================================================
`AssetAlias` decouples producer DAGs from consumer DAGs.

Problem it solves:
  Without aliases, a consumer DAG must hard-code the producer's exact asset
  URI in its `schedule=` declaration.  If the producer changes the URI
  (e.g. switches storage backends), the consumer DAG must also be updated.

How AssetAlias works:
  1. The producer publishes events to a *named alias* instead of a concrete URI.
  2. Inside the task the producer calls
       outlet_events[alias].add(Asset("concrete://uri"), extra={...})
     to resolve which real Asset(s) the alias points to — at runtime.
  3. The consumer DAG schedules on the *alias name*, completely unaware of
     the real URIs.  Airflow fans out asset events to all resolved assets.

Two DAGs in this file:
  a3_alias_producer — writes via AssetAlias; resolves to concrete assets
  a3_alias_consumer — scheduled on the alias; reads events from real assets

Extra patterns shown:
  - One alias resolving to MULTIPLE concrete assets in a single event
  - Consumer reading both the alias-level and asset-level event metadata
  - Dynamic URI resolution (the target URI is determined at runtime from conf)
"""

from __future__ import annotations

from datetime import datetime

import pendulum
from airflow.sdk import DAG, Asset, task

from assets.asset_defs import (
    alias_bronze_ingestion,
    asset_customers_raw,
    asset_orders_raw,
)


# ===========================================================================
# DAG 1 — PRODUCER via AssetAlias
# ===========================================================================
with DAG(
    dag_id="a3_alias_producer",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["type=demo", "exec=compose", "exec=kube", "subtype=assets", "intent=demo"],
    doc_md="""
    ## a3_alias_producer
    Produces asset events via the `bronze-ingestion` alias.

    The alias resolves to both `asset_orders_raw` and `asset_customers_raw`
    in the `ingest_via_alias` task.  The consumer DAG does not need to know
    these concrete URIs — it only knows the alias name.

    Advanced: `ingest_dynamic_target` shows runtime URI resolution from
    DAG run conf so the same DAG can target different storage backends.
    """,
) as dag_producer:

    # ------------------------------------------------------------------
    # Pattern A — Alias resolves to multiple concrete assets.
    # One task, one alias outlet, but TWO concrete asset events are emitted.
    # ------------------------------------------------------------------
    @task(outlets=[alias_bronze_ingestion])
    def ingest_via_alias(*, outlet_events) -> dict:
        """
        Write two bronze datasets and publish both under one alias.
        Each .add() call creates one concrete asset event.
        """
        print(f"Publishing via alias: {alias_bronze_ingestion.name!r}")

        orders_row_count    = 42_000
        customers_row_count = 8_800

        # Resolve alias → asset_orders_raw
        outlet_events[alias_bronze_ingestion].add(
            asset_orders_raw,
            extra={
                "row_count":   orders_row_count,
                "source":      "orders-db",
                "format":      "parquet",
                "produced_by": "a3_alias_producer.ingest_via_alias",
            },
        )

        # Resolve alias → asset_customers_raw (second concrete asset)
        outlet_events[alias_bronze_ingestion].add(
            asset_customers_raw,
            extra={
                "row_count":   customers_row_count,
                "source":      "customers-db",
                "format":      "parquet",
                "produced_by": "a3_alias_producer.ingest_via_alias",
            },
        )

        print(
            f"Alias resolved to 2 assets: "
            f"orders={orders_row_count:,} rows, customers={customers_row_count:,} rows"
        )
        return {"orders": orders_row_count, "customers": customers_row_count}

    # ------------------------------------------------------------------
    # Pattern B — Dynamic target URI from DAG run conf.
    # Useful when the same producer DAG can write to dev / staging / prod
    # storage without needing separate DAG definitions.
    #
    # Trigger with conf:
    #   { "target_env": "staging",
    #     "target_uri": "s3://my-data-lake-staging/raw/events.parquet" }
    # ------------------------------------------------------------------
    @task(outlets=[alias_bronze_ingestion])
    def ingest_dynamic_target(*, outlet_events) -> dict:
        """Resolve alias target at runtime from DAG run conf."""
        from airflow.sdk import get_current_context

        ctx   = get_current_context()
        conf  = (ctx["dag_run"].conf or {}) if ctx.get("dag_run") else {}

        target_env = conf.get("target_env", "production")
        target_uri = conf.get(
            "target_uri",
            "s3://my-data-lake/raw/events.parquet",   # fallback default
        )
        row_count = conf.get("row_count", 5_000)

        print(f"Dynamic target: env={target_env!r}  uri={target_uri!r}")

        # Concrete asset is constructed at runtime — not known at parse time
        dynamic_asset = Asset(uri=target_uri, name=f"events_{target_env}")

        outlet_events[alias_bronze_ingestion].add(
            dynamic_asset,
            extra={
                "row_count":  row_count,
                "target_env": target_env,
                "target_uri": target_uri,
            },
        )
        return {"target_env": target_env, "uri": target_uri, "rows": row_count}

    ingest_via_alias()
    ingest_dynamic_target()


# ===========================================================================
# DAG 2 — CONSUMER scheduled on the alias
# ===========================================================================
with DAG(
    dag_id="a3_alias_consumer",
    # Schedule on the alias name — completely decoupled from concrete URIs.
    # Airflow triggers this DAG when any asset event is published under
    # the "bronze-ingestion" alias, regardless of its real URI.
    schedule=alias_bronze_ingestion,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["type=demo", "exec=compose", "exec=kube", "subtype=assets", "intent=demo"],
    doc_md="""
    ## a3_alias_consumer
    Scheduled on the `bronze-ingestion` alias.

    This DAG triggers whenever ANY asset is published under that alias — it
    does not need to know whether the source was orders, customers, or a
    dynamically-resolved URI.

    `inlet_events` on the task uses the concrete assets to read metadata;
    the alias is used only at the DAG-schedule level.
    """,
) as dag_consumer:

    @task(inlets=[asset_orders_raw, asset_customers_raw])
    def process_bronze_arrival(*, inlet_events) -> dict:
        """
        Runs whenever any bronze asset fires under the alias.
        Checks each concrete asset for new events and processes whichever arrived.
        """
        results = {}

        for asset in [asset_orders_raw, asset_customers_raw]:
            events = inlet_events[asset]
            if not events:
                continue

            latest = events[-1]
            meta   = latest.extra or {}
            print(
                f"[{asset.name}]  {len(events)} event(s)  "
                f"rows={meta.get('row_count', 'n/a')}  "
                f"source={meta.get('source', 'unknown')}  "
                f"ts={latest.timestamp}"
            )
            results[asset.name] = {
                "event_count": len(events),
                "row_count":   meta.get("row_count"),
                "source":      meta.get("source"),
                "produced_by": meta.get("produced_by"),
            }

        if not results:
            print("[WARN] No concrete asset events found — was this an alias-only trigger?")

        return results

    @task
    def validate_bronze_quality(arrivals: dict) -> dict:
        """Simple quality check: warn if any arriving dataset is smaller than expected."""
        thresholds = {
            "orders_raw":    10_000,
            "customers_raw": 1_000,
        }
        passed = {}
        for name, info in arrivals.items():
            row_count = info.get("row_count") or 0
            threshold = thresholds.get(name, 0)
            ok = row_count >= threshold
            passed[name] = ok
            status = "OK" if ok else "WARN"
            print(f"  [{status}] {name}: {row_count:,} rows (threshold={threshold:,})")

        return {"passed": passed, "all_ok": all(passed.values())}

    arrivals = process_bronze_arrival()
    validate_bronze_quality(arrivals)
