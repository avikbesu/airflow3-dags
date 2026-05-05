"""
DAG: a5_asset_lifecycle_api

Airflow 3 Assets — lifecycle management via REST API v2
=======================================================
Assets live in Airflow's metastore and are managed through the REST API.
This DAG demonstrates every lifecycle operation:

  1. list_all_assets         — GET  /api/v2/assets
                               Discover all registered assets: URIs, names,
                               producing tasks, consuming DAGs.

  2. query_asset_events      — GET  /api/v2/assets/events?asset_id=<id>
                               Retrieve the event history for a specific
                               asset: who produced it, when, and what extra
                               metadata was attached.

  3. materialize_asset_event — POST /api/v2/assets/events
                               Manually create an asset event from OUTSIDE
                               a DAG task — useful for external systems
                               (Spark jobs, Flink, dbt, shell scripts) that
                               cannot run as Airflow operators but need to
                               signal that data is ready.

  4. archive_asset           — DELETE /api/v2/assets/<asset_id>
                               Remove an asset from Airflow's tracking.
                               Does NOT delete the underlying data; only
                               removes the asset record and its event history
                               from the metastore.  Dependent DAG schedules
                               that reference the asset URI will stop firing.

Run config (all optional):
  {
    "asset_uri":      "s3://my-data-lake/raw/orders.parquet",
    "asset_name":     "orders_raw",
    "max_events":     20,
    "dry_run":        true,   // true = show what would happen, no writes
    "external_extra": {"source": "spark-job", "rows": 50000}
  }

Auth setup (pick one):
  Option A — Airflow Connection (recommended):
    conn_id  : airflow_api
    conn_type: HTTP
    host     : http://localhost:8080
    login    : <admin user>
    password : <admin password>
  Option B — env vars:
    AIRFLOW_API_BASE_URL / AIRFLOW_API_USER / AIRFLOW_API_PASSWORD
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, get_current_context, task

from utility.airflow_api_client import get_session


@dag(
    dag_id="a5_asset_lifecycle_api",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["type=utility", "exec=compose", "exec=kube", "subtype=assets", "intent=utility"],
    doc_md=__doc__,
    params={
        "asset_uri":      "s3://my-data-lake/raw/orders.parquet",
        "asset_name":     "orders_raw",
        "max_events":     20,
        "dry_run":        True,
        "external_extra": {"source": "external-system", "rows": 5_000},
    },
)
def asset_lifecycle_api():

    @task
    def resolve_config() -> dict:
        ctx    = get_current_context()
        params = ctx.get("params") or {}
        conf   = (ctx["dag_run"].conf or {}) if ctx.get("dag_run") else {}
        merged = {**params, **conf}
        return {
            "asset_uri":      merged.get("asset_uri",      "s3://my-data-lake/raw/orders.parquet"),
            "asset_name":     merged.get("asset_name",     "orders_raw"),
            "max_events":     int(merged.get("max_events", 20)),
            "dry_run":        bool(merged.get("dry_run",   True)),
            "external_extra": merged.get("external_extra", {"source": "external-system"}),
        }

    # ------------------------------------------------------------------
    # Operation 1 — List all registered assets.
    #
    # Returns every asset Airflow knows about: its URI, name, group,
    # which tasks produce it, and which DAGs consume it.
    # Use this to audit your data lineage graph programmatically.
    # ------------------------------------------------------------------
    @task
    def list_all_assets(cfg: dict) -> list:
        base_url, session = get_session()
        resp = session.get(f"{base_url}/api/v2/assets", params={"limit": 100}, timeout=30)
        resp.raise_for_status()
        body   = resp.json()
        assets = body.get("assets", [])

        print("=" * 70)
        print(f" REGISTERED ASSETS ({len(assets)} total)")
        print("=" * 70)
        for a in assets:
            producers = [
                f"{t['dag_id']}.{t['task_id']}"
                for t in (a.get("producing_tasks") or [])
            ]
            consumers = [d["dag_id"] for d in (a.get("consuming_dags") or [])]
            print(
                f"  [{a.get('asset_id')}] {a.get('name') or a.get('uri')}"
            )
            print(f"      uri       : {a.get('uri')}")
            print(f"      producers : {producers or '(none)'}")
            print(f"      consumers : {consumers or '(none)'}")
            print(f"      created   : {a.get('created_at')}")

        return [
            {
                "asset_id": a.get("asset_id"),
                "name":     a.get("name"),
                "uri":      a.get("uri"),
            }
            for a in assets
        ]

    # ------------------------------------------------------------------
    # Operation 2 — Query event history for a specific asset.
    #
    # Each event records: which DAG run produced it, when, and any
    # extra= metadata the task attached.  Useful for lineage auditing,
    # debugging missed triggers, and SLA reporting.
    # ------------------------------------------------------------------
    @task
    def query_asset_events(cfg: dict, all_assets: list) -> list:
        target_uri = cfg["asset_uri"]
        max_events = cfg["max_events"]

        # Resolve asset_id from the catalog list (avoids URI-encoding issues)
        asset_id = next(
            (a["asset_id"] for a in all_assets if a.get("uri") == target_uri),
            None,
        )

        if not asset_id:
            print(f"[WARN] Asset with URI {target_uri!r} not found in catalog.")
            print("       Has the producer DAG run at least once?")
            return []

        base_url, session = get_session()
        resp = session.get(
            f"{base_url}/api/v2/assets/events",
            params={"asset_id": asset_id, "limit": max_events, "order_by": "-timestamp"},
            timeout=30,
        )
        resp.raise_for_status()
        events = resp.json().get("asset_events", [])

        print("=" * 70)
        print(f" ASSET EVENT HISTORY — {cfg['asset_name']!r} (last {max_events})")
        print("=" * 70)

        if not events:
            print("  No events recorded yet.  Run a1_asset_producers first.")
            return []

        for ev in events:
            extra = ev.get("extra") or {}
            print(
                f"  [{ev.get('id')}]  "
                f"dag={ev.get('source_dag_id')}  "
                f"run={ev.get('source_run_id')}  "
                f"ts={ev.get('timestamp')}"
            )
            if extra:
                for k, v in extra.items():
                    print(f"      {k}: {v}")

        return events

    # ------------------------------------------------------------------
    # Operation 3 — Manually create an asset event (external trigger).
    #
    # POST /api/v2/assets/events creates an event without running a DAG
    # task.  This is how external pipelines (Spark, dbt, shell scripts,
    # ML training runs) signal to Airflow that data is ready.
    #
    # Downstream DAGs scheduled on the asset URI will be queued just as
    # if an Airflow task had produced the event.
    # ------------------------------------------------------------------
    @task
    def materialize_asset_event(cfg: dict) -> dict:
        dry_run        = cfg["dry_run"]
        asset_uri      = cfg["asset_uri"]
        external_extra = cfg["external_extra"]

        payload = {
            "asset_uri": asset_uri,
            "extra":     {
                **external_extra,
                "trigger_source": "a5_asset_lifecycle_api.materialize_asset_event",
                "triggered_at":   datetime.utcnow().isoformat() + "Z",
            },
        }

        if dry_run:
            print("[DRY RUN] Would POST to /api/v2/assets/events:")
            import json
            print(json.dumps(payload, indent=2))
            print("Set dry_run=false in DAG run conf to actually create the event.")
            return {"dry_run": True, "payload": payload}

        base_url, session = get_session()
        resp = session.post(
            f"{base_url}/api/v2/assets/events",
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        created = resp.json()

        print(f"Asset event created:")
        print(f"  event_id  : {created.get('id')}")
        print(f"  asset_uri : {created.get('asset_uri')}")
        print(f"  timestamp : {created.get('timestamp')}")
        print(f"  extra     : {created.get('extra')}")
        print("Downstream DAGs scheduled on this URI are now queued.")

        return {"dry_run": False, "event": created}

    # ------------------------------------------------------------------
    # Operation 4 — Archive (delete) an asset from Airflow's metastore.
    #
    # DELETE /api/v2/assets/<asset_id>
    #
    # Effects:
    #   - The asset record is removed from the metastore.
    #   - All associated event history is deleted.
    #   - DAGs whose schedule= references this URI will stop triggering.
    #   - The UNDERLYING DATA is NOT deleted (Airflow only manages metadata).
    #
    # Use-case: decommissioning a data source, renaming an asset URI,
    # or cleaning up test assets from CI runs.
    # ------------------------------------------------------------------
    @task
    def archive_asset(cfg: dict, all_assets: list) -> dict:
        target_uri = cfg["asset_uri"]
        dry_run    = cfg["dry_run"]

        asset_record = next(
            (a for a in all_assets if a.get("uri") == target_uri),
            None,
        )

        if not asset_record:
            print(f"[SKIP] Asset with URI {target_uri!r} not found in catalog. Nothing to archive.")
            return {"skipped": True, "reason": "asset not found"}

        asset_id   = asset_record["asset_id"]
        asset_name = asset_record.get("name") or target_uri

        if dry_run:
            print(f"[DRY RUN] Would DELETE asset: id={asset_id}  name={asset_name!r}")
            print("          All event history would be removed from the metastore.")
            print("          The underlying data at the URI is NOT affected.")
            print("Set dry_run=false in DAG run conf to actually delete.")
            return {"dry_run": True, "asset_id": asset_id, "asset_name": asset_name}

        base_url, session = get_session()
        resp = session.delete(
            f"{base_url}/api/v2/assets/{asset_id}",
            timeout=30,
        )
        if resp.status_code == 404:
            print(f"[SKIP] Asset {asset_id} already deleted or not found.")
            return {"skipped": True, "reason": "404 not found"}

        resp.raise_for_status()

        print(f"Asset archived successfully:")
        print(f"  asset_id  : {asset_id}")
        print(f"  name      : {asset_name}")
        print(f"  uri       : {target_uri}")
        print("Event history removed.  Downstream DAG schedules on this URI are inactive.")

        return {"deleted": True, "asset_id": asset_id, "asset_name": asset_name}

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    @task
    def print_summary(
        asset_list:    list,
        event_history: list,
        materialized:  dict,
        archived:      dict,
    ) -> None:
        print("=" * 70)
        print(" ASSET LIFECYCLE SUMMARY")
        print("=" * 70)
        print(f"  Assets in catalog   : {len(asset_list)}")
        print(f"  Event history rows  : {len(event_history)}")
        print(f"  Manual event        : {'dry-run' if materialized.get('dry_run') else 'created'}")
        print(f"  Archive operation   : {'dry-run' if archived.get('dry_run') else ('skipped' if archived.get('skipped') else 'deleted')}")

    # ------------------------------------------------------------------
    # Wire
    # ------------------------------------------------------------------
    cfg           = resolve_config()
    asset_list    = list_all_assets(cfg)
    event_history = query_asset_events(cfg, asset_list)
    materialized  = materialize_asset_event(cfg)
    archived      = archive_asset(cfg, asset_list)
    print_summary(asset_list, event_history, materialized, archived)


asset_lifecycle_api()
