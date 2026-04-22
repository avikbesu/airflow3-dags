"""
DAG: 18_xcom_pruner

Deletes XCom entries older than a configurable retention window directly from
the Airflow metastore.  DAGs that push large serialised objects (DataFrames,
model artefacts, API payloads) can grow the xcom table to gigabytes; this job
keeps it under control.

The DAG runs in two phases:
  1. analyze_xcoms — dry-run SELECT that shows how many rows will be removed,
                     grouped by dag_id / task_id so you can spot the big producers.
  2. prune_xcoms   — DELETE rows older than the retention window (skipped when
                     dry_run=true).

Connection setup
----------------
Create an Airflow Connection (conn_id = airflow_db) pointing at the metastore:

  conn_type : postgres
  host      : <metastore host>
  port      : 5432
  schema    : airflow          ← database name
  login     : <user with DELETE privilege on the xcom table>
  password  : <password>

Run config
----------
    {
      "conn_id": "airflow_db",
      "retention_days": 30,
      "exclude_dag_ids": [],
      "dry_run": false
    }

Passing exclude_dag_ids lets you protect specific DAGs from pruning (e.g. DAGs
that use XCom as a long-lived key-value store).
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, get_current_context, task

_DEFAULT_CONN_ID = "airflow_db"
_DEFAULT_RETENTION_DAYS = 30


@dag(
    dag_id="18_xcom_pruner",
    schedule="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=[
        "type=utility",
        "exec=compose",
        "subtype=maintenance",
        "intent=operational",
    ],
    doc_md=__doc__,
    params={
        "conn_id": _DEFAULT_CONN_ID,
        "retention_days": _DEFAULT_RETENTION_DAYS,
        "exclude_dag_ids": [],
        "dry_run": False,
    },
)
def xcom_pruner():

    @task(task_id="resolve_config")
    def resolve_config() -> dict:
        ctx = get_current_context()
        params = ctx.get("params") or {}
        conf = (ctx["dag_run"].conf or {}) if ctx.get("dag_run") else {}
        merged = {**params, **conf}
        retention = int(merged.get("retention_days", _DEFAULT_RETENTION_DAYS))
        if retention < 1:
            raise ValueError(f"retention_days must be >= 1, got {retention}")
        return {
            "conn_id": merged.get("conn_id") or _DEFAULT_CONN_ID,
            "retention_days": retention,
            "exclude_dag_ids": list(merged.get("exclude_dag_ids") or []),
            "dry_run": bool(merged.get("dry_run", False)),
        }

    @task(task_id="analyze_xcoms")
    def analyze_xcoms(cfg: dict) -> dict:
        """
        Report how many XCom rows are eligible for pruning, grouped by dag_id/task_id.
        No rows are modified in this task.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=cfg["conn_id"])
        exclude = cfg["exclude_dag_ids"]

        # Build the exclusion clause using safe parameterisation.
        # psycopg2 does not support list binding for IN clauses, so we build
        # individual placeholders.
        if exclude:
            placeholders = ", ".join(["%s"] * len(exclude))
            exclude_clause = f"AND dag_id NOT IN ({placeholders})"
            exclude_params: tuple = tuple(exclude)
        else:
            exclude_clause = ""
            exclude_params = ()

        # Total count of eligible rows
        count_sql = f"""
            SELECT COUNT(*)
            FROM xcom
            WHERE timestamp < NOW() - INTERVAL '{cfg['retention_days']} days'
            {exclude_clause};
        """
        total_rows = hook.get_first(count_sql, parameters=exclude_params or None)[0]

        # Breakdown by dag_id / task_id for reporting
        breakdown_sql = f"""
            SELECT
                dag_id,
                task_id,
                COUNT(*)              AS row_count,
                MIN(timestamp)        AS oldest_entry,
                MAX(timestamp)        AS newest_entry,
                SUM(LENGTH(value))    AS approx_bytes
            FROM xcom
            WHERE timestamp < NOW() - INTERVAL '{cfg['retention_days']} days'
            {exclude_clause}
            GROUP BY dag_id, task_id
            ORDER BY row_count DESC
            LIMIT 50;
        """
        rows = hook.get_records(breakdown_sql, parameters=exclude_params or None)
        breakdown = [
            {
                "dag_id": r[0],
                "task_id": r[1],
                "row_count": r[2],
                "oldest_entry": str(r[3]),
                "newest_entry": str(r[4]),
                "approx_bytes": r[5] or 0,
            }
            for r in rows
        ]

        print(
            f"[INFO] Eligible XCom rows: {total_rows:,} "
            f"(retention={cfg['retention_days']}d, excluded={len(exclude)} DAG(s))"
        )
        return {
            "total_eligible": total_rows,
            "breakdown": breakdown,
        }

    @task(task_id="prune_xcoms")
    def prune_xcoms(cfg: dict, analysis: dict) -> dict:
        """Delete XCom entries older than retention_days (unless dry_run)."""
        total_eligible = analysis.get("total_eligible", 0)

        if total_eligible == 0:
            print("[INFO] No XCom rows to prune.")
            return {"deleted": 0, "dry_run": cfg["dry_run"]}

        if cfg["dry_run"]:
            print(f"[DRY-RUN] Would delete {total_eligible:,} XCom row(s).")
            return {"deleted": 0, "dry_run": True}

        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=cfg["conn_id"])
        exclude = cfg["exclude_dag_ids"]

        if exclude:
            placeholders = ", ".join(["%s"] * len(exclude))
            exclude_clause = f"AND dag_id NOT IN ({placeholders})"
            params: tuple = tuple(exclude)
        else:
            exclude_clause = ""
            params = ()

        delete_sql = f"""
            DELETE FROM xcom
            WHERE timestamp < NOW() - INTERVAL '{cfg['retention_days']} days'
            {exclude_clause};
        """
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(delete_sql, params or None)
        deleted = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()

        print(f"[INFO] Deleted {deleted:,} XCom row(s).")
        return {"deleted": deleted, "dry_run": False}

    @task(task_id="print_summary")
    def print_summary(cfg: dict, analysis: dict, prune_result: dict) -> None:
        width = 95
        total = analysis.get("total_eligible", 0)
        breakdown: list[dict] = analysis.get("breakdown", [])

        print("=" * width)
        print(f"  {'XCOM PRUNER REPORT':^{width - 4}}")
        print("=" * width)
        print(f"  retention_days : {cfg['retention_days']}")
        print(f"  exclude_dag_ids: {cfg['exclude_dag_ids'] or '(none)'}")
        print(f"  dry_run        : {cfg['dry_run']}")
        print(f"\n  Eligible rows  : {total:,}")

        action = "Would delete" if cfg["dry_run"] else "Deleted"
        print(f"  {action}      : {prune_result.get('deleted', total if cfg['dry_run'] else 0):,}")

        if breakdown:
            print(f"\n  {'DAG ID':<35} {'Task ID':<28} {'Rows':>8}  {'Approx size':>12}  Oldest entry")
            print(f"  {'-'*35} {'-'*28} {'-'*8}  {'-'*12}  {'-'*19}")
            for r in breakdown:
                size_kb = r["approx_bytes"] / 1024
                size_label = f"{size_kb:,.1f} KB" if size_kb < 1024 else f"{size_kb/1024:,.1f} MB"
                dag_id = r["dag_id"][:33]
                task_id = r["task_id"][:26]
                print(
                    f"  {dag_id:<35} {task_id:<28} {r['row_count']:>8,}  "
                    f"{size_label:>12}  {r['oldest_entry'][:19]}"
                )

        print("=" * width)

    cfg = resolve_config()
    analysis = analyze_xcoms(cfg)
    prune_result = prune_xcoms(cfg, analysis)
    print_summary(cfg, analysis, prune_result)


xcom_pruner()
