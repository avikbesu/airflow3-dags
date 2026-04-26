"""
DAG: 16_metastore_statistics_refresher

Runs ANALYZE on Airflow's Postgres metastore tables to keep the query-planner
statistics up-to-date.  Stale statistics cause poor index selection and slow
queries on dag_run, task_instance, log, and xcom — especially after bulk inserts
or deletions from log cleanup and XCom pruning jobs.

After each ANALYZE the DAG reads pg_stat_user_tables to surface live / dead
tuple counts so you can spot bloat at a glance.

Connection setup
----------------
Create an Airflow Connection (conn_id = airflow_db) pointing at the metastore:

  conn_type : postgres
  host      : <metastore host>
  port      : 5432
  schema    : airflow          ← database name
  login     : <user>
  password  : <password>

The user only needs SELECT + pg_analyze privilege (or table ownership).

Run config
----------
    {
      "conn_id": "airflow_db",
      "tables": ["dag_run", "task_instance"],
      "verbose": true
    }

Passing an empty "tables" list falls back to the default set.
"""

from __future__ import annotations

import re
from datetime import datetime

from airflow.sdk import dag, get_current_context, task

_DEFAULT_CONN_ID = "airflow_db"

_DEFAULT_TABLES = [
    "dag",
    "dag_run",
    "dag_version",
    "job",
    "log",
    "serialized_dag",
    "task_instance",
    "task_reschedule",
    "trigger",
    "xcom",
]

_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_table(name: str) -> str:
    """Reject names that are not safe SQL identifiers."""
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(f"Unsafe table name rejected: {name!r}")
    return name


@dag(
    dag_id="16_metastore_statistics_refresher",
    schedule="0 2 * * *",
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
        "tables": _DEFAULT_TABLES,
        "verbose": True,
    },
)
def metastore_statistics_refresher():

    @task(task_id="resolve_config")
    def resolve_config() -> dict:
        ctx = get_current_context()
        params = ctx.get("params") or {}
        conf = (ctx["dag_run"].conf or {}) if ctx.get("dag_run") else {}
        merged = {**params, **conf}
        tables = merged.get("tables") or _DEFAULT_TABLES
        return {
            "conn_id": merged.get("conn_id") or _DEFAULT_CONN_ID,
            "tables": [_validate_table(t) for t in tables],
            "verbose": bool(merged.get("verbose", True)),
        }

    @task(task_id="run_analyze")
    def run_analyze(cfg: dict) -> dict:
        """ANALYZE each table. Requires autocommit — ANALYZE cannot run in a txn block."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=cfg["conn_id"])
        conn = hook.get_conn()
        conn.autocommit = True
        cursor = conn.cursor()

        results: dict[str, str] = {}
        for table in cfg["tables"]:
            try:
                cursor.execute(f"ANALYZE {table};")  # names validated above
                results[table] = "ok"
                if cfg["verbose"]:
                    print(f"[OK]    ANALYZE {table}")
            except Exception as exc:
                results[table] = f"error: {exc}"
                print(f"[ERROR] ANALYZE {table}: {exc}")

        cursor.close()
        conn.close()
        return results

    @task(task_id="collect_table_stats")
    def collect_table_stats(cfg: dict, analyze_results: dict) -> list[dict]:
        """Read pg_stat_user_tables after ANALYZE so stats reflect the fresh scan."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=cfg["conn_id"])
        placeholders = ", ".join(f"'{t}'" for t in cfg["tables"])
        rows = hook.get_records(
            f"""
            SELECT
                relname,
                n_live_tup,
                n_dead_tup,
                last_analyze,
                last_autoanalyze,
                pg_size_pretty(pg_total_relation_size(relid)) AS total_size
            FROM pg_stat_user_tables
            WHERE relname IN ({placeholders})
            ORDER BY n_live_tup DESC;
            """
        )
        return [
            {
                "table": row[0],
                "live_rows": row[1],
                "dead_rows": row[2],
                "last_analyze": str(row[3]),
                "last_autoanalyze": str(row[4]),
                "total_size": row[5],
            }
            for row in rows
        ]

    @task(task_id="print_report")
    def print_report(analyze_results: dict, stats: list[dict]) -> None:
        width = 95
        ok = sum(1 for v in analyze_results.values() if v == "ok")
        err = len(analyze_results) - ok

        print("=" * width)
        print(f"  {'METASTORE STATISTICS REFRESHER REPORT':^{width - 4}}")
        print("=" * width)
        print(f"\n  ANALYZE — {ok} succeeded, {err} failed\n")

        if stats:
            hdr = f"  {'Table':<28} {'Live rows':>12} {'Dead rows':>12} {'Total size':>12}  Last Analyze"
            print(hdr)
            print(f"  {'-'*28} {'-'*12} {'-'*12} {'-'*12}  {'-'*26}")
            for s in stats:
                print(
                    f"  {s['table']:<28} {s['live_rows']:>12,} {s['dead_rows']:>12,}"
                    f" {s['total_size']:>12}  {s['last_analyze']}"
                )

        errors = {t: v for t, v in analyze_results.items() if v != "ok"}
        if errors:
            print("\n  [ERRORS]")
            for t, msg in errors.items():
                print(f"    {t}: {msg}")

        print("=" * width)

    cfg = resolve_config()
    analyze_results = run_analyze(cfg)
    stats = collect_table_stats(cfg, analyze_results)
    print_report(analyze_results, stats)


metastore_statistics_refresher()
