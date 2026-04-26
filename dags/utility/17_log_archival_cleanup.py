"""
DAG: 17_log_archival_cleanup

Archives old Airflow task-log files to Amazon S3 (Glacier Instant Retrieval
storage class) and hard-deletes logs that have exceeded a separate retention
deadline.  Keeps the local /logs directory lean without losing history.

Two configurable windows
  archive_after_days  — logs older than this are uploaded to S3 then removed
                        locally.  Skipped if s3_bucket is empty.
  delete_after_days   — logs older than this are deleted unconditionally
                        (archive window must be shorter or equal).

Expected log layout (Airflow FileTaskHandler default):
  <log_base_dir>/
    dag_id=<dag>/
      run_id=<run>/
        task_id=<task>/
          attempt=<n>.log

S3 connection setup (conn_id = aws_default):
  conn_type : Amazon Web Services
  extra     : {"region_name": "us-east-1"}
  Credentials via IAM instance role or explicit access-key / secret-key fields.

Run config
----------
    {
      "log_base_dir": "/opt/airflow/logs",
      "archive_after_days": 7,
      "delete_after_days": 30,
      "s3_bucket": "my-airflow-logs-archive",
      "s3_prefix": "airflow-logs/",
      "s3_conn_id": "aws_default",
      "glacier_transition": true,
      "dry_run": false
    }

Set dry_run=true to see what would happen without making any changes.
Set s3_bucket="" to skip archival and only perform local deletion.
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, get_current_context, task

_DEFAULT_LOG_BASE_DIR = "/opt/airflow/logs"
_DEFAULT_ARCHIVE_AFTER_DAYS = 7
_DEFAULT_DELETE_AFTER_DAYS = 30
_DEFAULT_S3_PREFIX = "airflow-logs/"
_DEFAULT_S3_CONN_ID = "aws_default"


@dag(
    dag_id="17_log_archival_cleanup",
    schedule="0 3 * * *",
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
        "log_base_dir": _DEFAULT_LOG_BASE_DIR,
        "archive_after_days": _DEFAULT_ARCHIVE_AFTER_DAYS,
        "delete_after_days": _DEFAULT_DELETE_AFTER_DAYS,
        "s3_bucket": "",
        "s3_prefix": _DEFAULT_S3_PREFIX,
        "s3_conn_id": _DEFAULT_S3_CONN_ID,
        "glacier_transition": True,
        "dry_run": False,
    },
)
def log_archival_cleanup():

    @task(task_id="resolve_config")
    def resolve_config() -> dict:
        ctx = get_current_context()
        params = ctx.get("params") or {}
        conf = (ctx["dag_run"].conf or {}) if ctx.get("dag_run") else {}
        merged = {**params, **conf}
        archive_days = int(merged.get("archive_after_days", _DEFAULT_ARCHIVE_AFTER_DAYS))
        delete_days = int(merged.get("delete_after_days", _DEFAULT_DELETE_AFTER_DAYS))
        if archive_days > delete_days:
            raise ValueError(
                f"archive_after_days ({archive_days}) must be <= delete_after_days ({delete_days})"
            )
        return {
            "log_base_dir": merged.get("log_base_dir") or _DEFAULT_LOG_BASE_DIR,
            "archive_after_days": archive_days,
            "delete_after_days": delete_days,
            "s3_bucket": merged.get("s3_bucket") or "",
            "s3_prefix": (merged.get("s3_prefix") or _DEFAULT_S3_PREFIX).strip("/") + "/",
            "s3_conn_id": merged.get("s3_conn_id") or _DEFAULT_S3_CONN_ID,
            "glacier_transition": bool(merged.get("glacier_transition", True)),
            "dry_run": bool(merged.get("dry_run", False)),
        }

    @task(task_id="discover_log_files")
    def discover_log_files(cfg: dict) -> dict:
        """Walk log_base_dir and bucket each *.log file into archive or delete lists."""
        from datetime import timezone
        from pathlib import Path

        log_dir = Path(cfg["log_base_dir"])
        if not log_dir.exists():
            print(f"[WARN] Log base dir does not exist: {log_dir}")
            return {"to_archive": [], "to_delete": [], "within_window": 0}

        now = datetime.now(timezone.utc).timestamp()
        archive_cutoff = now - cfg["archive_after_days"] * 86400
        delete_cutoff = now - cfg["delete_after_days"] * 86400

        to_archive: list[str] = []
        to_delete: list[str] = []
        within_window = 0

        for path in sorted(log_dir.rglob("*.log")):
            try:
                mtime = path.stat().st_mtime
            except OSError:
                continue
            if mtime < delete_cutoff:
                to_delete.append(str(path))
            elif mtime < archive_cutoff:
                to_archive.append(str(path))
            else:
                within_window += 1

        print(
            f"[INFO] Discovery: to_archive={len(to_archive)}  "
            f"to_delete={len(to_delete)}  within_window={within_window}"
        )
        return {
            "to_archive": to_archive,
            "to_delete": to_delete,
            "within_window": within_window,
        }

    @task(task_id="archive_to_s3")
    def archive_to_s3(cfg: dict, discovery: dict) -> dict:
        """Upload archivable logs to S3 Glacier IR then remove local copies."""
        to_archive: list[str] = discovery.get("to_archive", [])

        if not cfg["s3_bucket"]:
            print("[INFO] s3_bucket not configured — skipping archival.")
            return {"archived": 0, "failed": 0, "skipped": len(to_archive)}

        if not to_archive:
            print("[INFO] No log files to archive.")
            return {"archived": 0, "failed": 0, "skipped": 0}

        from pathlib import Path

        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id=cfg["s3_conn_id"])
        storage_class = "GLACIER_IR" if cfg["glacier_transition"] else "STANDARD"
        log_base = Path(cfg["log_base_dir"])
        archived, failed = 0, 0

        for local_str in to_archive:
            local = Path(local_str)
            try:
                relative = local.relative_to(log_base)
            except ValueError:
                print(f"[WARN] {local} is outside log_base_dir — skipping.")
                failed += 1
                continue

            s3_key = cfg["s3_prefix"] + str(relative)

            if cfg["dry_run"]:
                print(f"[DRY-RUN] upload {local} → s3://{cfg['s3_bucket']}/{s3_key}")
                archived += 1
                continue

            try:
                hook.load_file(
                    filename=str(local),
                    key=s3_key,
                    bucket_name=cfg["s3_bucket"],
                    replace=True,
                    extra_args={"StorageClass": storage_class},
                )
                local.unlink(missing_ok=True)
                archived += 1
            except Exception as exc:
                print(f"[ERROR] archive {local}: {exc}")
                failed += 1

        print(f"[INFO] Archival done — archived={archived}  failed={failed}")
        return {"archived": archived, "failed": failed, "skipped": 0}

    @task(task_id="delete_expired_logs")
    def delete_expired_logs(cfg: dict, discovery: dict) -> dict:
        """Hard-delete logs past the delete_after_days deadline."""
        from pathlib import Path

        to_delete: list[str] = discovery.get("to_delete", [])
        if not to_delete:
            print("[INFO] No expired log files to delete.")
            return {"deleted": 0, "failed": 0, "empty_dirs_removed": 0}

        deleted, failed = 0, 0
        for path_str in to_delete:
            path = Path(path_str)
            if cfg["dry_run"]:
                print(f"[DRY-RUN] delete {path}")
                deleted += 1
                continue
            try:
                path.unlink(missing_ok=True)
                deleted += 1
            except Exception as exc:
                print(f"[ERROR] delete {path}: {exc}")
                failed += 1

        # Prune empty parent directories bottom-up, staying inside log_base_dir.
        empty_dirs_removed = 0
        if not cfg["dry_run"]:
            log_base = cfg["log_base_dir"].rstrip("/")
            visited: set[str] = set()
            for path_str in to_delete:
                for parent in Path(path_str).parents:
                    parent_str = str(parent)
                    if parent_str == log_base or parent_str in visited:
                        break
                    visited.add(parent_str)
                    try:
                        parent.rmdir()
                        empty_dirs_removed += 1
                    except OSError:
                        break  # not empty or already gone

        print(
            f"[INFO] Deletion done — deleted={deleted}  failed={failed}"
            f"  empty_dirs_removed={empty_dirs_removed}"
        )
        return {"deleted": deleted, "failed": failed, "empty_dirs_removed": empty_dirs_removed}

    @task(task_id="print_summary")
    def print_summary(
        cfg: dict,
        discovery: dict,
        archive_result: dict,
        delete_result: dict,
    ) -> None:
        width = 80
        print("=" * width)
        print(f"  {'LOG ARCHIVAL & CLEANUP REPORT':^{width - 4}}")
        print("=" * width)
        print(f"  log_base_dir      : {cfg['log_base_dir']}")
        print(f"  archive_after_days: {cfg['archive_after_days']}")
        print(f"  delete_after_days : {cfg['delete_after_days']}")
        print(f"  dry_run           : {cfg['dry_run']}")
        print()
        print("  Discovery")
        print(f"    to archive  : {len(discovery.get('to_archive', []))}")
        print(f"    to delete   : {len(discovery.get('to_delete', []))}")
        print(f"    within window: {discovery.get('within_window', 0)}")
        print()
        bucket_label = cfg["s3_bucket"] or "not configured"
        print(f"  S3 Archival (bucket: {bucket_label})")
        print(f"    archived : {archive_result.get('archived', 0)}")
        print(f"    failed   : {archive_result.get('failed', 0)}")
        print(f"    skipped  : {archive_result.get('skipped', 0)}")
        print()
        print("  Local Deletion")
        print(f"    deleted          : {delete_result.get('deleted', 0)}")
        print(f"    failed           : {delete_result.get('failed', 0)}")
        print(f"    empty dirs pruned: {delete_result.get('empty_dirs_removed', 0)}")
        print("=" * width)

    cfg = resolve_config()
    discovery = discover_log_files(cfg)
    archive_result = archive_to_s3(cfg, discovery)
    delete_result = delete_expired_logs(cfg, discovery)
    print_summary(cfg, discovery, archive_result, delete_result)


log_archival_cleanup()
