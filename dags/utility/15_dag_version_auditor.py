"""
DAG: dag_version_auditor
Purpose: Track DAG file changes by comparing current file hashes against a
stored baseline. Detects new, modified, and deleted DAG files since the last
run. Optionally enriches output with git log metadata when git is available.
Sends a notification summary and updates the baseline on demand.

Baseline is persisted as an Airflow Variable (JSON dict of {path: hash}).
Set _UPDATE_BASELINE = True on first run to establish the baseline, then
flip it back to False for ongoing auditing.

Tune the constants below to adjust the audit scope.
"""
from __future__ import annotations

import hashlib
import os
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task, Variable

_DAGS_FOLDER = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags")
_BASELINE_VARIABLE = "dag_version_auditor_baseline"   # Airflow Variable key
_FILE_GLOB = "**/*.py"                                 # pattern for DAG files
_UPDATE_BASELINE = False  # set True to save the current hashes as the new baseline
_MAX_GIT_LOG_LINES = 5    # recent git commits to include per changed file


def _hash_file(path: str) -> str:
    """Return MD5 hex digest of a file's contents."""
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _scan_dag_files(dags_folder: str, glob_pattern: str) -> dict[str, str]:
    """Return {relative_path: md5_hash} for all matching files under dags_folder."""
    root = Path(dags_folder)
    result: dict[str, str] = {}
    for path in sorted(root.glob(glob_pattern)):
        if path.is_file():
            rel = str(path.relative_to(root))
            result[rel] = _hash_file(str(path))
    return result


@dag(
    dag_id="15_dag_version_auditor",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=[
        "type=utility",
        "exec=kube", "exec=compose",
        "subtype=monitoring",
        "intent=operational",
    ],
    doc_md="""
## DAG Version Auditor

Tracks changes to DAG files by comparing MD5 hashes against a persisted baseline
stored in an Airflow Variable. Reports:

- **New files**: `.py` files added since the baseline was set
- **Modified files**: files whose hash has changed
- **Deleted files**: files present in the baseline but no longer on disk

Enriches results with recent `git log` output when git is available in the worker.

**First-time setup**: set `_UPDATE_BASELINE = True` in the module constants, trigger
once to establish the baseline, then flip back to `False` for ongoing auditing.

**Module-level constants:**

| Constant | Default | Description |
|----------|---------|-------------|
| `_DAGS_FOLDER` | env / /opt/airflow/dags | Path scanned for DAG files |
| `_BASELINE_VARIABLE` | dag_version_auditor_baseline | Airflow Variable storing the baseline |
| `_FILE_GLOB` | `**/*.py` | Glob pattern for DAG files |
| `_UPDATE_BASELINE` | False | Set True to save current hashes as new baseline |

Trigger manually:
```bash
airflow dags trigger 15_dag_version_auditor
```
""",
)
def dag_version_auditor():

    @task(task_id="compute_current_hashes")
    def compute_current_hashes() -> dict[str, str]:
        """Compute MD5 hashes for all DAG files currently on disk."""
        hashes = _scan_dag_files(_DAGS_FOLDER, _FILE_GLOB)
        print(f"[INFO] Scanned {len(hashes)} file(s) under {_DAGS_FOLDER}.")
        return hashes

    @task(task_id="load_baseline")
    def load_baseline() -> dict[str, str]:
        """
        Load the stored baseline from Airflow Variable.
        Returns an empty dict on first run (no baseline yet).
        """
        import json

        raw = Variable.get(_BASELINE_VARIABLE, default="{}")
        try:
            baseline = json.loads(raw)
        except (ValueError, TypeError):
            baseline = {}

        if baseline:
            print(f"[INFO] Loaded baseline with {len(baseline)} file(s).")
        else:
            print("[INFO] No baseline found — all current files will appear as new.")
        return baseline

    @task(task_id="detect_changes")
    def detect_changes(current: dict[str, str], baseline: dict[str, str]) -> dict:
        """
        Compare current hashes against the baseline.
        Returns categorised change sets and optional git metadata.
        """
        current_paths = set(current.keys())
        baseline_paths = set(baseline.keys())

        new_files = sorted(current_paths - baseline_paths)
        deleted_files = sorted(baseline_paths - current_paths)
        modified_files = sorted(
            p for p in current_paths & baseline_paths
            if current[p] != baseline[p]
        )

        # Enrich with git log when available
        git_metadata: dict[str, str] = {}
        changed_paths = new_files + modified_files
        if changed_paths:
            try:
                import subprocess
                for rel_path in changed_paths:
                    abs_path = os.path.join(_DAGS_FOLDER, rel_path)
                    result = subprocess.run(
                        ["git", "log", f"--max-count={_MAX_GIT_LOG_LINES}",
                         "--oneline", "--follow", "--", abs_path],
                        capture_output=True, text=True, timeout=10,
                        cwd=_DAGS_FOLDER,
                    )
                    if result.returncode == 0 and result.stdout.strip():
                        git_metadata[rel_path] = result.stdout.strip()
            except Exception as exc:
                print(f"[INFO] git metadata unavailable: {exc}")

        total_changes = len(new_files) + len(modified_files) + len(deleted_files)
        print(
            f"[INFO] Changes detected: {len(new_files)} new, "
            f"{len(modified_files)} modified, {len(deleted_files)} deleted "
            f"({total_changes} total)."
        )
        return {
            "new_files": new_files,
            "modified_files": modified_files,
            "deleted_files": deleted_files,
            "git_metadata": git_metadata,
            "total_changes": total_changes,
        }

    @task(task_id="report_and_update_baseline")
    def report_and_update_baseline(
        changes: dict,
        current: dict[str, str],
    ) -> None:
        """
        Print the audit report. If _UPDATE_BASELINE is True, persist the current
        hashes as the new baseline in the Airflow Variable.
        """
        import json

        new_files = changes.get("new_files", [])
        modified_files = changes.get("modified_files", [])
        deleted_files = changes.get("deleted_files", [])
        git_metadata = changes.get("git_metadata", {})
        total_changes = changes.get("total_changes", 0)

        width = 74
        print("=" * width)
        print(f"  {'DAG VERSION AUDIT REPORT':^{width - 4}}")
        print(f"  Scanned folder : {_DAGS_FOLDER}")
        print(f"  Total DAG files: {len(current)}")
        print(f"  Total changes  : {total_changes}")
        print("=" * width)

        def _print_section(label: str, files: list[str], symbol: str) -> None:
            print(f"\n[{label}] {len(files)} file(s)")
            if files:
                for f in files:
                    print(f"  {symbol} {f}")
                    git_log = git_metadata.get(f)
                    if git_log:
                        for line in git_log.splitlines():
                            print(f"      git: {line}")
            else:
                print("  None.")

        _print_section("NEW FILES", new_files, "+")
        _print_section("MODIFIED FILES", modified_files, "~")
        _print_section("DELETED FILES", deleted_files, "-")

        if _UPDATE_BASELINE:
            Variable.set(_BASELINE_VARIABLE, json.dumps(current))
            print(f"\n[INFO] Baseline updated — {len(current)} file(s) stored in Variable '{_BASELINE_VARIABLE}'.")
        else:
            print(f"\n[INFO] _UPDATE_BASELINE=False — baseline unchanged.")
            if total_changes:
                print(f"[INFO] Set _UPDATE_BASELINE=True to accept current state as the new baseline.")

        print("\n" + "=" * width)
        verdict = "CLEAN — no changes since baseline." if not total_changes else f"CHANGES DETECTED — {total_changes} file(s) changed."
        print(f"  Result: {verdict}")
        print("=" * width)

    current_hashes = compute_current_hashes()
    baseline_hashes = load_baseline()
    changes = detect_changes(current_hashes, baseline_hashes)
    report_and_update_baseline(changes, current_hashes)


dag_version_auditor()
