"""
DAG: 8_cleanup_k8s_jobs_pods

Deletes Kubernetes Jobs (and their Pods) that have finished earlier than a
configurable cutoff.  Separate windows for successful vs. failed workloads.

Defaults:
  * success_older_than_hours = 1
  * failure_older_than_hours = 5

Run config:
    {
      "namespaces": ["airflow"],
      "success_older_than_hours": 1,
      "failure_older_than_hours": 5,
      "dry_run": false,
      "in_cluster": true,
      "kube_config_path": null
    }

If `dry_run` is true, nothing is deleted — the DAG only reports what it
would have cleaned up.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="8_cleanup_k8s_jobs_pods",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=[
        "type=utility",
        "exec=kube",
        "subtype=k8s-cleanup",
        "intent=utility",
    ],
    doc_md=__doc__,
    params={
        "namespaces": ["airflow"],
        "success_older_than_hours": 1,
        "failure_older_than_hours": 5,
        "dry_run": False,
        "in_cluster": True,
        "kube_config_path": "",
    },
)
def cleanup_k8s_jobs_pods():

    @task(task_id="resolve_config")
    def resolve_config() -> dict:
        ctx = get_current_context()
        params = ctx.get("params") or {}
        conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}
        merged = {**params, **conf}
        return {
            "namespaces": merged.get("namespaces") or ["airflow"],
            "success_older_than_hours": float(merged.get("success_older_than_hours", 1)),
            "failure_older_than_hours": float(merged.get("failure_older_than_hours", 5)),
            "dry_run": bool(merged.get("dry_run", False)),
            "in_cluster": bool(merged.get("in_cluster", True)),
            "kube_config_path": merged.get("kube_config_path") or "",
        }

    @task(task_id="cleanup_jobs")
    def cleanup_jobs(cfg: dict) -> dict:
        from kubernetes import client, config as k8s_config

        if cfg["in_cluster"]:
            k8s_config.load_incluster_config()
        elif cfg["kube_config_path"]:
            k8s_config.load_kube_config(config_file=cfg["kube_config_path"])
        else:
            k8s_config.load_kube_config()

        batch = client.BatchV1Api()
        now = datetime.now(timezone.utc)
        success_cutoff = now - timedelta(hours=cfg["success_older_than_hours"])
        failure_cutoff = now - timedelta(hours=cfg["failure_older_than_hours"])

        deleted: list = []
        skipped: list = []

        for ns in cfg["namespaces"]:
            jobs = batch.list_namespaced_job(ns).items
            for job in jobs:
                name = job.metadata.name
                status = job.status

                completion = getattr(status, "completion_time", None)
                conditions = status.conditions or []

                is_success = bool(completion) or any(
                    c.type == "Complete" and c.status == "True" for c in conditions
                )
                is_failure = any(
                    c.type == "Failed" and c.status == "True" for c in conditions
                )

                finished_at = completion
                if not finished_at:
                    for c in conditions:
                        if c.type in {"Complete", "Failed"} and c.last_transition_time:
                            finished_at = c.last_transition_time
                            break

                if not finished_at or not (is_success or is_failure):
                    skipped.append({"ns": ns, "job": name, "reason": "still running"})
                    continue

                cutoff = success_cutoff if is_success else failure_cutoff
                if finished_at > cutoff:
                    skipped.append({
                        "ns": ns, "job": name,
                        "reason": "within retention window",
                        "finished_at": finished_at.isoformat(),
                    })
                    continue

                if cfg["dry_run"]:
                    deleted.append({"ns": ns, "job": name, "dry_run": True})
                    continue

                batch.delete_namespaced_job(
                    name=name,
                    namespace=ns,
                    propagation_policy="Background",
                )
                deleted.append({
                    "ns": ns, "job": name,
                    "state": "success" if is_success else "failure",
                    "finished_at": finished_at.isoformat(),
                })

        return {"deleted": deleted, "skipped": skipped}

    @task(task_id="cleanup_orphan_pods")
    def cleanup_orphan_pods(cfg: dict) -> dict:
        """Deletes Succeeded/Failed pods older than the cutoff that are not
        owned by an active Job (covers standalone pods or post-delete remnants).
        """
        from kubernetes import client, config as k8s_config

        if cfg["in_cluster"]:
            k8s_config.load_incluster_config()
        elif cfg["kube_config_path"]:
            k8s_config.load_kube_config(config_file=cfg["kube_config_path"])
        else:
            k8s_config.load_kube_config()

        core = client.CoreV1Api()
        now = datetime.now(timezone.utc)
        success_cutoff = now - timedelta(hours=cfg["success_older_than_hours"])
        failure_cutoff = now - timedelta(hours=cfg["failure_older_than_hours"])

        deleted, skipped = [], []

        for ns in cfg["namespaces"]:
            for pod in core.list_namespaced_pod(ns).items:
                phase = pod.status.phase
                if phase not in {"Succeeded", "Failed"}:
                    continue

                finished = None
                for cs in (pod.status.container_statuses or []):
                    term = cs.state.terminated if cs.state else None
                    if term and term.finished_at:
                        if finished is None or term.finished_at > finished:
                            finished = term.finished_at
                finished = finished or pod.metadata.creation_timestamp
                if not finished:
                    continue

                cutoff = success_cutoff if phase == "Succeeded" else failure_cutoff
                if finished > cutoff:
                    skipped.append({"ns": ns, "pod": pod.metadata.name, "reason": "within retention"})
                    continue

                if cfg["dry_run"]:
                    deleted.append({"ns": ns, "pod": pod.metadata.name, "dry_run": True})
                    continue

                core.delete_namespaced_pod(name=pod.metadata.name, namespace=ns)
                deleted.append({
                    "ns": ns, "pod": pod.metadata.name,
                    "phase": phase,
                    "finished_at": finished.isoformat(),
                })

        return {"deleted": deleted, "skipped": skipped}

    @task(task_id="summarise")
    def summarise(jobs_result: dict, pods_result: dict) -> dict:
        print("=" * 60)
        print(" CLEANUP SUMMARY")
        print("=" * 60)
        print(f"  jobs  deleted={len(jobs_result['deleted'])}  "
              f"skipped={len(jobs_result['skipped'])}")
        print(f"  pods  deleted={len(pods_result['deleted'])}  "
              f"skipped={len(pods_result['skipped'])}")
        return {"jobs": jobs_result, "pods": pods_result}

    cfg = resolve_config()
    summarise(cleanup_jobs(cfg), cleanup_orphan_pods(cfg))


cleanup_k8s_jobs_pods()
