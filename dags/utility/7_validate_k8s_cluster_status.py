"""
DAG: 7_validate_k8s_cluster_status

Validates the health of a Kubernetes cluster on a per-namespace basis and
emits a JSON report as XCom (and to the task log).

Per-namespace checks:
  * pod phase counts (Running / Pending / Failed / Succeeded / Unknown)
  * pods in CrashLoopBackOff or ImagePullBackOff
  * deployments with unavailable replicas
  * PVCs not in Bound phase
  * nodes flagged NotReady (cluster-wide, reported once)

Run config:
    {
      "namespaces": ["airflow", "default"],   # omit to scan all
      "in_cluster": true,
      "kube_config_path": null
    }
"""

from __future__ import annotations

import json
from datetime import datetime

from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="7_validate_k8s_cluster_status",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=[
        "type=utility",
        "exec=kube",
        "subtype=k8s-health",
        "intent=utility",
    ],
    doc_md=__doc__,
    params={
        "namespaces": [],
        "in_cluster": True,
        "kube_config_path": "",
    },
)
def validate_k8s_cluster_status():

    @task(task_id="load_kube_client")
    def load_kube_client() -> dict:
        """Resolves config source from run conf — returns dict for downstream tasks."""
        ctx = get_current_context()
        params = ctx.get("params") or {}
        conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}
        merged = {**params, **conf}
        return {
            "in_cluster": bool(merged.get("in_cluster", True)),
            "kube_config_path": merged.get("kube_config_path") or "",
            "namespaces": merged.get("namespaces") or [],
        }

    @task(task_id="collect_report")
    def collect_report(cfg: dict) -> dict:
        from kubernetes import client, config as k8s_config

        if cfg["in_cluster"]:
            k8s_config.load_incluster_config()
        elif cfg["kube_config_path"]:
            k8s_config.load_kube_config(config_file=cfg["kube_config_path"])
        else:
            k8s_config.load_kube_config()

        core = client.CoreV1Api()
        apps = client.AppsV1Api()

        # Determine namespaces to scan
        target_ns = cfg["namespaces"]
        if not target_ns:
            target_ns = [ns.metadata.name for ns in core.list_namespace().items]

        # Cluster-wide node health (reported once)
        not_ready_nodes = []
        for node in core.list_node().items:
            ready = next(
                (c for c in (node.status.conditions or []) if c.type == "Ready"),
                None,
            )
            if not ready or ready.status != "True":
                not_ready_nodes.append(node.metadata.name)

        report: dict = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "cluster": {
                "nodes_total": len(core.list_node().items),
                "nodes_not_ready": not_ready_nodes,
            },
            "namespaces": {},
        }

        for ns in target_ns:
            pods = core.list_namespaced_pod(ns).items
            phases: dict = {}
            bad_pods: list = []

            for p in pods:
                phase = p.status.phase or "Unknown"
                phases[phase] = phases.get(phase, 0) + 1
                for cs in (p.status.container_statuses or []):
                    waiting = cs.state.waiting if cs.state else None
                    if waiting and waiting.reason in {"CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull"}:
                        bad_pods.append({
                            "pod": p.metadata.name,
                            "container": cs.name,
                            "reason": waiting.reason,
                            "message": waiting.message,
                        })

            deploys_unavail = []
            for d in apps.list_namespaced_deployment(ns).items:
                desired = d.spec.replicas or 0
                avail = d.status.available_replicas or 0
                if desired and avail < desired:
                    deploys_unavail.append({
                        "deployment": d.metadata.name,
                        "desired": desired,
                        "available": avail,
                    })

            pvcs_unbound = []
            for pvc in core.list_namespaced_persistent_volume_claim(ns).items:
                if (pvc.status.phase or "") != "Bound":
                    pvcs_unbound.append({
                        "pvc": pvc.metadata.name,
                        "phase": pvc.status.phase,
                    })

            healthy = (
                not bad_pods
                and not deploys_unavail
                and not pvcs_unbound
                and phases.get("Failed", 0) == 0
            )

            report["namespaces"][ns] = {
                "healthy": healthy,
                "pod_phase_counts": phases,
                "pods_in_bad_state": bad_pods,
                "deployments_unavailable": deploys_unavail,
                "pvcs_not_bound": pvcs_unbound,
            }

        return report

    @task(task_id="print_report")
    def print_report(report: dict) -> dict:
        print("=" * 70)
        print(" KUBERNETES CLUSTER HEALTH REPORT")
        print("=" * 70)
        print(json.dumps(report, indent=2, default=str))
        unhealthy = [
            ns for ns, info in report.get("namespaces", {}).items()
            if not info.get("healthy")
        ]
        if unhealthy:
            print(f"[WARN] Unhealthy namespaces: {unhealthy}")
        else:
            print("[OK] All scanned namespaces are healthy.")
        return report

    cfg = load_kube_client()
    report = collect_report(cfg)
    print_report(report)


validate_k8s_cluster_status()
