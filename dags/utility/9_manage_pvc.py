"""
DAG: 9_manage_pvc

Manages a Kubernetes PersistentVolumeClaim.

Defaults:
  * action     = "create"
  * name       = "airflow-default-pvc"
  * namespace  = "airflow"
  * storage    = "1Gi"
  * access_modes = ["ReadWriteOnce"]
  * storage_class = None  (cluster default)

Behaviour:
  * action="create": skipped if PVC already exists.
  * action="delete": skipped if PVC does not exist.

Run config overrides everything:
    {
      "action": "delete",
      "name": "my-pvc",
      "namespace": "data",
      "storage": "10Gi",
      "access_modes": ["ReadWriteMany"],
      "storage_class": "gp3",
      "in_cluster": true,
      "kube_config_path": null
    }
"""

from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, get_current_context, task

_DEFAULTS = {
    "action": "create",
    "name": "airflow-default-pvc",
    "namespace": "airflow",
    "storage": "1Gi",
    "access_modes": ["ReadWriteOnce"],
    "storage_class": "",
    "in_cluster": True,
    "kube_config_path": "",
}


@dag(
    dag_id="9_manage_pvc",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=[
        "type=utility",
        "exec=kube",
        "subtype=k8s-pvc",
        "intent=utility",
    ],
    doc_md=__doc__,
    params=_DEFAULTS,
)
def manage_pvc():

    @task(task_id="resolve_config")
    def resolve_config() -> dict:
        ctx = get_current_context()
        params = ctx.get("params") or {}
        conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}
        merged = {**_DEFAULTS, **params, **conf}
        action = str(merged["action"]).lower()
        if action not in {"create", "delete"}:
            raise ValueError(f"Invalid action '{action}' — must be create or delete.")
        merged["action"] = action
        return merged

    @task(task_id="apply_pvc")
    def apply_pvc(cfg: dict) -> dict:
        from kubernetes import client, config as k8s_config
        from kubernetes.client.rest import ApiException

        if cfg["in_cluster"]:
            k8s_config.load_incluster_config()
        elif cfg["kube_config_path"]:
            k8s_config.load_kube_config(config_file=cfg["kube_config_path"])
        else:
            k8s_config.load_kube_config()

        core = client.CoreV1Api()
        name = cfg["name"]
        ns = cfg["namespace"]

        # ── existence check ──────────────────────────────────────────────
        exists = True
        try:
            core.read_namespaced_persistent_volume_claim(name=name, namespace=ns)
        except ApiException as e:
            if e.status == 404:
                exists = False
            else:
                raise

        if cfg["action"] == "create":
            if exists:
                msg = f"[SKIP] PVC '{name}' already exists in namespace '{ns}'."
                print(msg)
                return {"action": "create", "status": "skipped", "name": name, "namespace": ns}

            body = client.V1PersistentVolumeClaim(
                metadata=client.V1ObjectMeta(name=name),
                spec=client.V1PersistentVolumeClaimSpec(
                    access_modes=cfg["access_modes"],
                    resources=client.V1ResourceRequirements(
                        requests={"storage": cfg["storage"]},
                    ),
                    storage_class_name=cfg["storage_class"] or None,
                ),
            )
            core.create_namespaced_persistent_volume_claim(namespace=ns, body=body)
            print(f"[CREATE] PVC '{name}' in namespace '{ns}' (storage={cfg['storage']}).")
            return {"action": "create", "status": "created", "name": name, "namespace": ns}

        # action == delete
        if not exists:
            msg = f"[SKIP] PVC '{name}' does not exist in namespace '{ns}'."
            print(msg)
            return {"action": "delete", "status": "skipped", "name": name, "namespace": ns}

        core.delete_namespaced_persistent_volume_claim(name=name, namespace=ns)
        print(f"[DELETE] PVC '{name}' removed from namespace '{ns}'.")
        return {"action": "delete", "status": "deleted", "name": name, "namespace": ns}

    apply_pvc(resolve_config())


manage_pvc()
