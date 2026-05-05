"""
DAG: k1_kubepod_operator

Execution method 1 of 4 — KubernetesPodOperator
================================================
Creates a bare Kubernetes Pod (not a Job resource).  Airflow owns the full
lifecycle: it submits the pod, streams logs, waits for termination, and
handles retries at the task level — not the K8s level.

When to prefer this over KubernetesJobOperator:
  * Simple one-shot tasks where Airflow's own retry is sufficient
  * You need sidecar containers alongside the main workload
  * Short-lived tasks that don't justify a Job resource overhead

Five patterns in this DAG (all run in parallel → summarise):
  1. basic_pod            – minimal alpine container with env vars
  2. env_from_config      – pull all keys from a ConfigMap as env vars
  3. volume_mount_pod     – emptyDir volume shared within the pod
  4. init_container_pod   – init container pre-seeds a file the main reads
  5. resource_profile_pod – requests/limits + nodeSelector + tolerations

Requirements:
  - apache-airflow-providers-cncf-kubernetes >= 8.0
  - RBAC: Airflow ServiceAccount must have pod create/get/watch/delete in
    the target namespace
"""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

with DAG(
    dag_id="k1_kubepod_operator",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["type=demo", "exec=kube", "subtype=k8s-pod", "intent=demo"],
    doc_md=__doc__,
) as dag:

    # ------------------------------------------------------------------
    # Pattern 1 — Bare minimum: image + command + env vars
    # KubernetesPodOperator creates a Pod; Airflow waits for it to exit 0.
    # ------------------------------------------------------------------
    basic_pod = KubernetesPodOperator(
        task_id="basic_pod",
        name="af-basic-pod",
        namespace="airflow",
        image="alpine:3.19",
        cmds=["sh", "-c"],
        arguments=[
            "echo '==> Running basic K8s pod'; "
            "echo \"Host: $(hostname)\"; "
            "echo \"Date: $(date -u)\"; "
            "echo '==> Done.'"
        ],
        env_vars={
            "APP_ENV":    "production",
            "JOB_SOURCE": "airflow-k1-dag",
        },
        labels={"managed-by": "airflow", "pattern": "basic"},
        get_logs=True,
        is_delete_operator_pod=True,   # clean up pod after task finishes
        in_cluster=True,
    )

    # ------------------------------------------------------------------
    # Pattern 2 — Pull all keys from a ConfigMap into the environment.
    # Assumes a ConfigMap named `airflow-config` exists in the namespace.
    # Create it with:
    #   kubectl create configmap airflow-config \
    #     --from-literal=APP_VERSION=1.2.3 \
    #     --from-literal=LOG_LEVEL=INFO \
    #     -n airflow
    # ------------------------------------------------------------------
    env_from_config = KubernetesPodOperator(
        task_id="env_from_config",
        name="af-env-config-pod",
        namespace="airflow",
        image="alpine:3.19",
        cmds=["sh", "-c"],
        arguments=[
            "echo 'Env from ConfigMap:'; "
            "echo \"APP_VERSION=$APP_VERSION\"; "
            "echo \"LOG_LEVEL=$LOG_LEVEL\"; "
            "echo 'Secret check (should be non-empty):'; "
            "[ -n \"$DB_PASSWORD\" ] && echo 'DB_PASSWORD: SET' || echo 'DB_PASSWORD: not found (Secret may be missing)'"
        ],
        # env_from imports ALL keys from a ConfigMap or Secret as env vars
        env_from=[
            k8s.V1EnvFromSource(
                config_map_ref=k8s.V1ConfigMapEnvSource(
                    name="airflow-config",
                    optional=True,   # pod still starts if ConfigMap is absent
                )
            ),
        ],
        # Individual key from a Secret → named env var
        env_vars=[
            k8s.V1EnvVar(
                name="DB_PASSWORD",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="airflow-secret",
                        key="db-password",
                        optional=True,
                    )
                ),
            )
        ],
        labels={"managed-by": "airflow", "pattern": "env-from-config"},
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    # ------------------------------------------------------------------
    # Pattern 3 — emptyDir volume: write a file, confirm it exists.
    # emptyDir is ephemeral (lives for the pod lifetime).  Use a PVC
    # instead if data must survive pod restarts or be shared across pods.
    # ------------------------------------------------------------------
    volume_mount_pod = KubernetesPodOperator(
        task_id="volume_mount_pod",
        name="af-volume-pod",
        namespace="airflow",
        image="alpine:3.19",
        cmds=["sh", "-c"],
        arguments=[
            "echo 'Writing to emptyDir volume...'; "
            "mkdir -p /workspace; "
            "echo '{\"status\": \"ok\", \"rows\": 42000}' > /workspace/result.json; "
            "echo 'Contents:'; cat /workspace/result.json; "
            "echo 'Size:'; du -sh /workspace"
        ],
        volumes=[
            k8s.V1Volume(
                name="workspace",
                empty_dir=k8s.V1EmptyDirVolumeSource(
                    medium="",           # "" = default disk; "Memory" = tmpfs
                    size_limit="512Mi",
                ),
            )
        ],
        volume_mounts=[
            k8s.V1VolumeMount(
                name="workspace",
                mount_path="/workspace",
            )
        ],
        labels={"managed-by": "airflow", "pattern": "volume-mount"},
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    # ------------------------------------------------------------------
    # Pattern 4 — Init container: seeds a shared volume before the main
    # container starts.  Useful for downloading config files, running
    # DB migrations, or decrypting secrets before the main app boots.
    # ------------------------------------------------------------------
    init_container_pod = KubernetesPodOperator(
        task_id="init_container_pod",
        name="af-init-pod",
        namespace="airflow",
        image="alpine:3.19",
        cmds=["sh", "-c"],
        arguments=[
            "echo '==> Main container reading init-seeded config:'; "
            "cat /shared/config.env; "
            "echo '==> Sourcing config:'; "
            ". /shared/config.env; "
            "echo \"APP_VERSION=$APP_VERSION  REGION=$REGION\""
        ],
        # Init containers run to completion sequentially before the main
        # container starts.  They share volumes but not the process namespace.
        init_containers=[
            k8s.V1Container(
                name="init-seeder",
                image="alpine:3.19",
                command=["sh", "-c"],
                args=[
                    "echo 'Init container writing config...'; "
                    "echo 'APP_VERSION=2.5.1' > /shared/config.env; "
                    "echo 'REGION=us-east-1' >> /shared/config.env; "
                    "echo 'ENV=production' >> /shared/config.env; "
                    "echo 'Init done.'"
                ],
                volume_mounts=[
                    k8s.V1VolumeMount(name="shared-data", mount_path="/shared")
                ],
            )
        ],
        volumes=[
            k8s.V1Volume(
                name="shared-data",
                empty_dir=k8s.V1EmptyDirVolumeSource(),
            )
        ],
        volume_mounts=[
            k8s.V1VolumeMount(name="shared-data", mount_path="/shared")
        ],
        labels={"managed-by": "airflow", "pattern": "init-container"},
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    # ------------------------------------------------------------------
    # Pattern 5 — Resource profile: set CPU/memory budgets, pin to nodes
    # matching a label, and tolerate a taint for dedicated batch nodes.
    #
    # nodeSelector: only schedule on nodes with kubernetes.io/os=linux
    # tolerations:  allow scheduling on nodes tainted  dedicated=batch:NoSchedule
    # ------------------------------------------------------------------
    resource_profile_pod = KubernetesPodOperator(
        task_id="resource_profile_pod",
        name="af-resource-pod",
        namespace="airflow",
        image="python:3.11-alpine",
        cmds=["python", "-c"],
        arguments=[
            "import platform, os; "
            "print(f'Python {platform.python_version()} on {platform.machine()}'); "
            "print(f'CPU count hint: {os.cpu_count()}'); "
            "print('Resource limits enforced by K8s cgroups.')"
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "256Mi", "cpu": "250m"},
            limits={"memory": "512Mi",  "cpu": "500m"},
        ),
        # Only schedule on Linux nodes (safe default on most clusters)
        node_selector={"kubernetes.io/os": "linux"},
        # Allow but don't require dedicated batch nodes
        tolerations=[
            k8s.V1Toleration(
                key="dedicated",
                operator="Equal",
                value="batch",
                effect="NoSchedule",
            )
        ],
        # Prefer nodes with the `workload-type=batch` label (soft constraint)
        affinity=k8s.V1Affinity(
            node_affinity=k8s.V1NodeAffinity(
                preferred_during_scheduling_ignored_during_execution=[
                    k8s.V1PreferredSchedulingTerm(
                        weight=50,
                        preference=k8s.V1NodeSelectorTerm(
                            match_expressions=[
                                k8s.V1NodeSelectorRequirement(
                                    key="workload-type",
                                    operator="In",
                                    values=["batch"],
                                )
                            ]
                        ),
                    )
                ]
            )
        ),
        labels={"managed-by": "airflow", "pattern": "resource-profile"},
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    # ------------------------------------------------------------------
    # Aggregation task — runs after all pods finish
    # ------------------------------------------------------------------
    @task
    def summarise():
        patterns = [
            "basic_pod            — minimal image + env vars",
            "env_from_config      — ConfigMap/Secret env injection",
            "volume_mount_pod     — emptyDir volume r/w",
            "init_container_pod   — init container seeds shared volume",
            "resource_profile_pod — CPU/memory limits + node affinity",
        ]
        print("All KubernetesPodOperator patterns completed:")
        for p in patterns:
            print(f"  ✓ {p}")

    [
        basic_pod,
        env_from_config,
        volume_mount_pod,
        init_container_pod,
        resource_profile_pod,
    ] >> summarise()
