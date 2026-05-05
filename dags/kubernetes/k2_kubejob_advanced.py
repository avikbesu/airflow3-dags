"""
DAG: k2_kubejob_advanced

Execution method 2 of 4 — KubernetesJobOperator (advanced patterns)
====================================================================
KubernetesJobOperator creates a Kubernetes *Job* resource — not a bare Pod.
A K8s Job tracks completions, handles pod-level retries automatically, and
persists in the API server after the workload finishes (useful for auditing).

Prefer KubernetesJobOperator over KubernetesPodOperator when:
  * You need K8s-level pod retry (backoffLimit) in addition to Airflow retries
  * The workload must run N completions with M workers in parallel
  * You want TTL-based auto-cleanup of finished jobs
  * You need an audit trail of job history in `kubectl get jobs`

Four advanced patterns in this DAG (run sequentially to show each clearly):
  1. ttl_backoff_job        – TTL auto-cleanup + backoffLimit + active deadline
  2. parallel_workers_job   – completions=6 / parallelism=3 (batch fan-out)
  3. init_container_job     – init container via full_pod_spec override
  4. priority_annotated_job – PriorityClass + Prometheus scrape annotations

Requirements:
  - apache-airflow-providers-cncf-kubernetes >= 8.0
  - RBAC: ServiceAccount must have jobs create/get/watch/delete
"""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG, task
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from kubernetes.client import models as k8s

with DAG(
    dag_id="k2_kubejob_advanced",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["type=demo", "exec=kube", "subtype=k8s-job", "intent=demo"],
    doc_md=__doc__,
) as dag:

    # ------------------------------------------------------------------
    # Pattern 1 — TTL cleanup + backoff retry + active deadline
    #
    # ttl_seconds_after_finished: K8s deletes the Job object this many
    #   seconds after it completes (keeps `kubectl get jobs` tidy).
    #
    # backoff_limit: K8s retries the pod up to N times before marking
    #   the Job failed (independent of Airflow's task retry_delay).
    #
    # active_deadline_seconds: K8s force-kills the Job if it has not
    #   finished within this wall-clock window (circuit breaker).
    # ------------------------------------------------------------------
    ttl_backoff_job = KubernetesJobOperator(
        task_id="ttl_backoff_job",
        name="af-ttl-backoff-job",
        namespace="airflow",
        image="alpine:3.19",
        cmds=["sh", "-c"],
        arguments=[
            "echo '==> Job with TTL + backoff'; "
            "sleep 2; "
            "echo 'Simulating successful processing'; "
            "echo 'Records processed: 10000'"
        ],
        # K8s-level retry: retry pod up to 2 times before Job fails
        backoff_limit=2,
        # K8s kills this Job if it hasn't finished in 300 s (hard ceiling)
        active_deadline_seconds=300,
        # K8s auto-deletes the Job object 120 s after completion
        # (keeps the API server tidy; pods are also removed)
        ttl_seconds_after_finished=120,
        env_vars={"JOB_TYPE": "ttl-backoff-demo"},
        labels={"managed-by": "airflow", "pattern": "ttl-backoff"},
        wait_until_job_complete=True,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    # ------------------------------------------------------------------
    # Pattern 2 — Parallel batch completion
    #
    # completions=6:   K8s runs 6 pod instances total (the "work units").
    # parallelism=3:   K8s runs at most 3 pods concurrently.
    #
    # K8s restarts failed pods automatically until all 6 succeed.
    # Airflow waits for the Job to reach the "Complete" condition.
    #
    # This is the K8s-native equivalent of a ThreadPoolExecutor(max_workers=3)
    # with 6 tasks — without Airflow needing to manage individual pods.
    # ------------------------------------------------------------------
    parallel_workers_job = KubernetesJobOperator(
        task_id="parallel_workers_job",
        name="af-parallel-workers-job",
        namespace="airflow",
        image="alpine:3.19",
        cmds=["sh", "-c"],
        arguments=[
            # JOB_COMPLETION_INDEX is injected by K8s into each pod
            # when completionMode=Indexed (set via full_job_spec below).
            "SHARD=${JOB_COMPLETION_INDEX:-0}; "
            "echo \"==> Worker shard $SHARD starting\"; "
            "sleep $((RANDOM % 5 + 1)); "
            "echo \"==> Shard $SHARD processed $(( (SHARD + 1) * 1000 )) records\""
        ],
        completions=6,
        parallelism=3,
        # full_job_spec lets you override any field not exposed as a
        # top-level parameter — here we enable Indexed completion mode
        # so each pod receives a unique JOB_COMPLETION_INDEX env var.
        full_job_spec=k8s.V1Job(
            spec=k8s.V1JobSpec(
                completion_mode="Indexed",
                completions=6,
                parallelism=3,
                template=k8s.V1PodTemplateSpec(
                    spec=k8s.V1PodSpec(
                        restart_policy="Never",
                        containers=[
                            k8s.V1Container(
                                name="base",
                                image="alpine:3.19",
                                command=["sh", "-c"],
                                args=[
                                    "SHARD=${JOB_COMPLETION_INDEX:-0}; "
                                    "echo \"==> Worker shard $SHARD starting\"; "
                                    "sleep $((RANDOM % 5 + 1)); "
                                    "echo \"==> Shard $SHARD done\""
                                ],
                                resources=k8s.V1ResourceRequirements(
                                    requests={"memory": "64Mi", "cpu": "100m"},
                                    limits={"memory": "128Mi",  "cpu": "200m"},
                                ),
                            )
                        ],
                    )
                ),
            )
        ),
        wait_until_job_complete=True,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    # ------------------------------------------------------------------
    # Pattern 3 — Init container via full_pod_spec
    #
    # KubernetesJobOperator doesn't expose init_containers as a direct
    # parameter, so we use full_job_spec to inject one.  The init
    # container downloads / prepares data before the main container runs.
    # ------------------------------------------------------------------
    init_container_job = KubernetesJobOperator(
        task_id="init_container_job",
        name="af-init-job",
        namespace="airflow",
        image="alpine:3.19",   # fallback image (overridden by full_job_spec)
        full_job_spec=k8s.V1Job(
            metadata=k8s.V1ObjectMeta(
                labels={"managed-by": "airflow", "pattern": "init-container"},
            ),
            spec=k8s.V1JobSpec(
                backoff_limit=1,
                ttl_seconds_after_finished=180,
                template=k8s.V1PodTemplateSpec(
                    metadata=k8s.V1ObjectMeta(
                        labels={"managed-by": "airflow"},
                    ),
                    spec=k8s.V1PodSpec(
                        restart_policy="Never",
                        # Init containers run sequentially before the main
                        # container.  Good for: config injection, DB schema
                        # checks, secret decryption, or data pre-fetching.
                        init_containers=[
                            k8s.V1Container(
                                name="downloader",
                                image="alpine:3.19",
                                command=["sh", "-c"],
                                args=[
                                    "echo 'Downloading dataset...'; "
                                    "echo 'col_a,col_b,col_c' > /data/input.csv; "
                                    "for i in $(seq 1 5); do "
                                    "  echo \"$i,value_$i,$((i*100))\" >> /data/input.csv; "
                                    "done; "
                                    "echo 'Download complete. Rows:'; "
                                    "wc -l /data/input.csv"
                                ],
                                volume_mounts=[
                                    k8s.V1VolumeMount(
                                        name="data-vol", mount_path="/data"
                                    )
                                ],
                            )
                        ],
                        containers=[
                            k8s.V1Container(
                                name="processor",
                                image="python:3.11-alpine",
                                command=["python", "-c"],
                                args=[
                                    "import csv; "
                                    "rows = list(csv.DictReader(open('/data/input.csv'))); "
                                    "print(f'Processing {len(rows)} rows from init container'); "
                                    "total = sum(int(r['col_c']) for r in rows); "
                                    "print(f'Sum of col_c = {total}')"
                                ],
                                volume_mounts=[
                                    k8s.V1VolumeMount(
                                        name="data-vol", mount_path="/data"
                                    )
                                ],
                                resources=k8s.V1ResourceRequirements(
                                    requests={"memory": "128Mi", "cpu": "100m"},
                                    limits={"memory": "256Mi",  "cpu": "300m"},
                                ),
                            )
                        ],
                        volumes=[
                            k8s.V1Volume(
                                name="data-vol",
                                empty_dir=k8s.V1EmptyDirVolumeSource(),
                            )
                        ],
                    ),
                ),
            ),
        ),
        wait_until_job_complete=True,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    # ------------------------------------------------------------------
    # Pattern 4 — PriorityClass + Prometheus / Datadog annotations
    #
    # PriorityClass controls pod scheduling order under resource pressure.
    # Annotations are passed through to the pod and can be scraped by
    # monitoring agents without changing any cluster-level config.
    #
    # Prerequisite: create the PriorityClass once per cluster:
    #   kubectl apply -f - <<EOF
    #   apiVersion: scheduling.k8s.io/v1
    #   kind: PriorityClass
    #   metadata:
    #     name: batch-medium
    #   value: 500
    #   globalDefault: false
    #   EOF
    # ------------------------------------------------------------------
    priority_annotated_job = KubernetesJobOperator(
        task_id="priority_annotated_job",
        name="af-priority-job",
        namespace="airflow",
        image="python:3.11-alpine",
        cmds=["python", "-c"],
        arguments=[
            "import time, random; "
            "start = time.time(); "
            "total = sum(random.random() for _ in range(100_000)); "
            "elapsed = time.time() - start; "
            "print(f'Monte-Carlo sum={total:.2f}  elapsed={elapsed:.3f}s')"
        ],
        # full_job_spec used only to inject priorityClassName and annotations;
        # all other fields are driven by the top-level parameters above.
        full_job_spec=k8s.V1Job(
            spec=k8s.V1JobSpec(
                ttl_seconds_after_finished=300,
                backoff_limit=2,
                template=k8s.V1PodTemplateSpec(
                    metadata=k8s.V1ObjectMeta(
                        labels={"managed-by": "airflow", "pattern": "priority"},
                        annotations={
                            # Prometheus: scrape metrics from port 8080
                            "prometheus.io/scrape": "true",
                            "prometheus.io/port":   "8080",
                            "prometheus.io/path":   "/metrics",
                            # Datadog: tag this pod's logs and traces
                            "ad.datadoghq.com/tags": '{"team":"data-eng","env":"prod"}',
                        },
                    ),
                    spec=k8s.V1PodSpec(
                        restart_policy="Never",
                        # Route to dedicated batch nodes under pressure
                        priority_class_name="batch-medium",
                        containers=[
                            k8s.V1Container(
                                name="base",
                                image="python:3.11-alpine",
                                command=["python", "-c"],
                                args=[
                                    "import time, random; "
                                    "start = time.time(); "
                                    "total = sum(random.random() for _ in range(100_000)); "
                                    "elapsed = time.time() - start; "
                                    "print(f'Monte-Carlo sum={total:.2f}  elapsed={elapsed:.3f}s')"
                                ],
                                resources=k8s.V1ResourceRequirements(
                                    requests={"memory": "128Mi", "cpu": "200m"},
                                    limits={"memory": "256Mi",  "cpu": "500m"},
                                ),
                            )
                        ],
                    ),
                ),
            )
        ),
        wait_until_job_complete=True,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    @task
    def summarise():
        patterns = [
            "ttl_backoff_job        — TTL cleanup + K8s-level backoffLimit + deadline",
            "parallel_workers_job   — 6 completions / 3 parallel (Indexed mode)",
            "init_container_job     — init container seeds data via shared volume",
            "priority_annotated_job — PriorityClass + Prometheus/Datadog annotations",
        ]
        print("All KubernetesJobOperator advanced patterns completed:")
        for p in patterns:
            print(f"  ✓ {p}")

    ttl_backoff_job >> parallel_workers_job >> init_container_job >> priority_annotated_job >> summarise()
