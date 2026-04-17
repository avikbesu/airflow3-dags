"""
Airflow 3 — KubernetesJobOperator XCom demo using a bash (alpine) container.

Flow:
  generate_data_job  -->  process_xcom_result
"""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG, task
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator


with DAG(
    dag_id="1_k8s_job_xcom_bash_demo",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=[
        "type=demo", 
        "exec=kube", 
        "subtype=xcom",
        "intent=utility"
    ],
    doc_md="""
    ## k8s_job_xcom_bash_demo
    Demonstrates XCom passing from a Kubernetes Job (alpine bash container) to
    downstream Airflow tasks using `KubernetesJobOperator`.

    **Requirements:**
    - `do_xcom_push=True`
    - `wait_until_job_complete=True`
    - The container MUST write valid JSON to `/airflow/xcom/return.json`
    """,
) as dag:

    # ------------------------------------------------------------------
    # Step 1 — Run a Kubernetes Job (alpine bash) that writes XCom JSON
    # ------------------------------------------------------------------
    generate_data_job = KubernetesJobOperator(
        task_id="generate_data_job",
        # Job / Pod identity
        name="airflow-bash-xcom-job",          # K8s Job name prefix
        namespace="airflow",                   # must match your Airflow namespace
        image="alpine:3.19",
        # Bash command: simulate work, then write structured JSON to XCom path
        cmds=["sh", "-c"],
        arguments=[
            """
            set -e
            echo "==> Starting job..."

            # Simulate processing work
            RECORD_COUNT=4200
            STATUS="success"
            OUTPUT_PATH="s3://my-bucket/output/run-$(date +%Y%m%d%H%M%S)"
            DURATION_MS=312

            # Create XCom directory (mounted by Airflow sidecar)
            mkdir -p /airflow/xcom

            # Write structured JSON result — this is what Airflow reads back
            cat <<EOF > /airflow/xcom/return.json
{
  "status": "$STATUS",
  "record_count": $RECORD_COUNT,
  "output_path": "$OUTPUT_PATH",
  "duration_ms": $DURATION_MS
}
EOF

            echo "==> XCom written:"
            cat /airflow/xcom/return.json
            echo ""
            echo "==> Job complete."
            """
        ],
        # XCom flags — BOTH required to receive container result
        do_xcom_push=True,
        wait_until_job_complete=True,
        # Behaviour
        get_logs=True,
        # deferrable=True,                       # async/deferrable — best practice in Airflow 3
        # Pod cleanup
        is_delete_operator_pod=True,
        # Resource requests (tune to your cluster)
        container_resources={
            "requests": {"memory": "128Mi", "cpu": "100m"},
            "limits":   {"memory": "256Mi", "cpu": "200m"},
        },
        # Pass an env var into the pod (example)
        env_vars={
            "APP_ENV": "production",
            "JOB_SOURCE": "airflow-dag",
        },
        # Pod labels for observability
        labels={
            "managed-by": "airflow",
            "dag-id":      "k8s_job_xcom_bash_demo",
        },
        # Run in the K8s cluster where Airflow is deployed
        in_cluster=True,
    )

    # ------------------------------------------------------------------
    # Step 2 — Process the XCom result in a downstream @task
    # ------------------------------------------------------------------
    @task
    def process_xcom_result(ti=None):
        raw = ti.xcom_pull(task_ids="generate_data_job")  # returns list or dict

        # KubernetesJobOperator wraps pod results in a list — unwrap safely
        if isinstance(raw, list):
            result = raw[0]   # single-pod job → take first element
        else:
            result = raw      # already a dict (future-proof)

        print(f"Container result  : {result}")
        print(f"  status          : {result['status']}")
        print(f"  record_count    : {result['record_count']}")
        print(f"  output_path     : {result['output_path']}")
        print(f"  duration_ms     : {result['duration_ms']}")

        job_name      = ti.xcom_pull(task_ids="generate_data_job", key="job_name")
        job_namespace = ti.xcom_pull(task_ids="generate_data_job", key="job_namespace")

        print(f"  K8s Job name      : {job_name}")
        print(f"  K8s Job namespace : {job_namespace}")

        return {
            "processed":    True,
            "record_count": result["record_count"],
            "job_name":     job_name,
        }
    
    # ------------------------------------------------------------------
    # Step 3 — Run a Kubernetes Job (alpine bash) that does not write XCom JSON
    # ------------------------------------------------------------------
    empty_data_job = KubernetesJobOperator(
        task_id="empty_data_job",
        # Job / Pod identity
        name="airflow-bash-xcom-job",          # K8s Job name prefix
        namespace="airflow",                   # must match your Airflow namespace
        image="alpine:3.19",
        # Bash command: simulate work, then write structured JSON to XCom path
        cmds=["sh", "-c"],
        arguments=[
            """
            set -e
            echo "==> Starting job..."
            echo "==> Doing nothing:"
            echo "==> Job complete."
            """
        ],
        # XCom flags — BOTH required to receive container result
        do_xcom_push=True,
        wait_until_job_complete=True,
        # Behaviour
        get_logs=True,
        # deferrable=True,                       # async/deferrable — best practice in Airflow 3
        # Pod cleanup
        is_delete_operator_pod=True,
        # Resource requests (tune to your cluster)
        container_resources={
            "requests": {"memory": "128Mi", "cpu": "100m"},
            "limits":   {"memory": "256Mi", "cpu": "200m"},
        },
        # Pass an env var into the pod (example)
        env_vars={
            "APP_ENV": "production",
            "JOB_SOURCE": "airflow-dag",
        },
        # Pod labels for observability
        labels={
            "managed-by": "airflow",
            "dag-id":      "k8s_job_xcom_bash_demo",
        },
        # Run in the K8s cluster where Airflow is deployed
        in_cluster=True,
    )


    generate_data_job >> process_xcom_result()
    empty_data_job