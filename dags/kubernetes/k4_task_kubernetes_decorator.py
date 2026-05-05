"""
DAG: k4_task_kubernetes_decorator

Execution method 4 of 4 — @task.kubernetes decorator
=====================================================
The @task.kubernetes decorator (from apache-airflow-providers-cncf-kubernetes)
lets you write a plain Python function and run it *inside* a Kubernetes pod —
no Dockerfile authoring or custom image build pipeline required.

How it works:
  1. Airflow serialises the decorated function + its arguments using pickle.
  2. A pod is launched with the provider's runner image (or your own).
  3. The runner deserialises and executes the function inside the pod.
  4. The return value is serialised back and pushed as XCom automatically.

When to use @task.kubernetes:
  * You want K8s isolation (CPU/memory limits, separate network policy)
    for a Python task without building a custom image.
  * You need to install heavy dependencies (torch, scipy) only for one task
    without bloating the main Airflow worker image.
  * You want to run a task on a GPU node or a node with special hardware.

When NOT to use it:
  * The function depends on a large custom library → build a proper image
    and use KubernetesJobOperator or KubernetesPodOperator instead.
  * You need init containers, sidecars, or complex pod specs →
    use KubernetesPodOperator with full pod_override.

DAG flow:
  fetch_pipeline_config (@task)
        ↓
  run_feature_engineering (@task.kubernetes)   ← runs inside K8s pod
        ↓
  run_model_scoring (@task.kubernetes)         ← runs inside K8s pod
        ↓
  notify_completion (@task)

Requirements:
  - apache-airflow-providers-cncf-kubernetes >= 8.0
  - The Kubernetes pod must be able to pull the image that includes the
    Airflow task runner (set image= to your custom image or use the
    provider's default runner image).
"""

from __future__ import annotations

import pendulum
from airflow.sdk import DAG, task

# @task.kubernetes is registered by the cncf-kubernetes provider as a
# task decorator.  It is accessed via the `task` object just like
# @task.branch, @task.sensor, etc.
# Import path for explicit use (optional — task.kubernetes works after install):
#   from airflow.providers.cncf.kubernetes.decorators.kubernetes import kubernetes_task

with DAG(
    dag_id="k4_task_kubernetes_decorator",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["type=demo", "exec=kube", "subtype=k8s-decorator", "intent=demo"],
    doc_md=__doc__,
) as dag:

    # ------------------------------------------------------------------
    # Step 1 — Normal @task: fetch config from Airflow Variables / conf.
    # Runs on the Airflow worker (no pod).
    # ------------------------------------------------------------------
    @task
    def fetch_pipeline_config() -> dict:
        """Resolve pipeline parameters from DAG run conf or defaults."""
        from airflow.sdk import get_current_context
        ctx = get_current_context()
        conf = (ctx["dag_run"].conf or {}) if ctx.get("dag_run") else {}
        return {
            "dataset":      conf.get("dataset",      "sales_2024"),
            "target_col":   conf.get("target_col",   "revenue"),
            "n_features":   int(conf.get("n_features", 10)),
            "model_type":   conf.get("model_type",   "gradient_boost"),
            "score_cutoff": float(conf.get("score_cutoff", 0.85)),
        }

    # ------------------------------------------------------------------
    # Step 2 — Feature engineering inside a K8s pod.
    #
    # The decorated function runs in the pod; `cfg` is serialised by
    # Airflow and passed in automatically via XCom injection.
    #
    # image: use a Python image that has the packages your function needs.
    #   For production, build a custom image:
    #     FROM python:3.11-slim
    #     RUN pip install apache-airflow-providers-cncf-kubernetes pandas scikit-learn
    #
    # namespace / in_cluster / name: same semantics as KubernetesPodOperator.
    # ------------------------------------------------------------------
    @task.kubernetes(
        image="python:3.11-slim",
        name="af-feature-eng-pod",
        namespace="airflow",
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
        # Resource budget for this specific task
        container_resources={
            "requests": {"memory": "512Mi", "cpu": "500m"},
            "limits":   {"memory": "1Gi",   "cpu": "1000m"},
        },
        env_vars={"PYTHONUNBUFFERED": "1"},
        labels={"managed-by": "airflow", "step": "feature-engineering"},
    )
    def run_feature_engineering(cfg: dict) -> dict:
        """
        Runs INSIDE a Kubernetes pod.
        Install-heavy work (pandas, sklearn) stays out of the worker image.
        """
        import hashlib, time, math

        dataset    = cfg["dataset"]
        n_features = cfg["n_features"]

        print(f"==> Feature engineering: dataset={dataset}, n_features={n_features}")
        start = time.time()

        # Simulate feature matrix construction
        features = {}
        for i in range(n_features):
            col_name = f"feature_{i:02d}"
            # Deterministic fake value derived from dataset + index
            seed = int(hashlib.md5(f"{dataset}_{i}".encode()).hexdigest(), 16) % 10_000
            features[col_name] = round(math.sin(seed) * 100, 4)

        elapsed = round(time.time() - start, 3)
        print(f"==> Built {n_features} features in {elapsed}s")
        print(f"    Sample: {dict(list(features.items())[:3])}")

        return {
            "dataset":       dataset,
            "features":      features,
            "feature_count": n_features,
            "elapsed_s":     elapsed,
        }

    # ------------------------------------------------------------------
    # Step 3 — Model scoring in a second K8s pod.
    #
    # A different resource profile (more CPU for model inference) and a
    # different node selector demonstrate per-task pod customisation.
    # ------------------------------------------------------------------
    @task.kubernetes(
        image="python:3.11-slim",
        name="af-model-scoring-pod",
        namespace="airflow",
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
        # CPU-heavy scoring gets more cores
        container_resources={
            "requests": {"memory": "256Mi", "cpu": "500m"},
            "limits":   {"memory": "512Mi", "cpu": "2000m"},
        },
        # Route scoring pods to compute-optimised nodes if available
        node_selector={"kubernetes.io/os": "linux"},
        env_vars={"PYTHONUNBUFFERED": "1", "OMP_NUM_THREADS": "2"},
        labels={"managed-by": "airflow", "step": "model-scoring"},
    )
    def run_model_scoring(features: dict, cfg: dict) -> dict:
        """
        Runs INSIDE a Kubernetes pod (different resource profile than step 2).
        Receives the feature dict produced by run_feature_engineering via XCom.
        """
        import math, time

        model_type   = cfg["model_type"]
        score_cutoff = cfg["score_cutoff"]
        target_col   = cfg["target_col"]
        feat_values  = list(features["features"].values())

        print(f"==> Scoring: model={model_type}, target={target_col}, cutoff={score_cutoff}")
        start = time.time()

        # Simulate model inference (replace with real model.predict())
        raw_score = abs(math.sin(sum(feat_values))) * 0.4 + 0.6   # always in [0.6, 1.0]
        score     = round(raw_score, 4)
        passed    = score >= score_cutoff

        elapsed = round(time.time() - start, 3)
        print(f"==> Score={score}  passed={passed}  elapsed={elapsed}s")

        return {
            "dataset":     features["dataset"],
            "model_type":  model_type,
            "score":       score,
            "score_cutoff": score_cutoff,
            "passed":      passed,
            "elapsed_s":   elapsed,
        }

    # ------------------------------------------------------------------
    # Step 4 — Normal @task back on the Airflow worker: send notification.
    # ------------------------------------------------------------------
    @task
    def notify_completion(scoring_result: dict) -> None:
        score   = scoring_result["score"]
        passed  = scoring_result["passed"]
        dataset = scoring_result["dataset"]
        model   = scoring_result["model_type"]

        status_icon = "PASS" if passed else "FAIL"
        print("=" * 60)
        print(f" PIPELINE COMPLETE [{status_icon}]")
        print("=" * 60)
        print(f"  Dataset     : {dataset}")
        print(f"  Model       : {model}")
        print(f"  Score       : {score}")
        print(f"  Cutoff      : {scoring_result['score_cutoff']}")
        print(f"  Result      : {'Above cutoff — model approved' if passed else 'Below cutoff — review required'}")
        print("=" * 60)

        if not passed:
            raise ValueError(
                f"Model score {score} is below cutoff {scoring_result['score_cutoff']}. "
                "Pipeline failed quality gate."
            )

    # ------------------------------------------------------------------
    # Wire the flow
    # ------------------------------------------------------------------
    cfg      = fetch_pipeline_config()
    features = run_feature_engineering(cfg)
    scoring  = run_model_scoring(features, cfg)
    notify_completion(scoring)
