from airflow.sdk import dag, get_current_context, task
from datetime import datetime

@dag(
    dag_id="2_monitor_dag", 
    start_date=datetime(2025, 1, 1), 
    schedule=None, 
    catchup=False,
    tags=[
        "type=demo", 
        "exec=kube", 
        "subtype=dag-status",
        "intent=utility"
    ]
)
def monitor_dag():

    @task(task_id="2_log_other_dag_status")
    def log_status():
        context = get_current_context()
        ti = context["ti"]

        # Check the latest run_id of the target DAG
        # get_dagrun_state returns the state for a given dag_id + run_id
        target_dag_id = "1_k8s_job_xcom_bash_demo"
        target_run_id = "manual__2026-03-05T14:03:25.737541+00:00"  # or pass dynamically

        state = ti.get_dagrun_state(dag_id=target_dag_id, run_id=target_run_id)
        dr_count = ti.get_dr_count(dag_id=target_dag_id)

        print(f"[INFO] DAG '{target_dag_id}' run state: {state}")
        print(f"[INFO] Total runs for '{target_dag_id}': {dr_count}")
        return state

    log_status()

monitor_dag()
