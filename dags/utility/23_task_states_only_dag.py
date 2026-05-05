from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime


@task
def check_states_only():
    context = get_current_context()
    ti = context["ti"]

    task_states = ti.get_task_states(
        dag_id=ti.dag_id,
        run_ids=[ti.run_id],
        task_ids=["task_a", "task_b", "task_c"],
    )
    print(task_states)
    return task_states


@dag(
    dag_id="23_task_states_only_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
)
def airflow3_task_states_only_dag():
    start = EmptyOperator(task_id="start")
    task_a = EmptyOperator(task_id="task_a")
    task_b = EmptyOperator(task_id="task_b")
    task_c = EmptyOperator(task_id="task_c")
    check = check_states_only()
    end = EmptyOperator(task_id="end")

    start >> [task_a, task_b] >> task_c >> check >> end


airflow3_task_states_only_dag()