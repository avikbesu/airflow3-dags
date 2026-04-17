"""
Airflow 3 — 4-task XCom demo
  task_1  : BashOperator    → pushes list via output_processor
  task_2  : EmptyOperator   → no XCom
  task_3  : @task (Python)  → pushes list by returning it
  task_4  : @task (Python)  → pulls from task_1 + task_3, appends, returns combined list

DAG shape:
  task_1 ──► task_2 ──► task_4
  task_3 ──────────────────►┘
"""

from __future__ import annotations

import json

import pendulum
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id="3_xcom_multi_operator_demo",
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
    ## xcom_multi_operator_demo
    Demonstrates XCom passing across BashOperator, EmptyOperator,
    PythonOperator (@task), and a collector task.

    | Task   | Operator       | XCom output                                         |
    |--------|----------------|------------------------------------------------------|
    | task_1 | BashOperator   | `[{"task": "bash task", "result": "success from bash"}]` |
    | task_2 | EmptyOperator  | None                                                 |
    | task_3 | @task (Python) | `[{"task": "py task", "result": "success from python"}]` |
    | task_4 | @task (Python) | Combined list from task_1 + task_3                   |
    """,
) as dag:

    # ──────────────────────────────────────────────────────────────
    # Task 1 — BashOperator
    # BashOperator pushes the LAST LINE of stdout as a plain string.
    # Use output_processor=json.loads to deserialise it into a list
    # so downstream tasks receive a proper Python object, not a string.
    # ──────────────────────────────────────────────────────────────
    task_1 = BashOperator(
        task_id="task_1",
        bash_command='echo \'[{"task": "bash task", "result": "success from bash"}]\'',
        do_xcom_push=True,
        # Airflow 3 / providers-standard: output_processor converts stdout
        # string → Python object before storing in XCom
        output_processor=lambda output: json.loads(output.strip()),
    )

    # ──────────────────────────────────────────────────────────────
    # Task 2 — EmptyOperator (no XCom, acts as a gate / sync point)
    # ──────────────────────────────────────────────────────────────
    task_2 = EmptyOperator(task_id="task_2")

    # ──────────────────────────────────────────────────────────────
    # Task 3 — PythonOperator (@task)
    # Returning a value from a @task function auto-pushes it as
    # return_value XCom (do_xcom_push=True by default).
    # ──────────────────────────────────────────────────────────────
    @task(task_id="task_3")
    def python_task() -> list:
        return [{"task": "py task", "result": "success from python"}]

    # ──────────────────────────────────────────────────────────────
    # Task 4 — Collector
    # Pulls return_value from task_1 and task_3, appends them,
    # and pushes the combined list as its own return_value XCom.
    # task_2 is an upstream but carries no XCom — handled safely.
    # ──────────────────────────────────────────────────────────────
    @task(task_id="task_4")
    def collect_xcoms(ti=None) -> list:
        all_results: list = []

        # ── Pull from task_1 (BashOperator) ─────────────────────
        # output_processor already deserialised it → list
        bash_xcom = ti.xcom_pull(task_ids="task_1")
        if bash_xcom is not None:
            # Normalise: extend if list, append if single item
            all_results.extend(bash_xcom if isinstance(bash_xcom, list) else [bash_xcom])

        # ── Pull from task_2 (EmptyOperator) ────────────────────
        # EmptyOperator never pushes XCom → always None; skip safely
        empty_xcom = ti.xcom_pull(task_ids="task_2")
        if empty_xcom is not None:
            all_results.extend(empty_xcom if isinstance(empty_xcom, list) else [empty_xcom])

        # ── Pull from task_3 (@task Python) ─────────────────────
        py_xcom = ti.xcom_pull(task_ids="task_3")
        if py_xcom is not None:
            all_results.extend(py_xcom if isinstance(py_xcom, list) else [py_xcom])

        print(f"Combined XCom results ({len(all_results)} items):")
        for idx, item in enumerate(all_results, start=1):
            print(f"  [{idx}] {item}")

        return all_results  # pushed as return_value XCom of task_4

    # ──────────────────────────────────────────────────────────────
    # Instantiate callable tasks
    # ──────────────────────────────────────────────────────────────
    task_3_result = python_task()
    task_4_result = collect_xcoms()

    # ──────────────────────────────────────────────────────────────
    # DAG wiring
    #   task_1 ──► task_2 ──► task_4
    #   task_3 ──────────────────►┘
    # ──────────────────────────────────────────────────────────────
    task_1 >> task_2 >> task_4_result
    task_3_result >> task_4_result