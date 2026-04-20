"""
Consolidated Airflow 3 Examples DAG - Set 2
===========================================
All example DAGs merged into one, each wrapped in a TaskGroup.
Groups run independently (no cross-group dependencies).

Groups:
  - trigger_rules     : All TriggerRule variants demo
"""

import random
import string
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.sdk.bases.operator import chain, cross_downstream
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sdk import TaskGroup

# ---------------------------------------------------------------------------
# Shared callables
# ---------------------------------------------------------------------------

def success_function():
    print("Task succeeded!")
    return "Success"


def failure_function():
    print("Task failed!")
    raise Exception("Intentional failure")


def random_outcome():
    if random.choice([True, False]):
        print("Random success")
        return "Success"
    raise Exception("Random failure")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="airflow_basics_trigger_rules",
    default_args=default_args,
    description="All Airflow concept examples consolidated into one DAG via TaskGroups",
    start_date=datetime(2025, 9, 27),
    schedule=None,
    catchup=False,
    tags=[
        "type=example",
        "exec=kube", "exec=compose",
        "subtype=trigger_rules",
        "intent=utility"
    ],
) as dag:

    # -----------------------------------------------------------------------
    # Group 1 — trigger rules showcase
    # -----------------------------------------------------------------------
    with TaskGroup("trigger_rules") as tg_triggers:
        tr_start = EmptyOperator(task_id="start")

        tr_success_1 = PythonOperator(task_id="task_success_1", python_callable=success_function)
        tr_success_2 = PythonOperator(task_id="task_success_2", python_callable=success_function)
        tr_fail_1    = PythonOperator(task_id="task_fail_1",    python_callable=failure_function)
        tr_fail_2    = PythonOperator(task_id="task_fail_2",    python_callable=failure_function)

        upstream_tasks = [tr_success_1, tr_success_2, tr_fail_1, tr_fail_2]

        tr_all_success   = PythonOperator(task_id="trigger_all_success",  python_callable=success_function, trigger_rule=TriggerRule.ALL_SUCCESS)
        tr_all_failed    = PythonOperator(task_id="trigger_all_failed",   python_callable=success_function, trigger_rule=TriggerRule.ALL_FAILED)
        tr_all_done      = PythonOperator(task_id="trigger_all_done",     python_callable=success_function, trigger_rule=TriggerRule.ALL_DONE)
        tr_one_success   = PythonOperator(task_id="trigger_one_success",  python_callable=success_function, trigger_rule=TriggerRule.ONE_SUCCESS)
        tr_one_failed    = PythonOperator(task_id="trigger_one_failed",   python_callable=success_function, trigger_rule=TriggerRule.ONE_FAILED)
        tr_none_failed   = PythonOperator(task_id="trigger_none_failed",  python_callable=success_function, trigger_rule=TriggerRule.NONE_FAILED)
        tr_none_skipped  = PythonOperator(task_id="trigger_none_skipped", python_callable=success_function, trigger_rule=TriggerRule.NONE_SKIPPED)

        trigger_tasks = [
            tr_all_success, tr_all_failed, tr_all_done,
            tr_one_success, tr_one_failed, tr_none_failed, tr_none_skipped,
        ]

        tr_end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

        tr_start >> upstream_tasks
        for t in trigger_tasks:
            upstream_tasks >> t
        trigger_tasks >> tr_end
    
    tg_triggers