"""
Consolidated Airflow 3 Examples DAG - Set 1
===========================================
All example DAGs merged into one, each wrapped in a TaskGroup.
Groups run independently (no cross-group dependencies).

Groups:
  - cross_downstream  : cross_downstream() utility demo
  - chain_dep         : chain() linear dependency demo
  - parallel_chain    : chain() with parallel branches demo
  - <add more here>
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
    dag_id="airflow_basics_downtreams",
    default_args=default_args,
    description="All Airflow concept examples consolidated into one DAG via TaskGroups",
    start_date=datetime(2025, 9, 27),
    schedule=None,
    catchup=False,
    tags=[
        "type=example",
        "exec=kube", "exec=compose",
        "subtype=trigger_rules"
    ],
) as dag:

    # -----------------------------------------------------------------------
    # Group 1 — cross_downstream
    # -----------------------------------------------------------------------
    with TaskGroup("cross_downstream") as tg_cross:
        cd_start = EmptyOperator(task_id="start")
        cd_a = EmptyOperator(task_id="task_a")
        cd_b = EmptyOperator(task_id="task_b")
        cd_d = EmptyOperator(task_id="task_d")
        cd_e = EmptyOperator(task_id="task_e")
        cd_end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

        cd_start >> [cd_a, cd_b]
        cross_downstream(from_tasks=[cd_a, cd_b], to_tasks=[cd_d, cd_e])
        [cd_d, cd_e] >> cd_end

    # -----------------------------------------------------------------------
    # Group 2 — chain linear dependency
    # -----------------------------------------------------------------------
    with TaskGroup("chain_dep") as tg_chain:
        chain_tasks = [
            EmptyOperator(task_id=f"task_{letter}")
            for letter in string.ascii_lowercase[:5]   # task_a … task_e
        ]
        chain(*chain_tasks)

    # -----------------------------------------------------------------------
    # Group 3 — parallel chain (pairwise branches)
    # -----------------------------------------------------------------------
    with TaskGroup("parallel_chain") as tg_parallel:
        pc_start = EmptyOperator(task_id="start")
        first_branch  = [EmptyOperator(task_id=name) for name in ("task_a", "task_b")]
        second_branch = [EmptyOperator(task_id=name) for name in ("task_c", "task_d")]
        pc_end = EmptyOperator(task_id="end")

        # task_start >> [task_a, task_b]
        # task_a >> task_c  /  task_b >> task_d
        # [task_c, task_d] >> task_end
        chain(pc_start, first_branch, second_branch, pc_end)

    # -----------------------------------------------------------------------
    # Add more groups here following the same pattern:
    #
    # with TaskGroup("my_new_example") as tg_new:
    #     ...
    # -----------------------------------------------------------------------
    
    tg_cross >> tg_chain >> tg_parallel