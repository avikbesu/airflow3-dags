from airflow.sdk import dag, task, Variable
from datetime import datetime

def _is_sensitive(key: str) -> bool:
    sensitive_keywords = {"password", "secret", "token", "key", "passwd", "credential"}
    return any(kw in key.lower() for kw in sensitive_keywords)

@dag(
    dag_id="5_print_a_variable",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=[
        "type=demo",
        "exec=kube", "exec=compose",
        "subtype=variables",
        "intent=utility"
    ]
)
def print_known_variables():

    @task
    def print_variables():
        known_keys = ["env", "s3_bucket", "db_host", "EXEC_ENV", "Another_password", "Another_pass"]

        for key in known_keys:
            value = Variable.get(key, default="<not set>")  # ← default, not default_var
            masked = "***MASKED***" if _is_sensitive(key) else value
            print(f"  {key}: {masked}")

    print_variables()

print_known_variables()