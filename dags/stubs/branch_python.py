from airflow import DAG

from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}


def eval_my():
    return "dummy_end_2"


with DAG(
    "branch_python",
    default_args=default_args,
    schedule_interval=None,
) as dag:
    dummy_start = DummyOperator(task_id="dummy_start")
    branch = BranchPythonOperator(task_id="branch", python_callable=eval_my)
    dummy_end = DummyOperator(task_id="dummy_end")
    dummy_end_2 = DummyOperator(task_id="dummy_end_2")
    dummy_end_3 = DummyOperator(
        task_id="dummy_end_3", trigger_rule=TriggerRule.NONE_FAILED
    )
dummy_start >> branch >> [dummy_end, dummy_end_2] >> dummy_end_3
