from airflow import DAG

from airflow.operators.python import ShortCircuitOperator
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
    return False


with DAG(
    "playground",
    default_args=default_args,
    schedule_interval=None,
) as dag:
    dummy_start = DummyOperator(task_id="dummy_start")
    sco = ShortCircuitOperator(
        task_id="sco",
        python_callable=eval_my,
        ignore_downstream_trigger_rules=False,  # I dunno if it is useful
    )
    dummy_end = DummyOperator(task_id="dummy_end")
    dummy_end_2 = DummyOperator(
        task_id="dummy_end_2", trigger_rule=TriggerRule.NONE_FAILED
    )
dummy_start >> sco >> dummy_end >> dummy_end_2
