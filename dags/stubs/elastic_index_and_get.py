import os

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

from datariver.operators.elasticsearch import (
    ElasticJsonPushOperator,
    ElasticSearchOperator,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "trigger_rule": TriggerRule.NONE_FAILED,
}


ES_CONN_ARGS = {
    "hosts": os.environ["ELASTIC_HOST"],
    "ca_certs": "/usr/share/elasticsearch/config/certs/ca/ca.crt",
    "basic_auth": ("elastic", os.environ["ELASTIC_PASSWORD"]),
    "verify_certs": True,
}

with DAG(
    "elastic_index_and_get",
    default_args=default_args,
    schedule_interval=None,
    render_template_as_native_obj=True,
) as dag:
    elastic_push = ElasticJsonPushOperator(
        task_id="elastic_push",
        fs_conn_id="fs_data",
        json_file_path="test wrong.json",
        input_key="key",
        index="test",
        es_conn_args=ES_CONN_ARGS,
    )

    elastic_get = ElasticSearchOperator(
        task_id="elastic_get",
        index="test",
        query={"term": {"_id": "{{task_instance.xcom_pull('elastic_push')['_id']}}"}},
        es_conn_args=ES_CONN_ARGS,
    )

elastic_push >> elastic_get
