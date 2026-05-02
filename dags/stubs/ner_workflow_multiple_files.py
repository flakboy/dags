from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowConfigException
from airflow.models.param import Param
from datariver.operators.translate import DeepTranslatorOperator
from datariver.operators.ner import NerOperator
from datariver.operators.elasticsearch import ElasticPushOperator, ElasticSearchOperator
from datariver.operators.stats import NerStatisticsOperator
from datariver.operators.collectstats import SummaryMarkdownOperator

import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}


def get_translated_path(path):
    parts = path.split("/")
    if len(parts) < 1:
        return path
    return "/".join(parts[:-1] + ["translated"] + parts[-1:])


FS_CONN_ID = "fs_data"  # id of connection defined in Airflow UI
FILE_NAME = "ner/*.txt"
ES_CONN_ARGS = {
    "hosts": os.environ["ELASTIC_HOST"],
    "ca_certs": "/usr/share/elasticsearch/config/certs/ca/ca.crt",
    "basic_auth": ("elastic", os.environ["ELASTIC_PASSWORD"]),
    "verify_certs": True,
}


def validate_params(**context):
    if (
        "params" not in context
        or "file_path" not in context["params"]
        or "fs_conn_id" not in context["params"]
    ):
        raise AirflowConfigException("No params defined")


with DAG(
    "ner_workflow_multiple_files",
    default_args=default_args,
    schedule_interval=None,
    render_template_as_native_obj=True,  # REQUIRED TO RENDER TEMPLATE TO NATIVE LIST INSTEAD OF STRING!!!
    params={
        "file_path": Param(
            type="string",
        ),
        "fs_conn_id": Param(type="string", default="fs_data"),
    },
) as dag:
    validate_params_task = PythonOperator(
        task_id="validate_params", python_callable=validate_params
    )

    translate_path_task = PythonOperator(
        task_id="translate_path",
        python_callable=get_translated_path,
        op_kwargs={"path": "{{params.file_path}}"},
    )

    translate_task = DeepTranslatorOperator(
        task_id="translate",
        file_path="{{params.file_path}}",
        fs_conn_id="{{params.fs_conn_id}}",
        translated_file_path="{{task_instance.xcom_pull('translate_path')}}",
        output_language="en",
    )

    ner_task = NerOperator(
        task_id="detect_entities",
        model="en_core_web_md",
        fs_conn_id="{{params.fs_conn_id}}",
        path="{{task_instance.xcom_pull('translate_path')}}",
    )

    es_push_task = ElasticPushOperator(
        task_id="elastic_push",
        fs_conn_id="{{params.fs_conn_id}}",
        index="ner",
        document={},
        es_conn_args=ES_CONN_ARGS,
        pre_execute=lambda self: setattr(
            self["task"],
            "document",
            {"document": list(self["task_instance"].xcom_pull("detect_entities"))},
        ),
    )

    stats_task = NerStatisticsOperator(
        task_id="generate_stats",
        json_data="{{task_instance.xcom_pull('detect_entities')}}",
    )

    es_search_task = ElasticSearchOperator(
        task_id="elastic_get",
        fs_conn_id="{{params.fs_conn_id}}",
        index="ner",
        query={"match_all": {}},
        es_conn_args=ES_CONN_ARGS,
    )

    summary_task = SummaryMarkdownOperator(
        task_id="summary",
        summary_filename="summary.md",
        output_dir='{{ "/".join(params["file_path"].split("/")[:-1] + ["summary"])}}',
        fs_conn_id="{{params.fs_conn_id}}",
        # this method works too, might be useful if we pull data with different xcom keys
        # stats="[{{task_instance.xcom_pull(task_ids = 'generate_stats', key = 'stats')}}, {{task_instance.xcom_pull(task_ids = 'translate', key = 'stats')}}]"
        stats="{{task_instance.xcom_pull(task_ids = ['generate_stats','translate'], key = 'stats')}}",
    )

(
    validate_params_task
    >> translate_path_task
    >> translate_task
    >> ner_task
    >> stats_task
    >> summary_task
    >> es_push_task
    >> es_search_task
)
