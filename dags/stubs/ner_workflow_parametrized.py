from airflow import DAG
from datetime import timedelta

from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowConfigException
from airflow.models.param import Param
from datariver.operators.translate import DeepTranslatorOperator
from datariver.operators.ner import NerOperator
from datariver.operators.elasticsearch import ElasticPushOperator, ElasticSearchOperator
from datariver.operators.stats import NerStatisticsOperator
from datariver.operators.collectstats import (
    SummaryStatsOperator,
    SummaryMarkdownOperator,
)

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
        or "files" not in context["params"]
        or len(context["params"]["files"]) == 0
    ):
        raise AirflowConfigException("No params defined")
    return context["params"]["files"]


with DAG(
    "ner_workflow_parametrized",
    default_args=default_args,
    schedule_interval=None,
    render_template_as_native_obj=True,  # REQUIRED TO RENDER TEMPLATE TO NATIVE LIST INSTEAD OF STRING!!!
    params={
        "files": Param(
            type="array",
        ),
    },
) as dag:
    validate_params_task = PythonOperator(
        task_id="validate_params", python_callable=validate_params
    )

    translate_task = DeepTranslatorOperator(
        task_id="translate",
        files="{{task_instance.xcom_pull('validate_params')}}",
        fs_conn_id=FS_CONN_ID,
        output_dir='{{ "/".join(params["files"][0].split("/")[:-1] + ["translated"]) }}',
        output_language="en",
    )

    ner_task = NerOperator.partial(
        task_id="detect_entities", model="en_core_web_md", fs_conn_id=FS_CONN_ID
    ).expand(
        path=validate_params_task.output.map(get_translated_path)
    )  # .output lets us fetch the return_value of previously executed Operator

    es_push_task = ElasticPushOperator(
        task_id="elastic_push",
        fs_conn_id=FS_CONN_ID,
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
        fs_conn_id=FS_CONN_ID,
        index="ner",
        query={"match_all": {}},
        es_conn_args=ES_CONN_ARGS,
    )

    # summary_task = SummaryStatsOperator(
    #     task_id="summary",
    #     ner_counters="{{task_instance.xcom_pull(task_ids = 'generate_stats', key = 'stats')}}",
    #     translate_stats="{{task_instance.xcom_pull(task_ids = 'translate', key = 'stats')}}",
    #     summary_filename="summary.out",
    #     output_dir='{{ "/".join(task_instance.xcom_pull("validate_params")[0].split("/")[:-1] + ["summary"])}}',
    #     fs_conn_id=FS_CONN_ID,
    # )

    summary_task = SummaryMarkdownOperator(
        task_id="summary",
        summary_filename="summary.md",
        output_dir='{{ "/".join(task_instance.xcom_pull("validate_params")[0].split("/")[:-1] + ["summary"])}}',
        fs_conn_id=FS_CONN_ID,
        # this method works too, might be useful if we pull data with different xcom keys
        # stats="[{{task_instance.xcom_pull(task_ids = 'generate_stats', key = 'stats')}}, {{task_instance.xcom_pull(task_ids = 'translate', key = 'stats')}}]"
        stats="{{task_instance.xcom_pull(task_ids = ['generate_stats','translate'], key = 'stats')}}",
    )

(
    validate_params_task
    >> translate_task
    >> ner_task
    >> stats_task
    >> summary_task
    >> es_push_task
    >> es_search_task
)
