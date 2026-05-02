from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.param import Param
from datariver.operators.common.json_tools import (
    JsonArgs,
    add_post_run_information,
    add_pre_run_information,
)
from datariver.operators.common.elasticsearch import ElasticJsonUpdateOperator
from datariver.operators.texts.langdetect import JsonLangdetectOperator
from datariver.operators.texts.translate import JsonTranslateOperator
from datariver.operators.texts.ner import NerJsonOperator
from datariver.operators.texts.stats import NerJsonStatisticsOperator
from datariver.operators.texts.collectstats import JsonSummaryMarkdownOperator
import common
import os

default_args = common.default_args.copy()
default_args.update({"trigger_rule": TriggerRule.NONE_FAILED})

ES_CONN_ARGS = {
    "hosts": os.environ["ELASTIC_HOST"],
    "ca_certs": "/usr/share/elasticsearch/config/certs/ca/ca.crt",
    "basic_auth": ("elastic", os.environ["ELASTIC_PASSWORD"]),
    "verify_certs": True,
}


def decide_about_translation(ti, **context):
    fs_conn_id = context["params"]["fs_conn_id"]
    json_files_paths = context["params"]["json_files_paths"]
    translation = []
    no_translation = []
    for file_path in json_files_paths:
        json_args = JsonArgs(fs_conn_id, file_path)
        language = json_args.get_value("language")
        if language != "en":
            translation.append(file_path)
        else:
            no_translation.append(file_path)

    branches = []
    if len(translation) > 0:
        branches.append("translate")
        ti.xcom_push(key="json_files_paths_translation", value=translation)
    if len(no_translation) > 0:
        branches.append("detect_entities_without_translation")
        ti.xcom_push(key="json_files_paths_no_translation", value=no_translation)
    return branches


def remove_temp_files(context, result):
    json_files_paths = context["params"]["json_files_paths"]
    for file_path in json_files_paths:
        os.remove(file_path)


with DAG(
    "ner_process",
    default_args=default_args,
    schedule_interval=None,
    # REQUIRED TO RENDER TEMPLATE TO NATIVE LIST INSTEAD OF STRING!!!
    render_template_as_native_obj=True,
    params={
        "json_files_paths": Param(
            type="array",
        ),
        "parent_dag_run_id": Param(type=["null", "string"], default=""),
        "fs_conn_id": Param(type="string", default="fs_data"),
        "encoding": Param(type="string", default="utf-8"),
    },
) as dag:
    add_pre_run_information_task = PythonOperator(
        task_id="add_pre_run_information",
        python_callable=add_pre_run_information,
        provide_context=True,
    )

    detect_language_task = JsonLangdetectOperator(
        task_id="detect_language",
        json_files_paths="{{ params.json_files_paths }}",
        fs_conn_id="{{ params.fs_conn_id }}",
        input_key="content",
        output_key="language",
        encoding="{{ params.encoding }}",
        error_key="error",
    )

    decide_about_translation = BranchPythonOperator(
        task_id="branch", python_callable=decide_about_translation, provide_context=True
    )

    translate_task = JsonTranslateOperator(
        task_id="translate",
        json_files_paths='{{ ti.xcom_pull(task_ids="branch", key="json_files_paths_translation") }}',
        fs_conn_id="{{ params.fs_conn_id }}",
        input_key="content",
        output_key="translated",
        output_language="en",
        encoding="{{ params.encoding }}",
        error_key="error",
    )

    ner_task = NerJsonOperator(
        task_id="detect_entities",
        model="en_core_web_md",
        fs_conn_id="{{params.fs_conn_id}}",
        json_files_paths='{{ ti.xcom_pull(task_ids="branch", key="json_files_paths_translation") }}',
        input_key="translated",
        output_key="ner",
        trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
        encoding="{{ params.encoding }}",
        error_key="error",
    )

    ner_without_translation_task = NerJsonOperator(
        task_id="detect_entities_without_translation",
        model="en_core_web_md",
        fs_conn_id="{{ params.fs_conn_id }}",
        json_files_paths='{{ ti.xcom_pull(task_ids="branch", key="json_files_paths_no_translation") }}',
        input_key="content",
        output_key="ner",
        encoding="{{ params.encoding }}",
        error_key="error",
    )

    stats_task = NerJsonStatisticsOperator(
        task_id="generate_stats",
        json_files_paths="{{ params.json_files_paths }}",
        fs_conn_id="{{ params.fs_conn_id }}",
        input_key="ner",
        output_key="ner_stats",
        encoding="{{ params.encoding }}",
        error_key="error",
    )

    summary_task = JsonSummaryMarkdownOperator(
        task_id="summary",
        summary_filenames='{{ params.json_files_paths|replace(".json",".md") }}',
        fs_conn_id="{{params.fs_conn_id}}",
        # this method works too, might be useful if we pull data with different xcom keys
        # stats="[{{task_instance.xcom_pull(task_ids = 'generate_stats', key = 'stats')}}, {{task_instance.xcom_pull(task_ids = 'translate', key = 'stats')}}]"
        json_files_paths="{{ params.json_files_paths }}",
        input_key="ner_stats",
        encoding="{{ params.encoding }}",
        error_key="error",
    )

    add_post_run_information_task = PythonOperator(
        task_id="add_post_run_information",
        python_callable=add_post_run_information,
        provide_context=True,
    )

    es_update_task = ElasticJsonUpdateOperator(
        task_id="elastic_update",
        fs_conn_id="{{ params.fs_conn_id }}",
        json_files_paths="{{ params.json_files_paths }}",
        index="ner",
        es_conn_args=ES_CONN_ARGS,
        encoding="{{ params.encoding }}",
        post_execute=remove_temp_files,
    )


(
    add_pre_run_information_task
    >> detect_language_task
    >> decide_about_translation
    >> [translate_task, ner_without_translation_task]
)
translate_task >> ner_task
(
    [ner_without_translation_task, ner_task]
    >> stats_task
    >> summary_task
    >> add_post_run_information_task
    >> es_update_task
)
