import os
import validators
import base64
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datariver.operators.common.elasticsearch import ElasticJsonPushOperator
from datariver.operators.common.json_tools import MapJsonFile
import common

ES_CONN_ARGS = {
    "hosts": os.environ["ELASTIC_HOST"],
    "ca_certs": "/usr/share/elasticsearch/config/certs/ca/ca.crt",
    "basic_auth": ("elastic", os.environ["ELASTIC_PASSWORD"]),
    "verify_certs": True,
}


def map_paths(paths, **context):
    batch_size = context["params"]["batch_size"]

    def create_conf(json_paths, start_index):
        return {
            "fs_conn_id": context["params"]["fs_conn_id"],
            "json_files_paths": json_paths[start_index : start_index + batch_size],
            "parent_dag_run_id": context["dag_run"].run_id,
        }

    clear_paths = [path for path in paths if path is not None]
    return [create_conf(clear_paths, i) for i in range(0, len(clear_paths), batch_size)]


def copy_item_to_file(item, context):
    import json
    from airflow.hooks.filesystem import FSHook

    hook = FSHook(context["params"]["fs_conn_id"])
    input_path = context["params"]["path"]
    run_id = context["dag_run"].run_id
    dag_id = context["dag_run"].dag_id
    date = context["dag_run"].start_date.replace(microsecond=0).isoformat()
    print(item)
    dir_path = os.path.join(
        hook.get_path(),
        os.path.dirname(input_path),
        context["ti"].run_id,
    )
    os.makedirs(dir_path, exist_ok=True)
    if validators.url(item):
        filename = f"{base64.b64encode(item.encode("utf-8")).decode("utf-8")}.json"
    else:
        item = f"../{item}"
        filename = f'{os.path.basename(item).split(".")[0]}.json'
    full_path = os.path.join(dir_path, filename)

    with open(full_path, "w") as file:
        file.write(
            json.dumps(
                {
                    "image_path": item,
                    "dags_info": {dag_id: {"start_date": date, "run_id": run_id}},
                },
                indent=2,
            )
        )
    return full_path


with DAG(
    "image_transform_dataset",
    default_args=common.default_args,
    schedule_interval=None,
    render_template_as_native_obj=True,
    params={
        "fs_conn_id": Param(type="string", default="fs_data"),
        "path": Param(
            type="string",
        ),
        "parent_dag_run_id": Param(type=["null", "string"], default=""),
        "batch_size": Param(type="integer", default="10"),
    },
) as dag:
    map_task = MapJsonFile(
        task_id="map_json",
        fs_conn_id="{{params.fs_conn_id}}",
        path="{{params.path}}",
        python_callable=copy_item_to_file,
    )

    create_confs_task = PythonOperator(
        task_id="create_confs",
        python_callable=map_paths,
        op_kwargs={"paths": "{{ task_instance.xcom_pull(task_ids='map_json') }}"},
    )

    es_push_task = ElasticJsonPushOperator.partial(
        task_id="elastic_push",
        fs_conn_id="{{ params.fs_conn_id }}",
        index="image_processing",
        es_conn_args=ES_CONN_ARGS,
    ).expand(
        json_files_paths=create_confs_task.output.map(
            lambda x: x.get("json_files_paths", [])
        )
    )

    trigger_images_workflow_task = TriggerDagRunOperator.partial(
        task_id="trigger_images_workflow",
        trigger_dag_id="image_process",
    ).expand(conf=create_confs_task.output)

map_task >> create_confs_task >> es_push_task >> trigger_images_workflow_task
