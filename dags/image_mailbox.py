from datetime import timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datariver.sensors.filesystem import MultipleFilesSensor
import common


def parse_paths(paths, **context):
    def create_conf(path):
        return {
            "path": path,
            "batch_size": context["params"]["batch_size"],
            "parent_dag_run_id": context["dag_run"].run_id,
        }

    paths_list = paths.split(",")

    return list(map(create_conf, paths_list))


with DAG(
    "image_mailbox",
    default_args=common.default_args,
    schedule_interval=None,
    render_template_as_native_obj=True,
    params={
        "fs_conn_id": Param(type="string", default="fs_data"),
        "filepath": Param(type="string", default="image_mailbox/*.json"),
        "batch_size": Param(type="integer", default=10),
    },
) as dag:
    detect_files_task = MultipleFilesSensor(
        task_id="wait_for_files",
        fs_conn_id="{{params.fs_conn_id}}",
        filepath="{{params.filepath}}",
        poke_interval=5,
        mode="reschedule",
        timeout=timedelta(minutes=60),
    )

    move_files_task = BashOperator(
        task_id="move_files",
        bash_command="""
            IFS=","
            first="true"
            # value pulled from xcom is in format ['file_1', 'file_2']
            # sed explanation:
            # - remove [' from the begginig
            # - replace ', ' with , everywhere
            # - remove '] from the end
            for file in $(echo "{{ ti.xcom_pull(task_ids="wait_for_files") }}" | sed "s/^\['//;s/', '/,/g;s/'\]$//") # noqa W605
            do
                # move detected files from mailbox to folder where processing will happen
                base_dir="$(dirname "$file")"
                filename="$(basename "$file")"
                # build folder name based on unique run_id
                dest="$base_dir/{{run_id}}"
                mkdir -p "$dest" && mv "$file" "$dest"
                if [[ "$first" == "true" ]]; then
                    first="false"
                    echo -n "$dest/$filename"
                else
                    echo -n ",$dest/$filename"
                fi
            done
        """,
        do_xcom_push=True,
    )

    parse_paths_task = PythonOperator(
        task_id="parse_paths",
        python_callable=parse_paths,
        op_kwargs={"paths": "{{ task_instance.xcom_pull(task_ids='move_files')}}"},
    )

    trigger_map_file_task = TriggerDagRunOperator.partial(
        task_id="trigger_map_file",
        trigger_dag_id="image_transform_dataset",
    ).expand(conf=parse_paths_task.output)

    trigger_mailbox = TriggerDagRunOperator(
        task_id="trigger_mailbox",
        trigger_dag_id="image_mailbox",
        conf="{{ params }}",  # noqa
    )


(
    detect_files_task
    >> move_files_task
    >> parse_paths_task
    >> trigger_mailbox
    >> trigger_map_file_task
)
