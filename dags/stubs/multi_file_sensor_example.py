from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
import os
from datetime import timedelta
from airflow.hooks.filesystem import FSHook

from datariver.sensors.filesystem import MultipleFilesSensor

FS_CONN_ID = "fs_data"  # id of connection defined in Airflow UI


FILE_NAME = "*.zip"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}


def load_file(ti):
    from time import sleep

    # hook = FSHook(FS_CONN_ID)       # the way to get data from specific fs hook
    # print("Loading file...")
    # sleep(5)
    # print("File loaded: " + os.path.join(hook.get_path(), FILE_NAME))

    print(ti.xcom_pull(key="return_value", task_ids="wait_for_files"))


with DAG(
    "multi_file_sensor_test", default_args=default_args, schedule_interval=None
) as dag:

    wait_for_files = MultipleFilesSensor(
        task_id="wait_for_files",
        #   fs_conn_id="fs_data",          # if you don't specify other fs_conn_id, the default one is fs_data which points to "/"
        fs_conn_id=FS_CONN_ID,
        filepath=FILE_NAME,  # FILEPATH IS RELATIVE TO BASE DIR OF CONNECTION!!!
        poke_interval=30,  # interval between probing if the file with given path exists,
        mode="reschedule",  # frees the worker slot after unsuccessful poke, so other DAGs can run in the meantime
        timeout=timedelta(
            minutes=60
        ),  # marked the task as 'failed' if the file isn't detected in 60 minutes since the first poke
        # deferrable=True
    )

    # wait_for_file = FileSensor(
    #     task_id='wait_for_file',
    #     # fs_conn_id="fs_data",          # if you don't specify other fs_conn_id, the default one is fs_data which points to "/"
    #     fs_conn_id=FS_CONN_ID,
    #     filepath=FILE_NAME,                 # FILEPATH IS RELATIVE TO BASE DIR OF CONNECTION!!!
    #     poke_interval=30,                   # interval between probing if the file with given path exists,
    #     mode="reschedule",                  # frees the worker slot after unsuccessful poke, so other DAGs can run in the meantime
    #     timeout=timedelta(minutes=60),       # marked the task as 'failed' if the file isn't detected in 60 minutes since the first poke
    #     # deferrable=True
    # )

    do_stuff_with_file = PythonOperator(
        task_id="extract_file",
        python_callable=load_file,
    )

wait_for_files >> do_stuff_with_file
