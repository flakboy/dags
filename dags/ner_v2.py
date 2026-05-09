from __future__ import annotations

import json
import ijson
import os
import tempfile
import logging
import datetime

from datetime import datetime, UTC
from pathlib import Path

from airflow.sdk import dag, task
from airflow.providers.standard.hooks.filesystem import FSHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


AWS_CONN_ID = "dr-s3"
INPUT_BUCKET = "airflow-input"
OUTPUT_BUCKET = "airflow-output"


@dag(
    dag_id="dr_process_files",
    # start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "input_file_name": "texts_small.json"
    },
)
def dynamic_s3_json_processing():
    @task
    def split_input_file(**context) -> list[str]:

        logger = logging.getLogger(__name__)

        input_file_name = context["params"]["input_file_name"]
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)

        with tempfile.TemporaryDirectory() as tmp_dir:
            local_input_path = os.path.join(tmp_dir)
            print(f"Saving file to directory {local_input_path}")
            downloaded_file_path = s3.download_file(
                key=input_file_name,
                bucket_name=INPUT_BUCKET,
                local_path=local_input_path,
                preserve_file_name=True
            )

            print(f"Downloaded file: {downloaded_file_path}")

            input_path = os.path.join(tmp_dir, downloaded_file_path)
            file_name = Path(input_path).stem
            file_ext = "".join(Path("/path/to/file.txt").suffixes)
            output_keys = []

            timestamp = datetime.now(UTC).isoformat().split(".")[0]

            with open(input_path, "rb") as f:
                f.name
                for index, record in enumerate(ijson.items(f, "item")):
                    logger.info(record)

                    with tempfile.NamedTemporaryFile(
                        dir=tmp_dir,
                        delete_on_close=False
                    ) as tmp_file:
                        tmp_file.write(json.dumps(record).encode("utf-8"))
                        tmp_file.close()

                        output_key = f"dr_process_files/{timestamp}/intermediate/{file_name}-{index}{file_ext}"


                        logger.info(f"Sending {tmp_file.name} as {output_key}")
                        s3.load_file(
                            filename=tmp_file.name,
                            key=output_key,
                            bucket_name=OUTPUT_BUCKET,
                            replace=True,
                        )
                        output_keys.append(output_key)
                        logger.info(f"Key {output_key} loaded sucessfully")

        return output_keys

    @task
    def process_single_file(file_name: str):
        import time
        logger = logging.getLogger(__name__)

        with tempfile.TemporaryDirectory() as tmp_dir:
            local_input_path = os.path.join(tmp_dir)
            s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
            downloaded_file_path = s3.download_file(
                key=file_name,
                bucket_name=OUTPUT_BUCKET,
                local_path=local_input_path,
                preserve_file_name=True
            )

            with open(downloaded_file_path, "r") as f:
                # read contents of file into memory in order to be able to observe the data from outside of the process
                data = downloaded_file_path.read()
                # simulate data processing...
                time.sleep(40)
                # printing the data just in case Python interpeter would optimize the variable away
                logger.info(f"Read content of file {file_name}: {data}")


    split_result = split_input_file()

    process_single_file.expand(
        file_name=split_result
    )



dag_instance = dynamic_s3_json_processing()