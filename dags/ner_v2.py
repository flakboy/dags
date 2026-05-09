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
            filename = s3.download_file(
                key=input_file_name,
                bucket_name=INPUT_BUCKET,
                local_path=local_input_path,
                preserve_file_name=True
            )

            print(f"Downloaded file: {filename}")

            input_path = os.path.join(tmp_dir, filename)
            output_keys = []

            timestamp = datetime.now(UTC).isoformat().split(".")[0]

            with open(input_path, "rb") as f:
                for index, record in enumerate(ijson.items(f, "item")):
                    logger.info(record)

                    with tempfile.NamedTemporaryFile(
                        dir=tmp_dir,
                        delete_on_close=False
                    ) as tmp_file:
                        tmp_file.write(json.dumps(record).encode("utf-8"))
                        tmp_file.close()

                        output_key = f"dr_process_files/{timestamp}/{Path(tmp_file.file).name}-{index}"

                        s3.load_file(
                            filename=Path(tmp_file).absolute,
                            key=output_key,
                            bucket_name=OUTPUT_BUCKET,
                            replace=True,
                        )

                        print(f"Wysłano plik: {output_key}")
                        output_keys.append(output_key)

                        tmp_file.delete()

            return output_keys

    # @task
    # def process_single_file(file_name: str):
    #     def processFile(file_name: str):
    #         """
    #         Właściwe przetwarzanie pliku.
    #         """

    #         print(f"Przetwarzam plik: {file_name}")


    #     print(f"Start przetwarzania: {file_name}")

    #     # Worker zna konkretną nazwę pliku
    #     processFile(file_name)

    #     print(f"Koniec przetwarzania: {file_name}")


    split_result = split_input_file()

    # process_single_file.expand(
    #     file_name=split_result
    # )



dag_instance = dynamic_s3_json_processing()