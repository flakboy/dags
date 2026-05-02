import json
import ijson
import os
import builtins
import fcntl
import datetime
import requests
from urllib.request import urlopen
import validators
import cv2
import numpy as np
from PIL import Image
from io import BytesIO
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.filesystem import FSHook
from airflow.utils.log.logging_mixin import LoggingMixin
from typing import Callable, Any
from airflow.utils.context import Context


class MapJsonFile(BaseOperator):
    template_fields = ("fs_conn_id", "path")

    def __init__(
        self,
        *,
        fs_conn_id="fs_data",
        path,
        python_callable: Callable[[dict, Context], Any],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.path = path
        self.fs_conn_id = fs_conn_id
        self.python_callable = python_callable

    def execute(self, context):
        hook = FSHook(self.fs_conn_id)
        basepath = hook.get_path()

        full_path = os.path.join(basepath, self.path)

        mapped = []
        with open(full_path, "rb") as f:
            for record in ijson.items(f, "item"):

                mapped.append(self.python_callable(record, context))

        return mapped


# I see here a huge room for improvement - many fields from operators working with json may have common fields described here?
class JsonArgs(LoggingMixin):
    def __init__(self, fs_conn_id, json_file_path, encoding="utf-8", **kwargs):
        super().__init__(**kwargs)
        self.fs_conn_id = fs_conn_id
        self.json_file_path = json_file_path
        self.encoding = encoding

    def hook(self):
        return FSHook(self.fs_conn_id)

    def get_base_path(self):
        return self.hook().get_path()

    def get_full_path(self):
        return os.path.join(self.get_base_path(), self.json_file_path)

    def get_value(self, key):
        value = None
        try:
            with open(self.get_full_path(), "r", encoding=self.encoding) as file:
                fcntl.flock(file.fileno(), fcntl.LOCK_SH)
                data = json.load(file)
                fcntl.flock(file.fileno(), fcntl.LOCK_UN)
                value = data.get(key, None)
                if value is None:
                    self.log.info(f"{self.get_full_path()} does not contain key {key}!")
        except IOError as e:
            self.log.error(f"Couldn't open {self.get_full_path()} ({str(e)})!")
        return value

    def add_value(self, key, value):
        try:
            with open(self.get_full_path(), "r+", encoding=self.encoding) as file:
                fcntl.flock(file.fileno(), fcntl.LOCK_EX)
                data = json.load(file)
                file.seek(0)
                file.truncate(0)
                data[key] = value
                json.dump(data, file, ensure_ascii=False, indent=2)
                fcntl.flock(file.fileno(), fcntl.LOCK_UN)
        except IOError as e:
            self.log.error(f"Couldn't open {self.get_full_path()} ({str(e)})!")

    def get_values(self, keys):
        value = None
        values = {}
        try:
            with open(self.get_full_path(), "r", encoding=self.encoding) as file:
                fcntl.flock(file.fileno(), fcntl.LOCK_SH)
                data = json.load(file)
                fcntl.flock(file.fileno(), fcntl.LOCK_UN)
                for key in keys:
                    value = data.get(key, None)
                    if value is None:
                        self.log.error(
                            f"{self.get_full_path()} does not contain key {key}!"
                        )
                    else:
                        values[key] = value
        except IOError as e:
            raise RuntimeError(f"Couldn't open {self.get_full_path()} ({str(e)})!")
        return values

    def get_keys(self):
        keys = []
        try:
            with open(self.get_full_path(), "r", encoding=self.encoding) as file:
                fcntl.flock(file.fileno(), fcntl.LOCK_SH)
                data = json.load(file)
                fcntl.flock(file.fileno(), fcntl.LOCK_UN)
                keys = data.keys()
        except IOError as e:
            self.log.error(f"Couldn't open {self.get_full_path()} ({str(e)})!")
        return keys

    def add_or_update(self, key, value):
        old_value = self.get_value(key)
        if old_value is None:
            self.add_value(key, value)
            return
        elif type(old_value) is not type(value):
            self.add_value(key, value)
            self.log.info(
                f"updating {key} with new value type, new value: {value}, old value: {old_value}"
            )
            return
        match type(old_value):
            case builtins.list:
                value.extend(old_value)
            case builtins.dict:
                value.update(old_value)
        self.add_value(key, value)

    def remove_value(self, key):
        try:
            with open(self.get_full_path(), "r+", encoding=self.encoding) as file:
                fcntl.flock(file.fileno(), fcntl.LOCK_EX)
                data = json.load(file)
                file.seek(0)
                file.truncate(0)
                data.pop(key, None)
                json.dump(data, file, ensure_ascii=False, indent=2)
                fcntl.flock(file.fileno(), fcntl.LOCK_UN)
        except IOError as e:
            self.log.error(f"Couldn't open {self.get_full_path()} ({str(e)})!")

    def get_PIL_image(self, key):
        image_path = self.get_value(key)
        if validators.url(image_path):
            result = requests.get(image_path)
            if result.status_code != 200:
                return None
            image_content = BytesIO(result.content)
        else:
            image_content = self.generate_absolute_path(
                self.get_full_path(), image_path
            )
        print(image_path)
        image = Image.open(image_content)
        return image

    def get_cv2_image(self, key):
        image_path = self.get_value(key)
        if validators.url(image_path):
            try:
                result = urlopen(image_path)
            except Exception:
                return None
            if result.code != 200:
                return None
            arr = np.asarray(bytearray(result.read()), dtype=np.uint8)
            image = cv2.imdecode(arr, -1)
        else:
            image_content = self.generate_absolute_path(
                self.get_full_path(), image_path
            )
            image = cv2.imread(image_content)
        if len(image.shape) == 3:
            image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        return image

    @staticmethod
    def generate_absolute_path(base_path: str, path: str) -> str:
        return os.path.normpath(os.path.join(os.path.dirname(base_path), path))


def add_pre_run_information(**context):
    fs_conn_id = context["params"]["fs_conn_id"]
    json_files_paths = context["params"]["json_files_paths"]
    date = context["dag_run"].start_date.replace(microsecond=0).isoformat()
    run_id = context["dag_run"].run_id
    dag_id = context["dag_run"].dag_id
    for file_path in json_files_paths:
        json_args = JsonArgs(fs_conn_id, file_path)
        json_args.add_or_update(
            "dags_info", {dag_id: {"start_date": date, "run_id": run_id}}
        )


def add_post_run_information(**context):
    fs_conn_id = context["params"]["fs_conn_id"]
    json_files_paths = context["params"]["json_files_paths"]
    date = datetime.datetime.now().replace(microsecond=0).isoformat()
    for file_path in json_files_paths:
        json_args = JsonArgs(fs_conn_id, file_path)
        json_args.add_value("processed_date", date)


# helper functions to use in preexecute
def _filter_errors(context, exclude):
    task = context["task"]
    result = [
        json_file
        for json_file in task.json_files_paths
        if exclude == JsonArgs(task.fs_conn_id, json_file).is_error_free()
    ]
    setattr(context["task"], "json_files_paths", result)


def filter_errors(context):
    _filter_errors(context, True)


def get_errors(context):
    _filter_errors(context, False)
