from __future__ import annotations

import datetime

import os
from functools import cached_property
from glob import glob

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.hooks.filesystem import FSHook
from airflow.sensors.base import BaseSensorOperator

from airflow.utils.context import Context


class MultipleFilesSensor(BaseSensorOperator):
    template_fields = ("fs_conn_id", "filepath")

    """
    Detects all files present in the base directory, matching the expression.
    If at list one file was detected,
    the list of all detected files is pushed to Xcom with key "return_value".

    :param fs_conn_id: reference to the File (path)
        connection id
    :param filepath: File or folder name (relative to
        the base path set within the connection), can be a glob.
    :param recursive: when set to ``True``, enables recursive directory matching behavior of
        ``**`` in glob filepath parameter. Defaults to ``False``.
    :param deferrable: If waiting for completion, whether to defer the task until done,
        default is ``False``.
    """

    def __init__(
        self,
        *,
        filepath,
        fs_conn_id="fs_data",
        recursive=False,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.filepath = filepath
        self.fs_conn_id = fs_conn_id
        self.recursive = recursive
        self.deferrable = deferrable

        self.detected_files = []

    @cached_property
    def path(self) -> str:
        hook = FSHook(self.fs_conn_id)
        basepath = hook.get_path()
        full_path = os.path.join(basepath, self.filepath)
        return full_path

    def poke(self, context: Context) -> bool:
        self.log.info("Poking for file %s", self.path)

        detected_files = []
        for path in glob(self.path, recursive=self.recursive):
            if os.path.isfile(path):
                mod_time = datetime.datetime.fromtimestamp(
                    os.path.getmtime(path)
                ).strftime("%Y%m%d%H%M%S")

                self.log.info("Found File %s last modified: %s", path, mod_time)

                detected_files.append(path)

            for _, _, files in os.walk(path):
                if files:
                    detected_files.append(path)

        status = len(detected_files) > 0

        if status:
            self.detected_files = detected_files
            # context["ti"].xcom_push(key='found_files', value=detected_files)

        return status

    def execute(self, context: Context) -> None:
        # TODO: perhaps define a MultipleFileTrigger?

        if not self.deferrable:
            super().execute(context=context)
        if not self.poke(context=context):
            self.defer(
                timeout=datetime.timedelta(seconds=self.timeout),
                # trigger=FileTrigger(
                #     filepath=self.path,
                #     recursive=self.recursive,
                #     poke_interval=self.poke_interval,
                # ),
                method_name="execute_complete",
            )

        # pushing files list do Xcom so next tasks can use it
        return self.detected_files

    def execute_complete(self, context: Context, event: bool | None = None) -> None:
        if not event:
            raise AirflowException(
                "%s task failed as %s not found.", self.task_id, self.filepath
            )
        self.log.info(
            "%s completed successfully as %s found.", self.task_id, self.filepath
        )
