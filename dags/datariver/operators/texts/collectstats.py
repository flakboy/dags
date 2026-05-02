from airflow.models.baseoperator import BaseOperator
import os
from datariver.operators.common.exception_managing import ErrorHandler
from datariver.operators.common.json_tools import JsonArgs


def write_dict_to_file(dictionary, file):
    sorted_dict = dict(
        sorted(dictionary.items(), key=lambda item: item[1], reverse=True)
    )
    for key in sorted_dict:
        file.write(key + " - " + str(sorted_dict[key]) + "\n")


def _escape_text(text):
    return text.replace("\\", "\\\\")


class JsonSummaryMarkdownOperator(BaseOperator):
    template_fields = (
        "output_dir",
        "summary_filenames",
        "fs_conn_id",
        "json_files_paths",
        "input_key",
        "encoding",
        "error_key",
    )

    def __init__(
        self,
        *,
        summary_filenames,
        output_dir=".",
        fs_conn_id="fs_data",
        input_key,
        json_files_paths,
        encoding="utf-8",
        error_key="error",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.fs_conn_id = fs_conn_id
        self.summary_filenames = summary_filenames
        self.output_dir = output_dir

        self.input_key = input_key
        self.encoding = encoding
        self.json_files_paths = json_files_paths
        self.error_key = error_key

    def __render_item(self, data, level=0):
        type_ = type(data)

        if type_ is str:
            return _escape_text(data) + "\n"  # we need to escape backslash
        elif type_ is float or type_ is int:
            return str(data) + "\n"
        elif type_ is dict:
            return self.__render_dict(data, level + 1)
        elif type_ is list or type_ is tuple:
            return "\n" + self.__render_list(data, level + 1)

    def __render_list(self, items, level=0):
        text = ""

        for item in items:
            text += (level * "\t") + "- " + self.__render_item(item) + "\n"

        return text

    def __render_dict(self, data, level=0):
        text = ""
        for key, value in data.items():
            text += (level * "\t") + f"{key}: " + self.__render_item(value, level + 1)

        return text

    def execute(self, context):
        for i, file_path in enumerate(self.json_files_paths):
            json_args = JsonArgs(self.fs_conn_id, file_path, self.encoding)
            full_path = os.path.join(
                json_args.get_base_path(), self.output_dir, self.summary_filenames[i]
            )
            error_handler = ErrorHandler(
                file_path, self.fs_conn_id, self.error_key, self.task_id, self.encoding
            )
            if error_handler.are_previous_tasks_error_free():
                os.makedirs(os.path.dirname(full_path), exist_ok=True)

                try:
                    with open(full_path, "w") as file:
                        file.write("# Summary statistics of dag run:\n")

                        # temporary solution until collecting translate task does not work
                        stats = []
                        stats.append(json_args.get_value(self.input_key))
                        for stat in stats:
                            if stat["title"]:
                                file.write(f"## {stat['title']}\n")

                            for key, value in stat["stats"].items():
                                rendered = ""
                                rendered += f"- {key}: "

                                rendered += self.__render_item(value)

                                file.write(rendered)

                except IOError as e:
                    raise Exception(f"Couldn't open {full_path} ({str(e)})!")
            else:
                self.log.info("Found error from previous task for file %s", file_path)
