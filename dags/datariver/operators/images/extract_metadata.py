import json
from airflow.models.baseoperator import BaseOperator
from datariver.operators.common.json_tools import JsonArgs
from datariver.operators.common.exception_managing import ErrorHandler


class JsonExtractMetadata(BaseOperator):
    template_fields = (
        "json_files_paths",
        "fs_conn_id",
        "input_key",
        "output_key",
        "encoding",
    )

    def __init__(
        self,
        *,
        json_files_paths,
        fs_conn_id="fs_data",
        input_key,
        output_key,
        encoding="utf-8",
        error_key="error",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.json_files_paths = json_files_paths
        self.fs_conn_id = fs_conn_id
        self.input_key = input_key
        self.output_key = output_key
        self.encoding = encoding
        self.error_key = error_key

    def execute(self, context):
        from PIL import ExifTags

        for file_path in self.json_files_paths:
            json_args = JsonArgs(self.fs_conn_id, file_path, self.encoding)
            image = json_args.get_PIL_image(self.input_key)
            if image is None:
                error_handler = ErrorHandler(
                    file_path,
                    self.fs_conn_id,
                    self.error_key,
                    self.task_id,
                    self.encoding,
                )
                error_handler.save_error_list_to_file("Cannot download file")
                continue
            exif_info = image._getexif()
            metadata = []
            if exif_info is not None:
                for tag, value in exif_info.items():
                    if isinstance(value, bytes):
                        try:
                            value = value.decode(encoding=json.detect_encoding(value))
                        except UnicodeDecodeError:
                            error_handler = ErrorHandler(
                                file_path,
                                self.fs_conn_id,
                                self.error_key,
                                self.task_id,
                                self.encoding,
                            )
                            error_handler.save_error_list_to_file(
                                f"Wrong {ExifTags.TAGS.get(tag)} encoding"
                            )
                            continue
                    else:
                        value = str(value)
                    metadata.append({"tag": ExifTags.TAGS.get(tag), "value": value})
            json_args.add_value(self.output_key, metadata)
