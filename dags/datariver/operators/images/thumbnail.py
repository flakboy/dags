from airflow.models.baseoperator import BaseOperator
from datariver.operators.common.json_tools import JsonArgs
from datariver.operators.common.exception_managing import ErrorHandler


class JsonThumbnailImage(BaseOperator):
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
        size=(100, 100),
        **kwargs
    ):
        super().__init__(**kwargs)
        self.json_files_paths = json_files_paths
        self.fs_conn_id = fs_conn_id
        self.input_key = input_key
        self.output_key = output_key
        self.encoding = encoding
        self.error_key = error_key
        self.size = size

    def execute(self, context):
        import base64
        import io

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
            image.thumbnail(self.size)
            byte_img_io = io.BytesIO()
            image.save(byte_img_io, image.format)
            byte_img_io.seek(0)
            encoded = base64.b64encode(byte_img_io.read()).decode()
            json_args.add_value(self.output_key, encoded)
