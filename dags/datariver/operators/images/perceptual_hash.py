from airflow.models.baseoperator import BaseOperator
from datariver.operators.common.json_tools import JsonArgs
from datariver.operators.common.exception_managing import ErrorHandler
from enum import StrEnum


class HashType(StrEnum):
    p_hash = "p_hash"
    block_mean_hash = "block_mean_hash"

    def __contains__(self, item):
        if isinstance(item, self):
            return True
        return any(item == member.value for member in self.__members__.values())


class JsonPerceptualHash(BaseOperator):
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
        hash_type: str = "block_mean_hash",
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
        if hash_type not in HashType:
            # todo add error handling here someheow
            raise AttributeError(f"unsupported HashType {hash_type}")
        self.hash_type: HashType = HashType(hash_type)

    def execute(self, context):

        for file_path in self.json_files_paths:
            json_args = JsonArgs(self.fs_conn_id, file_path, self.encoding)
            cv2_image = json_args.get_cv2_image(self.input_key)
            if cv2_image is None:
                error_handler = ErrorHandler(
                    file_path,
                    self.fs_conn_id,
                    self.error_key,
                    self.task_id,
                    self.encoding,
                )
                error_handler.save_error_list_to_file("Cannot download file")
                continue
            match self.hash_type:
                case HashType.p_hash:
                    hash_value = self.p_hash(cv2_image)
                case HashType.block_mean_hash:
                    hash_value = self.block_mean_hash(cv2_image)
                case _:
                    raise AttributeError()
            json_args.add_value(self.output_key, {self.hash_type: str(hash_value)})

    def p_hash(self, cv2_image):
        import cv2

        hash_value = cv2.img_hash.pHash(cv2_image)
        hash_int = int.from_bytes(hash_value.tobytes(), byteorder="big", signed=False)
        return hash_int

    def block_mean_hash(self, cv2_image):
        import cv2

        block_mean_hash = cv2.img_hash.BlockMeanHash_create()
        hash_value = block_mean_hash.compute(cv2_image)
        hash_int = int.from_bytes(hash_value.tobytes(), byteorder="big", signed=False)
        return hash_int
