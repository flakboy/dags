from airflow.models.baseoperator import BaseOperator
from datariver.operators.common.json_tools import JsonArgs
from datariver.operators.common.exception_managing import ErrorHandler


class NerJsonStatisticsOperator(BaseOperator):
    template_fields = (
        "json_files_paths",
        "fs_conn_id",
        "input_key",
        "output_key",
        "encoding",
        "error_key",
    )

    def __init__(
        self,
        *,
        json_files_paths,
        fs_conn_id,
        input_key,
        output_key,
        encoding="utf-8",
        error_key,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.json_files_paths = json_files_paths
        self.fs_conn_id = fs_conn_id
        self.input_key = input_key
        self.output_key = output_key
        self.encoding = encoding
        self.error_key = error_key

    def execute(self, context):
        for file_path in self.json_files_paths:
            label_counter = dict()
            entity_counter = dict()
            json_args = JsonArgs(self.fs_conn_id, file_path, self.encoding)
            error_handler = ErrorHandler(
                file_path, self.fs_conn_id, self.error_key, self.task_id, self.encoding
            )

            if error_handler.are_previous_tasks_error_free():
                ner_data = json_args.get_value(self.input_key)

                for data in ner_data:
                    for ent in data["ents"]:
                        label = ent["label"]
                        if label in label_counter:
                            label_counter[label] = label_counter[label] + 1
                        else:
                            label_counter[label] = 1

                        entity = ent["text"].replace("\n", "\\n").replace("\t", "\\t")

                        if entity in entity_counter:
                            entity_counter[entity] = entity_counter[entity] + 1
                        else:
                            entity_counter[entity] = 1

                labels = [
                    {"value": value, "count": count}
                    for value, count in label_counter.items()
                ]
                entities = [
                    {"value": value, "count": count}
                    for value, count in entity_counter.items()
                ]

                stats = {
                    "title": "NER statistics",
                    "stats": {"labels": labels, "entities": entities},
                }

                json_args.add_value(self.output_key, stats)
            else:
                self.log.info("Found error from previous task for file %s", file_path)
