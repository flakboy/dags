from airflow.models.baseoperator import BaseOperator
from datariver.operators.common.json_tools import JsonArgs
from datariver.operators.common.exception_managing import ErrorHandler


class NerJsonOperator(BaseOperator):
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
        fs_conn_id="fs_data",
        model="en_core_web_sm",
        language="english",
        input_key="translated",
        output_key="ner",
        encoding="utf-8",
        error_key="error",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.json_files_paths = json_files_paths
        self.fs_conn_id = fs_conn_id
        self.model = model
        self.language = language
        self.input_key = input_key
        self.output_key = output_key
        self.encoding = encoding
        self.error_key = error_key

    def execute(self, context):
        import spacy
        import nltk

        nlp = spacy.load(self.model)
        for file_path in self.json_files_paths:
            json_args = JsonArgs(self.fs_conn_id, file_path, self.encoding)
            error_handler = ErrorHandler(
                file_path, self.fs_conn_id, self.error_key, self.task_id, self.encoding
            )

            if error_handler.are_previous_tasks_error_free():
                text = json_args.get_value(self.input_key)

                sentences = nltk.tokenize.sent_tokenize(text, self.language)
                detected = []

                for s in sentences:
                    doc = nlp(s)

                    detected.append(
                        {
                            "sentence": s,
                            "ents": list(
                                map(
                                    lambda ent: {"text": ent.text, "label": ent.label_},
                                    doc.ents,
                                )
                            ),
                        }
                    )

                json_args.add_value(self.output_key, detected)
            else:
                self.log.info("Found error from previous task for file %s", file_path)
