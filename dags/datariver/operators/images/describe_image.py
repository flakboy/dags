from airflow.models.baseoperator import BaseOperator
from datariver.operators.common.json_tools import JsonArgs
from datariver.operators.common.exception_managing import ErrorHandler


class JsonDescribeImage(BaseOperator):
    template_fields = (
        "json_files_paths",
        "fs_conn_id",
        "input_key",
        "output_key",
        "encoding",
        "local_model_path",
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
        local_model_path=None,
        min_length=20,
        max_length=30,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.json_files_paths = json_files_paths
        self.fs_conn_id = fs_conn_id
        self.input_key = input_key
        self.output_key = output_key
        self.encoding = encoding
        self.local_model_path = local_model_path
        self.error_key = error_key
        self.min_length = min_length
        self.max_length = max_length

    def execute(self, context):
        from transformers import BlipProcessor, BlipForConditionalGeneration

        # Load the pre-trained BLIP model and processor
        if self.local_model_path is None:
            model_source = "Salesforce/blip-image-captioning-base"
        else:
            model_source = self.local_model_path
        model = BlipForConditionalGeneration.from_pretrained(model_source)
        processor = BlipProcessor.from_pretrained(model_source)
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

            # Preprocess the image and prepare inputs for the model
            inputs = processor(images=image, return_tensors="pt")
            # Generate caption
            caption = model.generate(
                **inputs,
                min_length=self.min_length,
                max_length=self.max_length,
                max_new_tokens=100,
                num_beams=5,
                repetition_penalty=2.0
            )
            # Decode the generated caption
            caption_text = processor.decode(caption[0], skip_special_tokens=True)

            json_args.add_value(self.output_key, caption_text)
