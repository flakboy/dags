from datariver.operators.common.json_tools import JsonArgs


class ErrorHandler:

    def __init__(
        self, json_file_path, fs_conn_id, error_key, task_id, encoding="utf-8", **kwargs
    ):
        super().__init__(**kwargs)
        self.json_files_path = json_file_path
        self.fs_conn_id = fs_conn_id
        self.error_key = error_key
        self.encoding = encoding
        self.task_id = task_id
        self.json_args = JsonArgs(self.fs_conn_id, json_file_path, self.encoding)

    # this approach to error handling may be not fully effective due to relying on files, but it supports batching
    def is_file_error_free(self):
        return self.error_key not in self.json_args.get_keys()

    # this method checks, if there was an error in previous tasks, but NOT current task to allow current task rerun,
    # so even if error is found, we need to make sure it's from previous task
    def are_previous_tasks_error_free(self):
        if self.error_key in self.json_args.get_keys():
            if self.task_id == self.json_args.get_value(self.error_key).get("task_id"):
                return True
            return False
        return True

    def save_error_to_file(self, message):
        error_data = {"task_id": self.task_id, "message": message}
        self.json_args.add_value(self.error_key, error_data)

    def save_error_list_to_file(self, message):
        error_data = {"task_id": self.task_id, "message": message}
        self.json_args.add_or_update(self.error_key, [error_data])

    # for all cases by now, getting only one error from file should be sufficient, as further processing a file containing error is not foreseen
    def get_error_from_file(self):
        if self.error_key in self.json_args.get_keys():
            return self.json_args.get_value(self.error_key)
        return None

    # to be used after successful rerun, in order to allow further flow, as file is recognized as failed if it contains error
    def remove_error(self):
        if self.error_key in self.json_args.get_keys():
            self.json_args.remove_value(self.error_key)
