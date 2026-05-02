from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}
example_text = "Nie ma tak, że dobrze czy nie dobrze.   "


def detect_language(text: str):
    import langdetect

    lang = langdetect.detect(text)
    print(f"detected language {lang}")
    return lang


# PythonVirtualenvOperator todo check
def translate(text: str, language: str):
    if language == "en":
        print("translation is not necessary")
        return text
    from translate import Translator

    translator = Translator(from_lang=language, to_lang="en")
    translation = translator.translate(text)
    print(f"translation: {translation}")
    return translation


with DAG(
    "language_detection", default_args=default_args, schedule_interval=None
) as dag:
    detect_language_task = PythonOperator(
        task_id="detect_language",
        python_callable=detect_language,
        op_kwargs={"text": example_text},
    )
    translate_task = PythonOperator(
        task_id="translate",
        python_callable=translate,
        op_kwargs={
            "text": example_text,
            "language": "pl",
        },  # todo pass argument from downstream task
    )

detect_language_task >> translate_task
