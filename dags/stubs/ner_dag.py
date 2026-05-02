from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# Needed to import airflow variables defined in http://.../variable/list/
from airflow.models import Variable


minio_url = Variable.get("minio_api_url")  # variables are defined through Airflow panel
minio_access_key = Variable.get("minio_access_key")
minio_secret_key = Variable.get("minio_secret_key")


text = "Google LLC is an American multinational corporation and technology company focusing on online advertising, search engine technology, cloud computing, computer software, quantum computing, e-commerce, consumer electronics, and artificial intelligence (AI). It has been referred to as \"the most powerful company in the world\" and is one of the world's most valuable brands due to its market dominance, data collection, and technological advantages in the field of AI. Google's parent company, Alphabet Inc. is one of the five Big Tech companies, alongside Amazon, Apple, Meta, and Microsoft.\
Google was founded on September 4, 1998, by American computer scientists Larry Page and Sergey Brin while they were PhD students at Stanford University in California. Together, they own about 14% of its publicly listed shares and control 56% of its stockholder voting power through super-voting stock. The company went public via an initial public offering (IPO) in 2004. In 2015, Google was reorganized as a wholly owned subsidiary of Alphabet Inc. Google is Alphabet's largest subsidiary and is a holding company for Alphabet's internet properties and interests. Sundar Pichai was appointed CEO of Google on October 24, 2015, replacing Larry Page, who became the CEO of Alphabet. On December 3, 2019, Pichai also became the CEO of Alphabet.\
The company has since rapidly grown to offer a multitude of products and services beyond Google Search, many of which hold dominant market positions. These products address a wide range of use cases, including email (Gmail), navigation (Waze & Maps), cloud computing (Cloud), web navigation (Chrome), video sharing (YouTube), productivity (Workspace), operating systems (Android), cloud storage (Drive), language translation (Translate), photo storage (Photos), videotelephony (Meet), smart home (Nest), smartphones (Pixel), wearable technology (Pixel Watch & Fitbit), music streaming (YouTube Music), video on demand (YouTube TV), AI (Google Assistant & Gemini), machine learning APIs (TensorFlow), AI chips (TPU), and more. Discontinued Google products include gaming (Stadia), Glass, Google+, Reader, Play Music, Nexus, Hangouts, and Inbox by Gmail."


def recognize_entities():
    # from io import BytesIO
    from minio import Minio
    import spacy

    client = Minio(
        minio_url,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False,
    )

    download_path = "data/example_file.txt"
    # second argument - object_name, perhaps should be passed from previous tasks
    response = client.fget_object("airflow-bucket", "example_file.txt", download_path)

    # Read data from response.
    # print(response.data.decode("utf-8"))

    nlp = spacy.load("en_core_web_md")
    # TODO:
    # We shouldn't load all data at once - perhaps we need to format it properly in previous tasks, or split data to separate files and read them one by one.
    try:
        with open(download_path, "r") as f:
            doc = nlp(f.read())

            for ent in doc.ents:
                print(ent.text + " | " + str(ent.label_) + " | " + str(ent.sent))

            # TODO:
            # pass data from this task further, so we can store it in database
    except IOError:
        raise Exception("Given file doesn't exist!")


with DAG(
    dag_id="minio_test",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        # "retries": 1,
        # "retry_delay": timedelta(minutes=5),
    },
    description="Lorem ipsum",
    start_date=datetime(2024, 4, 18),
    # schedule_interval = '*/5 * * * *', # schedule_interval supports CRON expressions
    # schedule=timedelta(minutes=5)
) as dag:
    # in order to connect, we need to create an "Amazon Web Service" connection in Airflow panel!!!
    # check if the object exists in S3/MinIO storage
    sensor_task = S3KeySensor(
        task_id="file_sensor",
        bucket_name="airflow-bucket",
        bucket_key="data.csv",
        aws_conn_id="sensor_minio_s3",
    )

    ner_task = PythonOperator(task_id="ner_task", python_callable=recognize_entities)

sensor_task >> ner_task
