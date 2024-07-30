from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor

from providerdags.utils.api_client import get_data_key
from providerdags.utils.aws_s3 import delete_file_from_s3
from providerdags.utils.constants import (
    ENCRYPTED_PATH_SPLIT_KEY,
    INCOMING_PATH_SUFIX,
    LANDING_PATH_SUFIX,
)
from providerdags.utils.utils import (
    encrypt_file,
    list_files_from_messages,
    parse_file_details,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(day=2, year=2022, month=8),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # "retry_delay": timedelta(minutes=1)
}


def _encrypt_source_files(file_keys: list):
    ingestion_bucket = Variable.get("DATA_INGESTION_BUCKET")
    for file_key in file_keys:
        if not file_key.strip().lower().endswith(("/", ".dmy")):
            conf = parse_file_details(file_key, ENCRYPTED_PATH_SPLIT_KEY)
            tenant = conf.get("file_tenant", "")
            data_key = get_data_key(tenant)
            src_path = f"s3://{ingestion_bucket}/{file_key}"
            encrypt_key = file_key.replace(INCOMING_PATH_SUFIX, LANDING_PATH_SUFIX)
            encrypt_path = f"s3://{ingestion_bucket}/{encrypt_key}"
            encrypt_file(data_key, src_path, encrypt_path)
            # delete source file from incoming directory
            delete_file_from_s3(ingestion_bucket, file_key)


with DAG(
    dag_id="source_encryption_sensor_dag",
    default_args=default_args,
    schedule_interval="*/2 * * * *",
    max_active_runs=1,
    catchup=False,
    description="Runs for every 2 minutes and encrypt each source files and write into landing path",
    render_template_as_native_obj=True,
    tags=["sensor", "encryption"],
) as dag:
    sqs_sensor = SqsSensor(
        task_id="sqs_sensor",
        sqs_queue=Variable.get("INCOMING_SQS_QUEUE_URL"),
        max_messages=10,
        num_batches=3,
        mode="reschedule",
        soft_fail=True,
        poke_interval=30,
        timeout=120,
    )

    list_files = PythonOperator(
        task_id="list_files",
        python_callable=list_files_from_messages,
        op_kwargs={
            "messages": '{{ ti.xcom_pull(task_ids="sqs_sensor", key="messages") }}',
        },
    )

    encrypt_files = PythonOperator(
        task_id="encrypt_files",
        python_callable=_encrypt_source_files,
        op_kwargs={
            "tenant": '{{ ti.xcom_pull(task_ids="list_files") }}',
            "file_keys": '{{ ti.xcom_pull(task_ids="list_files") }}',
        },
    )

    sqs_sensor >> list_files >> encrypt_files
