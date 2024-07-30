from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor

from patientdags.utils.constants import (
    FHIRBUNDLE_BULK_PROCESSING_DAG,
    RESTRICTED_EXTENTIONS,
)
from patientdags.utils.utils import list_files_from_messages, trigger_fhirbundle_dag

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(day=6, year=2023, month=12),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # "retry_delay": timedelta(minutes=1)
}

# Note: Need to add SQS URL in Airflow in a name like FHIRBUNDLE_BULK_SQS_QUEUE_URL

with DAG(
    dag_id="fhirbundle_bulk_processing_sensor_dag",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    description="Runs for every 5 minutes and triggers `fhirbundle_bulk_processing_dag` \
        for each new bulk FHIR bundle file placed in S3 bucket",
    render_template_as_native_obj=True,
    tags=["sensor", "fhirbundle", "bulk"],
) as dag:
    sqs_sensor = SqsSensor(
        task_id="sqs_sensor",
        sqs_queue=Variable.get("FHIRBUNDLE_BULK_SQS_QUEUE_URL"),
        max_messages=10,
        num_batches=3,
        mode="reschedule",
        soft_fail=True,
        poke_interval=60,
        timeout=300,
    )

    list_files = PythonOperator(
        task_id="list_files",
        python_callable=list_files_from_messages,
        op_kwargs={
            "messages": '{{ ti.xcom_pull(task_ids="sqs_sensor", key="messages") }}',
            "restrict_ext": RESTRICTED_EXTENTIONS,
        },
    )

    trigger_parser = PythonOperator(
        task_id="trigger_parser",
        python_callable=trigger_fhirbundle_dag,
        op_kwargs={
            "file_keys": '{{ ti.xcom_pull(task_ids="list_files") }}',
            "dag_to_trigger": FHIRBUNDLE_BULK_PROCESSING_DAG,
        },
    )

    sqs_sensor >> list_files >> trigger_parser
