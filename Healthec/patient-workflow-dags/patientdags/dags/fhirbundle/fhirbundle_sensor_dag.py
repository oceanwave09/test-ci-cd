from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor

from patientdags.utils.constants import FHIRBUNDLE_PROCESSING_DAG
from patientdags.utils.utils import list_files_from_messages, trigger_fhirbundle_dag

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(day=2, year=2022, month=8),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # "retry_delay": timedelta(minutes=1)
}


with DAG(
    dag_id="fhirbundle_sensor_dag",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    description="Runs for every 5 minutes and triggers `fhirbundle_processing_dag` \
        for each new FHIR bundle file placed in S3 bucket",
    render_template_as_native_obj=True,
    tags=["sensor", "fhirbundle"],
) as dag:
    sqs_sensor = SqsSensor(
        task_id="sqs_sensor",
        sqs_queue=Variable.get("FHIRBUNDLE_SQS_QUEUE_URL"),
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
        },
    )

    trigger_parser = PythonOperator(
        task_id="trigger_parser",
        python_callable=trigger_fhirbundle_dag,
        op_kwargs={
            "file_keys": '{{ ti.xcom_pull(task_ids="list_files") }}',
            "dag_to_trigger": FHIRBUNDLE_PROCESSING_DAG,
        },
    )

    sqs_sensor >> list_files >> trigger_parser
