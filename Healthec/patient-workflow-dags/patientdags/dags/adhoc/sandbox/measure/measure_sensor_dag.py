from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor

from patientdags.utils.constants import MEASURE_PROCESSING_DAG
from patientdags.utils.utils import list_files_from_messages, trigger_parsing_dag

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
    dag_id="measure_sensor_dag",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    description="Runs for every 5 minutes and triggers `measure_processing_dag` \
        for each new file placed in measure S3 bucket",
    render_template_as_native_obj=True,
) as dag:
    sqs_sensor = SqsSensor(
        task_id="sqs_sensor",
        sqs_queue=Variable.get("MEASURE_SQS_QUEUE_URL"),
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
        python_callable=trigger_parsing_dag,
        op_kwargs={
            "file_keys": '{{ ti.xcom_pull(task_ids="list_files") }}',
            "dag_to_trigger": MEASURE_PROCESSING_DAG,
        },
    )

    sqs_sensor >> list_files >> trigger_parser
