import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from patientdags.utils.api_client import calculate_patient_measure
from patientdags.utils.auth import AuthClient
from patientdags.utils.aws import read_file_from_s3
from patientdags.utils.constants import (
    MEASURE_AUTH_CLIENT_ID,
    MEASURE_AUTH_CLIENT_SECRET,
    MEASURE_CONNECTION,
)
from patientdags.utils.utils import (
    initialize_target_files,
    publish_failure_status,
    publish_start_status,
    publish_success_status,
    read_conf,
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


def _calculate_measures(data: str):
    patient_ids = [
        entry.split(",")[0] for entry in data.split("\n") if len(entry.split(",")) > 0 and entry.split(",")[0]
    ]
    auth_client = AuthClient(
        host=MEASURE_CONNECTION.host,
        subdomain=MEASURE_CONNECTION.extra_dejson.get("auth_subdomain"),
        tenant_subdomain=MEASURE_CONNECTION.extra_dejson.get("tenant_subdomain"),
    )
    total_patient_ids = len(patient_ids)
    if total_patient_ids <= 19:
        progress_count = 1
    else:
        progress_count = total_patient_ids // 10
    processed_count = 0
    for patient_id in patient_ids:
        bearer_token = auth_client.get_access_token(MEASURE_AUTH_CLIENT_ID, MEASURE_AUTH_CLIENT_SECRET)
        calculate_patient_measure(patient_id, bearer_token)
        processed_count += 1
        if processed_count % progress_count == 0 and processed_count < total_patient_ids:
            logging.info(f"Measures processed for {processed_count} patients")
    logging.info(f"Measures processed for {processed_count} patients")


with DAG(
    dag_id="measure_processing_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Triggers the endpoint to calculate measures for patient",
) as dag:
    parse_config = PythonOperator(
        task_id="parse_config",
        python_callable=read_conf,
        on_success_callback=publish_start_status,
    )

    init_dag = PythonOperator(task_id="init_dag", python_callable=initialize_target_files)

    read_file = PythonOperator(
        task_id="read_file",
        python_callable=read_file_from_s3,
        op_kwargs={
            "bucket": Variable.get("DATA_INGESTION_BUCKET"),
            "key": '{{ ti.xcom_pull(task_ids="parse_config", key="file_key") }}',
        },
    )

    calculate_measures = PythonOperator(
        task_id="calculate_measures",
        python_callable=_calculate_measures,
        op_kwargs={
            "data": '{{ ti.xcom_pull(task_ids="read_file") }}',
        },
        on_success_callback=publish_success_status,
    )

    dag_failure_alert_task = PythonOperator(
        task_id="dag_failure_alert_task",
        python_callable=publish_failure_status,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    (parse_config >> init_dag >> read_file >> calculate_measures >> dag_failure_alert_task)
