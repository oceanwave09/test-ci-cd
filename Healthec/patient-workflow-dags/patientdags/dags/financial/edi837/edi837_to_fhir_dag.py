from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from patientdags.edi837parser.converter import EDI837ToFhir
from patientdags.utils.error_codes import PatientDagsErrorCodes, publish_error_code
from patientdags.utils.utils import (
    initialize_target_files,
    publish_start_status,
    read_conf,
    update_dag_status,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(day=27, year=2023, month=6),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # "retry_delay": timedelta(minutes=1)
}


def _edi837_to_fhir(bucket: str, src_key: str, dest_key: str, metadata: dict = {}):
    try:
        src_file_path = f"s3://{bucket}/{src_key}"
        dest_file_path = f"s3://{bucket}/{dest_key}"
        # TODO: update claim type based on file pattern, right now professional is applied by default
        converter = EDI837ToFhir(src_file_path)
        converter.convert(dest_file_path, metadata)
    except Exception as e:
        publish_error_code(f"{PatientDagsErrorCodes.EDI837_TO_FHIR_CONVERSION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


with DAG(
    dag_id="edi837_to_fhir_dag",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=2,
    concurrency=2,
    dagrun_timeout=timedelta(minutes=30),
    description="Converts EDI837 to FHIR bundle and write it into S3 bucket",
    tags=["edi837", "fhirbundle"],
) as dag:
    parse_config = PythonOperator(
        task_id="parse_config",
        python_callable=read_conf,
        on_success_callback=publish_start_status,
    )

    init_dag = PythonOperator(task_id="init_dag", python_callable=initialize_target_files)

    convert_edi837_to_fhirbundle = PythonOperator(
        task_id="convert_edi837_to_fhirbundle",
        python_callable=_edi837_to_fhir,
        op_kwargs={
            "bucket": Variable.get("DATA_INGESTION_BUCKET"),
            "src_key": '{{ ti.xcom_pull(task_ids="parse_config", key="file_key") }}',
            "dest_key": '{{ ti.xcom_pull(task_ids="init_dag", key="fhirbundle_key") }}',
            "metadata": {
                "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
                "file_source": '{{ ti.xcom_pull(task_ids="parse_config", key="file_source") }}',
                "file_batch_id": '{{ ti.xcom_pull(task_ids="parse_config", key="file_batch_id") }}',
                "resource_type": '{{ ti.xcom_pull(task_ids="parse_config", key="resource_type") }}',
                "src_file_name": '{{ ti.xcom_pull(task_ids="parse_config", key="src_file_name") }}',
                "src_organization_id": '{{ ti.xcom_pull(task_ids="parse_config", key="src_organization_id") }}',
            },
        },
    )

    post_dag_status = PythonOperator(
        task_id="post_dag_status",
        python_callable=update_dag_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (parse_config >> init_dag >> convert_edi837_to_fhirbundle >> post_dag_status)
