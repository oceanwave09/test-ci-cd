# import json
import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from patientdags.fhirbundle.bundle_processor import BundleProcessor
from patientdags.utils.constants import DATA_SEARCH_PATH
from patientdags.utils.utils import (
    decrypt_file,
    get_data_key,
    publish_start_status,
    read_conf,
    update_dag_status,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(day=28, year=2023, month=11),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # "retry_delay": timedelta(minutes=1)
}


def process_bulk_fhir_bundle(
    file_key: str,
    file_tenant: str,
    file_source: str,
    file_resource_type: str,
    file_batch_id: str,
    src_file_name: str,
    src_organization_id: str,
):
    bucket, key = Variable.get("DATA_INGESTION_BUCKET"), file_key
    src_path = f"s3://{bucket}/{key}"
    # prepare original(decrypt) local path
    if not os.path.isdir(DATA_SEARCH_PATH):
        os.makedirs(DATA_SEARCH_PATH)
    orig_local_path = os.path.join(DATA_SEARCH_PATH, os.path.basename(file_key))
    # get data key
    data_key = get_data_key(file_tenant)
    try:
        # apply descrypt and copy into local
        decrypt_file(data_key=data_key, encrypt_path=src_path, orig_path=orig_local_path)
        with open(orig_local_path, "r") as file:
            # process bundle from each line
            for line in file:
                obj = BundleProcessor(
                    data=line,
                    file_tenant=file_tenant,
                    file_source=file_source,
                    file_resource_type=file_resource_type,
                    file_batch_id=file_batch_id,
                    src_file_name=src_file_name,
                    src_organization_id=src_organization_id,
                )
                obj.process_bundle()
    finally:
        # remove original local file
        if os.path.exists(orig_local_path):
            os.remove(orig_local_path)


with DAG(
    dag_id="fhirbundle_bulk_processing_dag",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=2,
    concurrency=2,
    description="Parse FHIR bundles and post resources with platform services",
    tags=["fhirbundle", "bulk"],
) as dag:
    parse_config = PythonOperator(
        task_id="parse_config",
        python_callable=read_conf,
        on_success_callback=publish_start_status,
    )

    process_fhirbundle = PythonOperator(
        task_id="process_fhirbundle",
        python_callable=process_bulk_fhir_bundle,
        op_kwargs={
            "file_key": '{{ ti.xcom_pull(task_ids="parse_config", key="file_key") }}',
            "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
            "file_source": '{{ ti.xcom_pull(task_ids="parse_config", key="file_source") }}',
            "file_resource_type": '{{ ti.xcom_pull(task_ids="parse_config", key="resource_type") }}',
            "file_batch_id": '{{ ti.xcom_pull(task_ids="parse_config", key="file_batch_id") }}',
            "src_file_name": '{{ ti.xcom_pull(task_ids="parse_config", key="src_file_name") }}',
            "src_organization_id": '{{ ti.xcom_pull(task_ids="parse_config", key="src_organization_id") }}',
        },
    )

    post_dag_status = PythonOperator(
        task_id="post_dag_status",
        python_callable=update_dag_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (parse_config >> process_fhirbundle >> post_dag_status)
