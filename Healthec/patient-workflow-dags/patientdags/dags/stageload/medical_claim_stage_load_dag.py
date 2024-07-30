import os
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from patientdags.utils.constants import TMPL_SEARCH_PATH
from patientdags.utils.utils import (  # prepare_run_template,
    generate_job_definition,
    get_random_string,
    get_validation_results,
    init_files_for_run,
    publish_start_status,
    read_conf,
    trigger_dag_with_config,
    update_dag_status,
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


def _prepare_run_template(ti):
    file_key = ti.xcom_pull(task_ids="parse_config", key="file_key")
    file_tenant = ti.xcom_pull(task_ids="parse_config", key="file_tenant")
    file_source = ti.xcom_pull(task_ids="parse_config", key="file_source")
    file_batch_id = ti.xcom_pull(task_ids="parse_config", key="file_batch_id")
    src_file_name = ti.xcom_pull(task_ids="parse_config", key="src_file_name")
    src_template = os.path.join(TMPL_SEARCH_PATH, "patient", "stageload", "medical_claim_stage_load_job.yaml")
    ingestion_bucket = Variable.get("DATA_INGESTION_BUCKET")
    canonical_file_path = f"s3a://{ingestion_bucket}/{file_key}"
    substitute = {
        "RANDOM_ID": get_random_string(),
        "CANONICAL_FILE_PATH": canonical_file_path,
        "TENANT": file_tenant,
        "FILE_BATCH_ID": file_batch_id,
        "FILE_SOURCE": file_source,
        "FILE_NAME": src_file_name,
    }
    job_definition = generate_job_definition(src_template, substitute)
    ti.xcom_push(key="job_definition", value=job_definition)


def _field_validation(ti):
    validation_result = get_validation_results(ti)
    if validation_result is None:
        raise AirflowFailException("Failed to Validate")
    ti.xcom_push(key="validation_error_list", value=validation_result)


def _trigger_fhir_bundle(ti):
    fhir_config = {
        "file_tenant": ti.xcom_pull(task_ids="parse_config", key="file_tenant"),
        "file_source": ti.xcom_pull(task_ids="parse_config", key="file_source"),
        "file_format": ti.xcom_pull(task_ids="parse_config", key="file_format"),
        "file_type": ti.xcom_pull(task_ids="parse_config", key="file_type"),
        "resource_type": ti.xcom_pull(task_ids="parse_config", key="resource_type"),
        "file_batch_id": ti.xcom_pull(task_ids="parse_config", key="file_batch_id"),
        "src_file_name": ti.xcom_pull(task_ids="parse_config", key="src_file_name"),
        "src_organization_id": ti.xcom_pull(task_ids="parse_config", key="src_organization_id"),
    }
    trigger_dag = ti.xcom_pull(task_ids="field_validation_task", key="fhir_script_name")
    trigger_dag_with_config(dag_to_trigger=trigger_dag, conf=fhir_config)


with DAG(
    dag_id="medical_claim_stage_load_dag",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=2,
    concurrency=2,
    description="Loads canonical claims data into staging table in delta lake",
    template_searchpath=[TMPL_SEARCH_PATH],
    tags=["stageload", "claim", "medical"],
) as dag:
    parse_config = PythonOperator(
        task_id="parse_config",
        python_callable=read_conf,
        on_success_callback=publish_start_status,
    )

    init_dag = PythonOperator(task_id="init_dag", python_callable=init_files_for_run)

    with TaskGroup(group_id="stage_load") as stage_load:
        prepare_run_template = PythonOperator(
            task_id="prepare_run_template",
            python_callable=_prepare_run_template,
            do_xcom_push=True,
        )

        stage_load_task = SparkKubernetesOperator(
            task_id="stage_load_task",
            namespace="data-pipeline",
            application_file="{{ ti.xcom_pull(task_ids='stage_load.prepare_run_template', key='job_definition') }}",
            do_xcom_push=True,
            dag=dag,
        )

        # monitors spark job submitted with timeout 3600 seconds
        stage_load_monitor_task = SparkKubernetesSensor(
            task_id="stage_load_monitor_task",
            namespace="data-pipeline",
            timeout=3600,
            application_name="{{ ti.xcom_pull(task_ids='stage_load.stage_load_task')['metadata']['name'] }}",
            dag=dag,
        )

        prepare_run_template >> stage_load_task >> stage_load_monitor_task

    field_validation = PythonOperator(
        task_id="field_validation_task",
        python_callable=_field_validation,
    )

    trigger_fhir_bundle = PythonOperator(
        task_id="trigger_fhir_bundle_task",
        python_callable=_trigger_fhir_bundle,
    )

    post_dag_status = PythonOperator(
        task_id="post_dag_status",
        python_callable=update_dag_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (parse_config >> init_dag >> [stage_load, field_validation] >> trigger_fhir_bundle >> post_dag_status)
