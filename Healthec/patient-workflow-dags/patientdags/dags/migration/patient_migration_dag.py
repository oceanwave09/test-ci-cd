import os
from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.utils.trigger_rule import TriggerRule

from patientdags.utils.constants import LANDING_PATH_SUFIX, TEMPLATE_SEARCH_PATH
from patientdags.utils.utils import (  # prepare_run_template,
    generate_job_definition,
    get_random_string,
    init_files_for_run,
    publish_start_status,
    read_conf,
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
    file_format = ti.xcom_pull(task_ids="parse_config", key="file_format")
    src_template = os.path.join(TEMPLATE_SEARCH_PATH, "patient", "migration", "patient_migration_job.yaml")

    ingestion_bucket = Variable.get("DATA_INGESTION_BUCKET")
    data_file_path = f"s3a://{ingestion_bucket}/{file_key}"

    path = Path(file_key)
    error_file_key = os.path.join(
        str(path.parent).replace(LANDING_PATH_SUFIX, "error_report"),
        os.path.splitext(path.name)[0]
        + "_error_report_"
        + datetime.now(timezone.utc).strftime("%Y_%m_%d_%H_%M_%S")
        + ".csv",
    )
    error_file_path = f"s3://{ingestion_bucket}/{error_file_key}"
    substitute = {
        "RANDOM_ID": get_random_string(),
        "TENANT": file_tenant,
        "ENTITY": file_format,
        "DATA_FILE_PATH": data_file_path,
        "ERROR_FILE_PATH": error_file_path,
    }
    job_definition = generate_job_definition(src_template, substitute)
    ti.xcom_push(key="job_definition", value=job_definition)


with DAG(
    dag_id="patient_migration_dag",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    concurrency=1,
    description="Migrate patient entities with platform services",
    template_searchpath=[TEMPLATE_SEARCH_PATH],
    tags=["migration", "patient"],
) as dag:
    parse_config = PythonOperator(
        task_id="parse_config",
        python_callable=read_conf,
        on_success_callback=publish_start_status,
    )

    init_dag = PythonOperator(task_id="init_dag", python_callable=init_files_for_run)

    prepare_run_template = PythonOperator(
        task_id="prepare_run_template",
        python_callable=_prepare_run_template,
        do_xcom_push=True,
    )

    migration_task = SparkKubernetesOperator(
        task_id="migration_task",
        namespace="data-pipeline",
        application_file="{{ ti.xcom_pull(task_ids='prepare_run_template', key='job_definition') }}",
        do_xcom_push=True,
        dag=dag,
    )

    migration_monitor_task = SparkKubernetesSensor(
        task_id="migration_monitor_task",
        namespace="data-pipeline",
        application_name="{{ ti.xcom_pull(task_ids='migration_task')['metadata']['name'] }}",
        dag=dag,
    )

    post_dag_status = PythonOperator(
        task_id="post_dag_status",
        python_callable=update_dag_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (parse_config >> init_dag >> prepare_run_template >> migration_task >> migration_monitor_task >> post_dag_status)
