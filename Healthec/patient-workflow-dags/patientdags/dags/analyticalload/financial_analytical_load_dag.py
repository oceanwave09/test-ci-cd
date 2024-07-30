import os
from datetime import datetime

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

from patientdags.utils.constants import TEMPLATE_SEARCH_PATH
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
    src_template = os.path.join(TEMPLATE_SEARCH_PATH, "patient", "analyticalload", "financial_analytical_load_job.yaml")

    ingestion_bucket = Variable.get("DATA_INGESTION_BUCKET")
    event_file_path = f"s3a://{ingestion_bucket}/{file_key}"
    substitute = {
        "RANDOM_ID": get_random_string(),
        "TENANT": file_tenant,
        "EVENT_FILE_PATH": event_file_path,
    }
    job_definition = generate_job_definition(src_template, substitute)
    ti.xcom_push(key="job_definition", value=job_definition)


with DAG(
    dag_id="financial_analytical_load_dag",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    concurrency=1,
    description="Loads financial data into analytical table in delta lake",
    template_searchpath=[TEMPLATE_SEARCH_PATH],
    tags=["analyticalload", "financial"],
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

    analytical_load_task = SparkKubernetesOperator(
        task_id="analytical_load_task",
        namespace="data-pipeline",
        application_file="{{ ti.xcom_pull(task_ids='prepare_run_template', key='job_definition') }}",
        do_xcom_push=True,
        dag=dag,
    )

    # monitors spark job submitted with timeout 3600 seconds
    analytical_load_monitor_task = SparkKubernetesSensor(
        task_id="analytical_load_monitor_task",
        namespace="data-pipeline",
        timeout=3600,
        application_name="{{ ti.xcom_pull(task_ids='analytical_load_task')['metadata']['name'] }}",
        dag=dag,
    )

    post_dag_status = PythonOperator(
        task_id="post_dag_status",
        python_callable=update_dag_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        parse_config
        >> init_dag
        >> prepare_run_template
        >> analytical_load_task
        >> analytical_load_monitor_task
        >> post_dag_status
    )
