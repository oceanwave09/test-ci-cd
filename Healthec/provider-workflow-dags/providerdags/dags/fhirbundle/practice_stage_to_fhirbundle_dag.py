import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.utils.trigger_rule import TriggerRule

from providerdags.utils.constants import TMPL_SEARCH_PATH
from providerdags.utils.utils import (
    generate_job_definition,
    get_random_string,
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
    file_tenant = ti.xcom_pull(task_ids="parse_config", key="file_tenant")
    file_source = ti.xcom_pull(task_ids="parse_config", key="file_source")
    file_batch_id = ti.xcom_pull(task_ids="parse_config", key="file_batch_id")
    resource_type = str(ti.xcom_pull(task_ids="parse_config", key="resource_type"))
    src_file_name = str(ti.xcom_pull(task_ids="parse_config", key="src_file_name"))
    src_template = os.path.join(TMPL_SEARCH_PATH, "provider", "fhirbundle", "practice_stage_to_fhirbundle_job.yaml")

    substitute = {
        "RANDOM_ID": get_random_string(),
        "TENANT": file_tenant,
        "FILE_TENANT": file_tenant,
        "FILE_SOURCE": file_source,
        "RESOURCE_TYPE": resource_type,
        "FILE_BATCH_ID": file_batch_id,
        "SRC_FILE_NAME": src_file_name,
    }
    job_definition = generate_job_definition(src_template, substitute)
    ti.xcom_push(key="job_definition", value=job_definition)


with DAG(
    dag_id="practice_stage_to_fhirbundle_dag",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    concurrency=1,
    description="Prepare fhir bundle with from stage delta lake tables",
    template_searchpath=[TMPL_SEARCH_PATH],
    tags=["fhirbundle", "practice"],
) as dag:
    parse_config = PythonOperator(
        task_id="parse_config",
        python_callable=read_conf,
        on_success_callback=publish_start_status,
    )

    prepare_run_template = PythonOperator(task_id="prepare_run_template", python_callable=_prepare_run_template)

    prepare_fhirbundle_task = SparkKubernetesOperator(
        task_id="prepare_fhirbundle_task",
        namespace="data-pipeline",
        application_file="{{ ti.xcom_pull(task_ids='prepare_run_template', key='job_definition') }}",
        do_xcom_push=True,
        dag=dag,
    )

    # monitors spark job submitted with timeout 3600 seconds
    prepare_fhirbundle_monitor_task = SparkKubernetesSensor(
        task_id="prepare_fhirbundle_monitor_task",
        namespace="data-pipeline",
        timeout=3600,
        application_name="{{ ti.xcom_pull(task_ids='prepare_fhirbundle_task')['metadata']['name'] }}",
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
        >> prepare_run_template
        >> prepare_fhirbundle_task
        >> prepare_fhirbundle_monitor_task
        >> post_dag_status
    )
