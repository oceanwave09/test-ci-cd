import os
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
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
from patientdags.utils.utils import (
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


def _prepare_run_template(ti, entity: str):
    file_tenant = ti.xcom_pull(task_ids="parse_config", key="file_tenant")
    file_source = ti.xcom_pull(task_ids="parse_config", key="file_source")
    file_batch_id = ti.xcom_pull(task_ids="parse_config", key="file_batch_id")
    file_format = str(ti.xcom_pull(task_ids="parse_config", key="file_format"))
    resource_type = str(ti.xcom_pull(task_ids="parse_config", key="resource_type"))
    src_file_name = str(ti.xcom_pull(task_ids="parse_config", key="src_file_name"))
    src_organization_id = str(ti.xcom_pull(task_ids="parse_config", key="src_organization_id"))

    if file_format.lower() != "cclf":
        raise ValueError("Invalid file format")

    src_template = os.path.join(TMPL_SEARCH_PATH, "patient", "fhirbundle", "cclf_stage_to_fhirbundle_job.yaml")

    substitute = {
        "RANDOM_ID": get_random_string(),
        "TENANT": file_tenant,
        "FILE_TENANT": file_tenant,
        "FILE_SOURCE": file_source,
        "RESOURCE_TYPE": resource_type,
        "FILE_BATCH_ID": file_batch_id,
        "SRC_FILE_NAME": src_file_name,
        "SRC_ORGANIZATION_ID": src_organization_id,
        "ENTITY": entity.lower(),
    }
    job_definition = generate_job_definition(src_template, substitute)
    ti.xcom_push(key="job_definition", value=job_definition)


with DAG(
    dag_id="cclf_stage_to_fhirbundle_dag",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=2,
    concurrency=2,
    description="Prepare fhir bundle with from stage delta lake tables",
    template_searchpath=[TMPL_SEARCH_PATH],
    tags=["fhirbundle", "cclf"],
) as dag:
    parse_config = PythonOperator(
        task_id="parse_config",
        python_callable=read_conf,
        on_success_callback=publish_start_status,
    )

    # beneficiary processing
    with TaskGroup(group_id="beneficiary") as beneficiary:
        prepare_run_template = PythonOperator(
            task_id="prepare_run_template",
            python_callable=_prepare_run_template,
            op_kwargs={
                "entity": "beneficiary",
            },
        )
        fhirbundle_task = SparkKubernetesOperator(
            task_id="fhirbundle_task",
            namespace="data-pipeline",
            application_file="{{ ti.xcom_pull(task_ids='beneficiary.prepare_run_template', key='job_definition') }}",
            do_xcom_push=True,
            dag=dag,
        )
        fhirbundle_monitor_task = SparkKubernetesSensor(
            task_id="fhirbundle_monitor_task",
            namespace="data-pipeline",
            timeout=3600,
            application_name="{{ ti.xcom_pull(task_ids='beneficiary.fhirbundle_task')['metadata']['name'] }}",
            dag=dag,
        )
        (prepare_run_template >> fhirbundle_task >> fhirbundle_monitor_task)

    # institutional claim processing
    with TaskGroup(group_id="insclaim") as insclaim:
        prepare_run_template = PythonOperator(
            task_id="prepare_run_template",
            python_callable=_prepare_run_template,
            op_kwargs={
                "entity": "insclaim",
            },
        )
        fhirbundle_task = SparkKubernetesOperator(
            task_id="fhirbundle_task",
            namespace="data-pipeline",
            application_file="{{ ti.xcom_pull(task_ids='insclaim.prepare_run_template', key='job_definition') }}",
            do_xcom_push=True,
            dag=dag,
        )
        fhirbundle_monitor_task = SparkKubernetesSensor(
            task_id="fhirbundle_monitor_task",
            namespace="data-pipeline",
            timeout=3600,
            application_name="{{ ti.xcom_pull(task_ids='insclaim.fhirbundle_task')['metadata']['name'] }}",
            dag=dag,
        )
        (prepare_run_template >> fhirbundle_task >> fhirbundle_monitor_task)

    # physician claim processing
    with TaskGroup(group_id="phyclaim") as phyclaim:
        prepare_run_template = PythonOperator(
            task_id="prepare_run_template",
            python_callable=_prepare_run_template,
            op_kwargs={
                "entity": "phyclaim",
            },
        )
        fhirbundle_task = SparkKubernetesOperator(
            task_id="fhirbundle_task",
            namespace="data-pipeline",
            application_file="{{ ti.xcom_pull(task_ids='phyclaim.prepare_run_template', key='job_definition') }}",
            do_xcom_push=True,
            dag=dag,
        )
        fhirbundle_monitor_task = SparkKubernetesSensor(
            task_id="fhirbundle_monitor_task",
            namespace="data-pipeline",
            timeout=3600,
            application_name="{{ ti.xcom_pull(task_ids='phyclaim.fhirbundle_task')['metadata']['name'] }}",
            dag=dag,
        )
        (prepare_run_template >> fhirbundle_task >> fhirbundle_monitor_task)

    # DME claim processing
    with TaskGroup(group_id="dmeclaim") as dmeclaim:
        prepare_run_template = PythonOperator(
            task_id="prepare_run_template",
            python_callable=_prepare_run_template,
            op_kwargs={
                "entity": "dmeclaim",
            },
        )
        fhirbundle_task = SparkKubernetesOperator(
            task_id="fhirbundle_task",
            namespace="data-pipeline",
            application_file="{{ ti.xcom_pull(task_ids='dmeclaim.prepare_run_template', key='job_definition') }}",
            do_xcom_push=True,
            dag=dag,
        )
        fhirbundle_monitor_task = SparkKubernetesSensor(
            task_id="fhirbundle_monitor_task",
            namespace="data-pipeline",
            timeout=3600,
            application_name="{{ ti.xcom_pull(task_ids='dmeclaim.fhirbundle_task')['metadata']['name'] }}",
            dag=dag,
        )
        (prepare_run_template >> fhirbundle_task >> fhirbundle_monitor_task)

    # pharmacy claim processing
    with TaskGroup(group_id="rxclaim") as rxclaim:
        prepare_run_template = PythonOperator(
            task_id="prepare_run_template",
            python_callable=_prepare_run_template,
            op_kwargs={
                "entity": "rxclaim",
            },
        )
        fhirbundle_task = SparkKubernetesOperator(
            task_id="fhirbundle_task",
            namespace="data-pipeline",
            application_file="{{ ti.xcom_pull(task_ids='rxclaim.prepare_run_template', key='job_definition') }}",
            do_xcom_push=True,
            dag=dag,
        )
        fhirbundle_monitor_task = SparkKubernetesSensor(
            task_id="fhirbundle_monitor_task",
            namespace="data-pipeline",
            timeout=3600,
            application_name="{{ ti.xcom_pull(task_ids='rxclaim.fhirbundle_task')['metadata']['name'] }}",
            dag=dag,
        )
        (prepare_run_template >> fhirbundle_task >> fhirbundle_monitor_task)

    mark_processed_file = DummyOperator(
        task_id="mark_processed_file",
    )

    post_dag_status = PythonOperator(
        task_id="post_dag_status",
        python_callable=update_dag_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (parse_config >> [beneficiary, insclaim, phyclaim, dmeclaim, rxclaim] >> mark_processed_file >> post_dag_status)
