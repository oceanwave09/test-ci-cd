import json
import time
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule

from patientdags.cynchealth.constants import PROCESSED_PATH_SUFIX  # ERROR_PATH_SUFIX,
from patientdags.cynchealth.process import (
    process_condition,
    process_encounter,
    process_immunization,
    process_observation,
    process_organization,
    process_patient,
    process_practitioner,
    process_procedure,
)
from patientdags.cynchealth.utils import group_files_by_resources, parse_s3_path
from patientdags.utils.aws_s3 import list_file_from_s3
from patientdags.utils.constants import TMPL_SEARCH_PATH

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(day=8, year=2023, month=11),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # "retry_delay": timedelta(minutes=1)
}


def init_rmsc_load(ti):
    dag_run = get_current_context()["dag_run"]
    file_tenant = dag_run.conf.get("file_tenant", "")
    file_path = dag_run.conf.get("file_path", "")
    bucket, prefix = parse_s3_path(file_path)
    files = list_file_from_s3(bucket=bucket, prefix=prefix)
    # skip processed and error directory
    # files = [file for file in files if (PROCESSED_PATH_SUFIX not in file) and (ERROR_PATH_SUFIX not in file)]
    files = [file for file in files if PROCESSED_PATH_SUFIX not in file]
    file_groups = group_files_by_resources(files)
    print(f"init_rmsc_load - file_groups: {file_groups}")
    # write into xcom
    ti.xcom_push(key="file_tenant", value=file_tenant)
    ti.xcom_push(key="file_path", value=file_path)
    ti.xcom_push(key="file_groups", value=json.dumps(file_groups))


def _check_dag_status():
    status = TaskInstanceState.SUCCESS
    context = get_current_context()
    dag_run = context["dag_run"]
    task_instances = dag_run.get_task_instances()
    for task_instance in task_instances:
        print(f"task id: {task_instance.task_id}, status: {task_instance.current_state()}")
        state = task_instance.current_state()
        if state == TaskInstanceState.FAILED and status == TaskInstanceState.SUCCESS:
            status = state
        elif state == TaskInstanceState.RUNNING and task_instance.task_id != "post_dag_status":
            status = state
    return status


def update_dag_status():
    status = TaskInstanceState.RUNNING
    while status == TaskInstanceState.RUNNING:
        time.sleep(5)
        status = _check_dag_status()
    context = get_current_context()
    if status == TaskInstanceState.FAILED:
        dag_run = context["dag_run"]
        dag_run.set_state(DagRunState.FAILED)
    else:
        dag_run = context["dag_run"]
        dag_run.set_state(DagRunState.SUCCESS)


with DAG(
    dag_id="rmsc_load_dag",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    concurrency=1,
    description="Process Cynchealth RMSC files, prepare FHIR resources and post with platform services",
    template_searchpath=[TMPL_SEARCH_PATH],
    tags=["rmsc"],
) as dag:
    init_dag = PythonOperator(
        task_id="init_dag",
        python_callable=init_rmsc_load,
    )

    # Organization processing
    organization_processing = PythonOperator(
        task_id="organization_processing",
        python_callable=process_organization,
        op_kwargs={
            "file_tenant": '{{ ti.xcom_pull(task_ids="init_dag", key="file_tenant") }}',
            "file_path": '{{ ti.xcom_pull(task_ids="init_dag", key="file_path") }}',
            "file_groups": '{{ ti.xcom_pull(task_ids="init_dag", key="file_groups") }}',
        },
    )

    # Prcatitioner processing
    practitioner_processing = PythonOperator(
        task_id="practitioner_processing",
        python_callable=process_practitioner,
        op_kwargs={
            "file_tenant": '{{ ti.xcom_pull(task_ids="init_dag", key="file_tenant") }}',
            "file_path": '{{ ti.xcom_pull(task_ids="init_dag", key="file_path") }}',
            "file_groups": '{{ ti.xcom_pull(task_ids="init_dag", key="file_groups") }}',
            "org_ids": '{{ ti.xcom_pull(task_ids="organization_processing") }}',
        },
    )

    # Patient processing
    patient_processing = PythonOperator(
        task_id="patient_processing",
        python_callable=process_patient,
        op_kwargs={
            "file_tenant": '{{ ti.xcom_pull(task_ids="init_dag", key="file_tenant") }}',
            "file_path": '{{ ti.xcom_pull(task_ids="init_dag", key="file_path") }}',
            "file_groups": '{{ ti.xcom_pull(task_ids="init_dag", key="file_groups") }}',
            "org_ids": '{{ ti.xcom_pull(task_ids="organization_processing") }}',
        },
    )

    # Encounter processing
    encounter_processing = PythonOperator(
        task_id="encounter_processing",
        python_callable=process_encounter,
        op_kwargs={
            "file_tenant": '{{ ti.xcom_pull(task_ids="init_dag", key="file_tenant") }}',
            "file_path": '{{ ti.xcom_pull(task_ids="init_dag", key="file_path") }}',
            "file_groups": '{{ ti.xcom_pull(task_ids="init_dag", key="file_groups") }}',
            "pat_ids": '{{ ti.xcom_pull(task_ids="patient_processing") }}',
            "pract_ids": '{{ ti.xcom_pull(task_ids="practitioner_processing") }}',
            "org_ids": '{{ ti.xcom_pull(task_ids="organization_processing") }}',
        },
    )

    # Condition processing
    condition_processing = PythonOperator(
        task_id="condition_processing",
        python_callable=process_condition,
        op_kwargs={
            "file_tenant": '{{ ti.xcom_pull(task_ids="init_dag", key="file_tenant") }}',
            "file_path": '{{ ti.xcom_pull(task_ids="init_dag", key="file_path") }}',
            "file_groups": '{{ ti.xcom_pull(task_ids="init_dag", key="file_groups") }}',
            "pat_ids": '{{ ti.xcom_pull(task_ids="patient_processing") }}',
            "enc_ids": '{{ ti.xcom_pull(task_ids="encounter_processing") }}',
        },
    )

    # Observation processing
    observation_processing = PythonOperator(
        task_id="observation_processing",
        python_callable=process_observation,
        op_kwargs={
            "file_tenant": '{{ ti.xcom_pull(task_ids="init_dag", key="file_tenant") }}',
            "file_path": '{{ ti.xcom_pull(task_ids="init_dag", key="file_path") }}',
            "file_groups": '{{ ti.xcom_pull(task_ids="init_dag", key="file_groups") }}',
            "pat_ids": '{{ ti.xcom_pull(task_ids="patient_processing") }}',
            "enc_ids": '{{ ti.xcom_pull(task_ids="encounter_processing") }}',
        },
    )

    # Procedure processing
    procedure_processing = PythonOperator(
        task_id="procedure_processing",
        python_callable=process_procedure,
        op_kwargs={
            "file_tenant": '{{ ti.xcom_pull(task_ids="init_dag", key="file_tenant") }}',
            "file_path": '{{ ti.xcom_pull(task_ids="init_dag", key="file_path") }}',
            "file_groups": '{{ ti.xcom_pull(task_ids="init_dag", key="file_groups") }}',
            "pat_ids": '{{ ti.xcom_pull(task_ids="patient_processing") }}',
            "enc_ids": '{{ ti.xcom_pull(task_ids="encounter_processing") }}',
        },
    )

    # Immunization processing
    immunization_processing = PythonOperator(
        task_id="immunization_processing",
        python_callable=process_immunization,
        op_kwargs={
            "file_tenant": '{{ ti.xcom_pull(task_ids="init_dag", key="file_tenant") }}',
            "file_path": '{{ ti.xcom_pull(task_ids="init_dag", key="file_path") }}',
            "file_groups": '{{ ti.xcom_pull(task_ids="init_dag", key="file_groups") }}',
            "pat_ids": '{{ ti.xcom_pull(task_ids="patient_processing") }}',
            "enc_ids": '{{ ti.xcom_pull(task_ids="encounter_processing") }}',
        },
    )

    mark_processed_file = DummyOperator(
        task_id="mark_processed_file",
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    post_dag_status = PythonOperator(
        task_id="post_dag_status",
        python_callable=update_dag_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        init_dag
        >> organization_processing
        >> practitioner_processing
        >> patient_processing
        >> encounter_processing
        >> [condition_processing, observation_processing, procedure_processing, immunization_processing]
        >> mark_processed_file
        >> post_dag_status
    )
