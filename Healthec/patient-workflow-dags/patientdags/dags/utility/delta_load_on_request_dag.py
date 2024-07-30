import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.utils.task_group import TaskGroup

from patientdags.utils.constants import TMPL_SEARCH_PATH
from patientdags.utils.utils import generate_job_definition, get_random_string

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(day=2, year=2022, month=8),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


def _read_conf(ti):
    dag_run = get_current_context()["dag_run"]
    data_file_path = dag_run.conf.get("data_file_path", "")
    file_tenant = dag_run.conf.get("file_tenant", "")
    delta_schema = dag_run.conf.get("delta_schema", "")
    delta_table = dag_run.conf.get("delta_table", "")
    delimiter = dag_run.conf.get("delimiter", "")
    mode = dag_run.conf.get("mode", "")
    ti.xcom_push(key="data_file_path", value=data_file_path)
    ti.xcom_push(key="file_tenant", value=file_tenant)
    ti.xcom_push(key="delta_schema", value=delta_schema)
    ti.xcom_push(key="delta_table", value=delta_table)
    ti.xcom_push(key="delimiter", value=delimiter)
    ti.xcom_push(key="mode", value=mode)
    return


def _prepare_run_template(ti):
    data_file_path = ti.xcom_pull(task_ids="parse_config", key="data_file_path")
    file_tenant = ti.xcom_pull(task_ids="parse_config", key="file_tenant")
    delta_schema = ti.xcom_pull(task_ids="parse_config", key="delta_schema")
    delta_table = ti.xcom_pull(task_ids="parse_config", key="delta_table")
    delimiter = ti.xcom_pull(task_ids="parse_config", key="delimiter")
    mode = ti.xcom_pull(task_ids="parse_config", key="mode")

    # aws s3 path - replace `s3` with `s3a`
    data_file_path = data_file_path.replace("s3:", "s3a:")

    src_template = os.path.join(TMPL_SEARCH_PATH, "patient", "utility", "delta_load_on_request_job.yaml")

    substitute = {
        "TENANT": file_tenant,
        "RANDOM_ID": get_random_string(),
        "DATA_FILE_PATH": data_file_path,
        "FILE_TENANT": file_tenant,
        "DELTA_SCHEMA": delta_schema,
        "DELTA_TABLE": delta_table,
        "DELIMITER": delimiter,
        "MODE": mode,
    }
    job_definition = generate_job_definition(src_template, substitute)
    ti.xcom_push(key="job_definition", value=job_definition)


with DAG(
    dag_id="delta_load_on_request_dag",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    concurrency=1,
    description="Load data file into delta table",
    tags=["delta", "load"],
) as dag:
    parse_config = PythonOperator(
        task_id="parse_config",
        python_callable=_read_conf,
    )

    with TaskGroup(group_id="delta_load") as delta_load:
        prepare_run_template = PythonOperator(
            task_id="prepare_run_template",
            python_callable=_prepare_run_template,
            do_xcom_push=True,
        )

        delta_load_task = SparkKubernetesOperator(
            task_id="delta_load_task",
            namespace="data-pipeline",
            application_file="{{ ti.xcom_pull(task_ids='delta_load.prepare_run_template', key='job_definition') }}",
            do_xcom_push=True,
            dag=dag,
        )

        # monitors spark job submitted with timeout 3600 seconds
        delta_load_monitor_task = SparkKubernetesSensor(
            task_id="delta_load_monitor_task",
            namespace="data-pipeline",
            timeout=3600,
            application_name="{{ ti.xcom_pull(task_ids='delta_load.delta_load_task')['metadata']['name'] }}",
            dag=dag,
        )

        prepare_run_template >> delta_load_task >> delta_load_monitor_task

    parse_config >> delta_load
