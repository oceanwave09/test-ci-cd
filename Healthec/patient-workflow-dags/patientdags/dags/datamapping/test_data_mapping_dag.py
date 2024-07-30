import os
import tempfile
import time
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, get_current_context
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule
from omniparser.parser import OmniParser

from patientdags.utils.aws_s3 import download_file_from_s3
from patientdags.utils.utils import get_random_string

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(day=2, year=2022, month=8),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # "retry_delay": timedelta(minutes=1)
}


def _read_config(ti):
    dag_run = get_current_context()["dag_run"]
    schema_file = dag_run.conf.get("schema_file", "")
    input_file = dag_run.conf.get("input_file", "")
    if not schema_file or not input_file:
        raise ValueError("Please provide both the schema and input file.")
    result_file = f"{os.path.splitext(input_file)[0]}.json"
    ti.xcom_push(key="schema_file", value=schema_file)
    ti.xcom_push(key="input_file", value=input_file)
    ti.xcom_push(key="result_file", value=result_file)


def _convert_raw_to_canonical(schema_file: str, input_file: str, result_file: str):
    bucket = Variable.get("DATA_INGESTION_BUCKET")
    key_prefix = "files/DATA_MAPPING"
    # download schema file into local
    schema_key = f"{key_prefix}/schema/{schema_file}"
    local_schema_file = os.path.join(tempfile.gettempdir(), get_random_string(8))
    schema_local = download_file_from_s3(bucket, schema_key, local_schema_file)[0]

    src_file_path = f"s3://{bucket}/{key_prefix}/input/{input_file}"
    dest_file_path = f"s3://{bucket}/{key_prefix}/result/{result_file}"
    parser = OmniParser(schema_local)
    parser.transform(src_file_path, dest_file_path)


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


def _update_dag_status():
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
    dag_id="test_data_mapping_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description=(
        "This DAG transform the input data into a canonical format, allowing us "
        "to verify the accuracy of the data mappings."
    ),
    tags=["mapping", "test"],
) as dag:
    parse_config = PythonOperator(
        task_id="parse_config",
        python_callable=_read_config,
    )

    convert_raw_to_canonical_task = PythonOperator(
        task_id="convert_raw_to_canonical",
        python_callable=_convert_raw_to_canonical,
        op_kwargs={
            "schema_file": '{{ ti.xcom_pull(task_ids="parse_config", key="schema_file") }}',
            "input_file": '{{ ti.xcom_pull(task_ids="parse_config", key="input_file") }}',
            "result_file": '{{ ti.xcom_pull(task_ids="parse_config", key="result_file") }}',
        },
    )

    post_dag_status = PythonOperator(
        task_id="post_dag_status",
        python_callable=_update_dag_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (parse_config >> convert_raw_to_canonical_task >> post_dag_status)
