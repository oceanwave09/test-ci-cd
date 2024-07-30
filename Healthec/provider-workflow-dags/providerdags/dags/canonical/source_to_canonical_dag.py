from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from providerdags.utils.parser import convert_raw_to_canonical
from providerdags.utils.utils import (
    init_canonical_files_for_run,
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


with DAG(
    dag_id="source_to_canonical_dag",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=5,
    concurrency=5,
    description="Converts raw data into canonical format using Omniparser and write it into S3 bucket",
    tags=["canonical"],
) as dag:
    parse_config = PythonOperator(
        task_id="parse_config",
        python_callable=read_conf,
        on_success_callback=publish_start_status,
    )

    init_dag = PythonOperator(task_id="init_dag", python_callable=init_canonical_files_for_run)

    convert_raw_to_canonical_task = PythonOperator(
        task_id="convert_raw_to_canonical",
        python_callable=convert_raw_to_canonical,
        op_kwargs={
            "schema": '{{ ti.xcom_pull(task_ids="init_dag", key="schema") }}',
            "src_path": '{{ ti.xcom_pull(task_ids="init_dag", key="src_path") }}',
            "dest_path": '{{ ti.xcom_pull(task_ids="init_dag", key="dest_path") }}',
            "metadata": {
                "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
                "file_source": '{{ ti.xcom_pull(task_ids="parse_config", key="file_source") }}',
                "file_batch_id": '{{ ti.xcom_pull(task_ids="parse_config", key="file_batch_id") }}',
                "resource_type": '{{ ti.xcom_pull(task_ids="parse_config", key="resource_type") }}',
                "src_file_name": '{{ ti.xcom_pull(task_ids="parse_config", key="src_file_name") }}',
                "src_organization_id": '{{ ti.xcom_pull(task_ids="init_dag", key="src_organization_id") }}',
            },
        },
    )

    post_dag_status = PythonOperator(
        task_id="post_dag_status",
        python_callable=update_dag_status,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (parse_config >> init_dag >> convert_raw_to_canonical_task >> post_dag_status)
