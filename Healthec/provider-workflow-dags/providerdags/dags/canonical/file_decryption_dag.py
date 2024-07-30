import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context

from providerdags.utils.api_client import get_data_key
from providerdags.utils.utils import decrypt_file

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
    encrypt_file_path = dag_run.conf.get("encrypt_file_path", "")
    file_tenant = dag_run.conf.get("file_tenant", "")
    # decrypt file path
    get_base_dir = os.path.dirname(encrypt_file_path)
    encrypt_path = Path(encrypt_file_path)
    decrypt_file_path = os.path.join(get_base_dir, f"{encrypt_path.stem}_decrypted{encrypt_path.suffix}")
    ti.xcom_push(key="encrypt_file_path", value=encrypt_file_path)
    ti.xcom_push(key="decrypt_file_path", value=decrypt_file_path)
    ti.xcom_push(key="file_tenant", value=file_tenant)
    return


def _apply_decrypt(file_tenant, encrypt_file_path, decrypt_file_path):
    pipeline_data_key = get_data_key(file_tenant)
    decrypt_file(pipeline_data_key, encrypt_file_path, decrypt_file_path)
    return


with DAG(
    dag_id="file_decryption_dag",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    concurrency=1,
    description="Decrypt file and write into given path",
    tags=["decrypt", "file"],
) as dag:
    parse_config = PythonOperator(
        task_id="parse_config",
        python_callable=_read_conf,
    )

    apply_decrypt = PythonOperator(
        task_id="apply_decrypt",
        python_callable=_apply_decrypt,
        op_kwargs={
            "file_tenant": '{{ ti.xcom_pull(task_ids="parse_config", key="file_tenant") }}',
            "encrypt_file_path": '{{ ti.xcom_pull(task_ids="parse_config", key="encrypt_file_path") }}',
            "decrypt_file_path": '{{ ti.xcom_pull(task_ids="parse_config", key="decrypt_file_path") }}',
        },
    )

    (parse_config >> apply_decrypt)
