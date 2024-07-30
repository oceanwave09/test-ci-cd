import os
import zipfile
import tempfile
from datetime import datetime, timezone

# from io import BytesIO
from pathlib import Path

# import boto3
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule

from patientdags.utils.aws_s3 import move_file_from_s3
from patientdags.utils.constants import (
    CCLF_TYPES,
    DOWNLOAD_ZIP_FILE_PATH,
    SCHEMA_SEARCH_PATH,
    TMPL_SEARCH_PATH,
)
from patientdags.utils.parser import convert_raw_to_canonical
from patientdags.utils.utils import (
    generate_job_definition,
    get_random_string,
    load_yaml_to_dict,
    parse_s3_path,
    publish_start_status,
    trigger_dag_with_config,
    update_dag_status,
    decrypt_file,
    get_data_key,
)


def _rm_tmp_file(file_path: str) -> None:
    if file_path.startswith("/tmp/") and os.path.exists(file_path):
        os.unlink(file_path)


def _init_cclf_canonical_files(ti, file_name: str):
    file_type = ""
    for type, pattern in CCLF_TYPES.items():
        if pattern in file_name:
            file_type = type
            break

    parse_config_dict = ti.xcom_pull(task_ids="parse_config")
    file_tenant = parse_config_dict.get("file_tenant", "")
    file_source = parse_config_dict.get("file_source", "")
    file_key = parse_config_dict.get("file_key", "")
    file_format = parse_config_dict.get("file_format", "")
    file_batch_id = parse_config_dict.get("file_batch_id", "")
    schema_type = f"{file_format}_{file_type}" if file_type else file_format

    # load tenant based schema mapping
    schema_mapping_filepath = os.path.join(SCHEMA_SEARCH_PATH, f"{file_tenant}_schema_mapping.yaml")
    schema_mapping = load_yaml_to_dict(schema_mapping_filepath)

    format_details = {}
    if file_source:
        format_details = schema_mapping.get(file_tenant, {}).get(file_source, {}).get(schema_type, {})
    else:
        format_details = schema_mapping.get(file_tenant, {}).get("common", {}).get(schema_type, {})
    schema_filepath = os.path.join(SCHEMA_SEARCH_PATH, format_details.get("schema"))

    file_key_path = Path(file_key)
    file_name_path = Path(file_name)
    canonical_key = os.path.join(
        str(file_key_path.parent).replace(file_format, f"CANONICAL/{file_format}/{file_type}"),
        f'{file_name_path.stem}_{datetime.now(timezone.utc).strftime("%Y_%m_%d_%H_%M_%S")}.json',
    )
    ingestion_bucket = Variable.get("DATA_INGESTION_BUCKET")
    src_path = f"{DOWNLOAD_ZIP_FILE_PATH}/{file_type}/{file_name}"
    dest_path = f"s3://{ingestion_bucket}/{canonical_key}"
    canonical_landing_path = f"s3a://{ingestion_bucket}/{canonical_key}"
    return {
        "file_key": file_key,
        "file_tenant": file_tenant,
        "file_source": file_source,
        "file_format": file_format,
        "file_batch_id": file_batch_id,
        "file_name": file_name,
        "file_type": file_type,
        "schema": schema_filepath,
        "src_path": src_path,
        "dest_path": dest_path,
        "canonical_landing_path": canonical_landing_path,
    }


def _convert_cclf_to_canonical(ti, local_path: str = DOWNLOAD_ZIP_FILE_PATH) -> tuple:
    map_index = ti.map_index
    init_group_dict = ti.xcom_pull(
        task_ids=["stage_load_group.init_group"],
        map_indexes=[map_index],
        key="return_value",
    )
    file_name = init_group_dict[0]["file_name"]
    file_type = init_group_dict[0]["file_type"]
    schema = init_group_dict[0]["schema"]
    src_path = init_group_dict[0]["src_path"]
    dest_path = init_group_dict[0]["dest_path"]
    file_key = init_group_dict[0]["file_key"]
    file_tenant = init_group_dict[0]["file_tenant"]
    if not os.path.isdir(local_path):
        os.makedirs(local_path)
    local_file_path = os.path.join(local_path, f"{file_type}")
    bucket_name = Variable.get("DATA_INGESTION_BUCKET")
    # client = boto3.client("s3")
    # response = client.get_object(Bucket=bucket_name, Key=file_key)
    # zip_content = response["Body"].read()
    # with zipfile.ZipFile(BytesIO(zip_content)) as zip_ref:
    #     zip_ref.extract(file_name, path=local_file_path)
    zip_file_name = Path(file_key).name
    src_file_path = f"s3://{bucket_name}/{file_key}"
    pipeline_data_key = get_data_key(file_tenant)
    decrypt_file_path = os.path.join(tempfile.gettempdir(), zip_file_name)
    decrypt_file(pipeline_data_key, src_file_path, decrypt_file_path)
    with zipfile.ZipFile(decrypt_file_path) as zip_ref:
        zip_ref.extract(file_name, path=local_file_path)
    resource_type = f"CCLF_{file_type}"
    metadata = {
        "file_tenant": ti.xcom_pull(task_ids="parse_config", key="file_tenant"),
        "file_source": ti.xcom_pull(task_ids="parse_config", key="file_source"),
        "file_format": ti.xcom_pull(task_ids="parse_config", key="file_format"),
        "file_batch_id": ti.xcom_pull(task_ids="parse_config", key="file_batch_id"),
        "resource_type": resource_type,
        "src_file_name": ti.xcom_pull(task_ids="parse_config", key="src_file_name"),
        "src_organization_id": ti.xcom_pull(task_ids="parse_config", key="src_organization_id"),
    }
    convert_raw_to_canonical(schema, src_path, dest_path, metadata)
    _rm_tmp_file(file_path=decrypt_file_path)
    return dest_path


def _prepare_run_template(ti):
    map_index = ti.map_index
    init_group_dict = ti.xcom_pull(
        task_ids=["stage_load_group.init_group"],
        map_indexes=[map_index],
        key="return_value",
    )
    file_tenant = init_group_dict[0]["file_tenant"]
    file_source = init_group_dict[0]["file_source"]
    file_format = init_group_dict[0]["file_format"]
    file_batch_id = init_group_dict[0]["file_batch_id"]
    file_key = init_group_dict[0]["file_key"]
    file_type = init_group_dict[0]["file_type"]
    file_name = init_group_dict[0]["file_name"]
    canonical_landing_path = init_group_dict[0]["canonical_landing_path"]

    if file_format.upper() != "CCLF":
        raise ValueError("Invalid file format")

    src_template = os.path.join(TMPL_SEARCH_PATH, "patient", "stageload", "cclf_stage_load_job.yaml")

    substitute = {
        "RANDOM_ID": get_random_string(),
        "CANONICAL_FILE_PATH": canonical_landing_path,
        "TENANT": file_tenant,
        "FILE_BATCH_ID": file_batch_id,
        "FILE_SOURCE": file_source,
        "FILE_NAME": f"{file_key}/{file_name}",
        "ENTITY": file_type.lower(),
    }
    job_definition = generate_job_definition(src_template, substitute)
    return job_definition


def _update_task_group_status(ti):
    map_index = ti.map_index
    canonical_return_value = ti.xcom_pull(
        task_ids=["stage_load_group.convert_raw_to_canonical"],
        map_indexes=[map_index],
        key="return_value",
    )
    landing_file_path = str(canonical_return_value[0])
    print(f"landing_file_path: {landing_file_path}")
    has_failure = False
    context = get_current_context()
    dag_run = context["dag_run"]
    task_instances = dag_run.get_task_instances()
    for task_instance in task_instances:
        if (
            task_instance.task_id in ["stage_load_group.stage_load_task", "stage_load_group.stage_load_monitor_task"]
            and task_instance.map_index == map_index
        ):
            state = task_instance.current_state()
            if state == TaskInstanceState.FAILED:
                has_failure = True
    bucket, landing_key = parse_s3_path(landing_file_path)
    if has_failure:
        target_key = str(landing_key).replace("landing", "error")
    else:
        target_key = str(landing_key).replace("landing", "processed")
    move_file_from_s3(
        src_bucket=bucket,
        src_key=landing_key,
        dest_bucket=bucket,
        dest_key=target_key,
    )


@dag(
    dag_id="cclf_stage_load_dag",
    start_date=datetime(day=2, year=2022, month=8),
    schedule=None,
    max_active_runs=1,
    concurrency=3,
    description="Loads canonical cclf data into staging table in delta lake",
    render_template_as_native_obj=True,
    template_searchpath=[TMPL_SEARCH_PATH],
    tags=["stageload", "cclf"],
)
def cclf_stage_load():
    @task(on_success_callback=publish_start_status, multiple_outputs=True)
    def parse_config():
        dag_run = get_current_context()["dag_run"]
        return {
            "file_key": dag_run.conf.get("file_key", ""),
            "file_tenant": dag_run.conf.get("file_tenant", ""),
            "file_source": dag_run.conf.get("file_source", ""),
            "file_format": dag_run.conf.get("file_format", ""),
            "file_type": dag_run.conf.get("file_type", ""),
            "resource_type": dag_run.conf.get("resource_type", ""),
            "file_batch_id": dag_run.conf.get("file_batch_id", ""),
            "src_file_name": dag_run.conf.get("src_file_name", ""),
            "src_organization_id": dag_run.conf.get("src_organization_id", ""),
            "src_location_id": dag_run.conf.get("src_location_id", ""),
            "src_practitioner_id": dag_run.conf.get("src_practitioner_id", ""),
        }

    @task
    def extract_file_names(file_key, file_tenant):
        file_names = []
        bucket_name = Variable.get("DATA_INGESTION_BUCKET")
        # client = boto3.client("s3")
        # response = client.get_object(Bucket=bucket_name, Key=file_key)
        # zip_file = response["Body"].read()
        # with zipfile.ZipFile(BytesIO(zip_file), "r") as zip_ref:
        #     file_names.extend(zip_ref.namelist())
        # return file_names
        zip_file_name = Path(file_key).name
        src_file_path = f"s3://{bucket_name}/{file_key}"
        pipeline_data_key = get_data_key(file_tenant)
        decrypt_file_path = os.path.join(tempfile.gettempdir(), zip_file_name)
        decrypt_file(pipeline_data_key, src_file_path, decrypt_file_path)
        try:
            zipped_file = zipfile.ZipFile(decrypt_file_path)
            file_names.extend(zipped_file.namelist())
        except Exception as e:
            raise ValueError(str(e))
        finally:
            _rm_tmp_file(file_path=decrypt_file_path)
        return file_names

    @task_group
    def stage_load_group(file_name):
        init_canonical_files = PythonOperator(
            task_id="init_group",
            python_callable=_init_cclf_canonical_files,
            op_kwargs={
                "file_name": file_name,
            },
            provide_context=True,
        )

        convert_raw_to_canonical = PythonOperator(
            task_id="convert_raw_to_canonical",
            python_callable=_convert_cclf_to_canonical,
            provide_context=True,
        )

        prepare_run_template = PythonOperator(
            task_id="prepare_run_template",
            python_callable=_prepare_run_template,
        )

        stage_load_task = SparkKubernetesOperator(
            task_id="stage_load_task",
            namespace="data-pipeline",
            application_file="{{ ti.xcom_pull(task_ids='stage_load_group.prepare_run_template',\
                map_indexes=ti.map_index) }}",
        )

        # monitors spark job submitted with timeout 3600 seconds
        stage_load_monitor_task = SparkKubernetesSensor(
            task_id="stage_load_monitor_task",
            namespace="data-pipeline",
            timeout=3600,
            application_name="{{ ti.xcom_pull(task_ids='stage_load_group.stage_load_task',\
                map_indexes=ti.map_index)['metadata']['name'] }}",
        )

        post_task_group_status = PythonOperator(
            task_id="post_task_group_status",
            python_callable=_update_task_group_status,
            provide_context=True,
            trigger_rule=TriggerRule.ALL_DONE,
        )

        (
            init_canonical_files
            >> convert_raw_to_canonical
            >> prepare_run_template
            >> stage_load_task
            >> stage_load_monitor_task
            >> post_task_group_status
        )

    @task
    def trigger_fhir_bundle():
        ti = get_current_context()["task_instance"]
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
        trigger_dag = "cclf_stage_to_fhirbundle_dag"
        trigger_dag_with_config(dag_to_trigger=trigger_dag, conf=fhir_config)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def post_dag_status():
        update_dag_status()

    config_dict = parse_config()
    file_names = extract_file_names(config_dict["file_key"], config_dict["file_tenant"])

    stage_load_group.expand(file_name=file_names) >> trigger_fhir_bundle() >> post_dag_status()


result = cclf_stage_load()
