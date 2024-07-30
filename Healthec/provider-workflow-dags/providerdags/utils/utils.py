import json
import logging
import os
import random
import string
from base64 import b64decode, b64encode
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import requests
import smart_open
import yaml
from airflow.exceptions import AirflowFailException
from airflow.models import DagRun, Variable
from airflow.operators.python import get_current_context
from airflow.utils.state import DagRunState
from confluent_kafka import KafkaException, Producer
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from requests.auth import HTTPBasicAuth

from providerdags.utils.api_client import post_event_message
from providerdags.utils.aws_s3 import get_file_metadata_from_s3, move_file_from_s3
from providerdags.utils.constants import (
    AIRFLOW_ENDPOINT,
    AIRFLOW_USER,
    ANALYTICAL_PROVIDER_DAG,
    CANONICAL_PATH_STR,
    ERROR_PATH_SUFIX,
    EVENT_COMPLETE,
    EVENT_FAILURE_STATUS,
    EVENT_START,
    EVENT_SUCCESS_STATUS,
    LANDING_PATH_SUFIX,
    PRACTICE_STAGE_LOAD_DAG,
    PROCESSED_PATH_SUFIX,
    SCHEMA_SEARCH_PATH,
    SOURCE_TO_CANONICAL_DAG,
)
from providerdags.utils.error_codes import ProviderDagsErrorCodes, publish_error_code
from providerdags.utils.models import EventMessage
from providerdags.utils.validation import validate_fields


def load_yaml_to_dict(filepath: str) -> dict:
    with smart_open.open(filepath, "r", encoding="utf8") as file:
        return yaml.load(file, Loader=yaml.SafeLoader)


def create_run_template(src_file_path, dest_file_path, substitute: dict):
    with smart_open.open(src_file_path, "r") as f:
        content = f.read()
    tmpl = string.Template(content)
    with smart_open.open(dest_file_path, "w") as f:
        f.write(tmpl.substitute(substitute))
    return


def parse_s3_path(path: str) -> tuple:
    return path.replace("s3://", "").replace("s3a://", "").split("/", 1)


def parse_file_details(file_key: str, split_key: str = "landing"):
    file_key_dir = os.path.dirname(file_key)
    file_key_split = file_key_dir.replace("files/", "").replace("CANONICAL/", "").split(f"/{split_key}/")
    if len(file_key_split) != 2:
        raise ValueError("Invalid file key")
    file_format_split = file_key_split[0].split("/")
    file_format = file_format_split[0] if len(file_format_split) > 0 else ""
    file_type = file_format_split[1] if len(file_format_split) > 1 else ""
    resource_type = "_".join([file_format, file_type]) if file_type else file_format

    file_tenant_split = file_key_split[1].split("/")
    file_tenant = file_tenant_split[0] if len(file_tenant_split) > 0 else ""
    file_source = file_tenant_split[1] if len(file_tenant_split) > 1 else ""
    file_details = {
        "file_key": file_key,
        "file_tenant": file_tenant,
        "file_source": file_source,
        "file_format": file_format,
        "file_type": file_type,
        "resource_type": resource_type,
    }
    return file_details


def trigger_canonical_dag(file_keys: List):
    """
    Triggers specified dag
    """
    for file_key in file_keys:
        if not file_key.strip().lower().endswith(("/", ".dmy")):
            conf = parse_file_details(file_key)
            src_file_name = os.path.basename(file_key)
            conf.update(
                {
                    "file_batch_id": get_random_string(),
                    "src_file_name": src_file_name,
                }
            )
            dag_to_trigger = SOURCE_TO_CANONICAL_DAG
            trigger_dag_with_config(dag_to_trigger, conf)


def trigger_stage_load_dag(file_keys: List):
    """
    Triggers stage load dags based on file format and type
    """
    for file_key in file_keys:
        if not file_key.strip().lower().endswith(("/", ".dmy")):
            conf = parse_file_details(file_key)
            file_format = conf.get("file_format", "")
            if file_format == "PRACTICE":
                dag_to_trigger = PRACTICE_STAGE_LOAD_DAG
            else:
                raise ValueError("Unsupported file format")
            ingestion_bucket = Variable.get("DATA_INGESTION_BUCKET")
            file_metadata = get_file_metadata_from_s3(ingestion_bucket, file_key)
            conf.update(file_metadata)
            trigger_dag_with_config(dag_to_trigger, conf)


def trigger_analytical_load_dag(file_keys: List):
    """
    Triggers analytical load dags based on file format and type
    """
    for file_key in file_keys:
        if not file_key.strip().lower().endswith(("/", ".dmy")):
            conf = parse_file_details(file_key)
            dag_to_trigger = ANALYTICAL_PROVIDER_DAG
            trigger_dag_with_config(dag_to_trigger, conf)


def trigger_dag_with_config(dag_to_trigger: str, conf: Dict):
    response = requests.post(
        url=f'{os.getenv("AIRFLOW_ENDPOINT", AIRFLOW_ENDPOINT)}/api/v1/dags/{dag_to_trigger}/dagRuns',
        auth=HTTPBasicAuth(Variable.get("AIRFLOW_USERNAME"), Variable.get("AIRFLOW_PASSWORD")),
        json={"conf": conf},
    )
    logging.info(response.json())


def list_files_from_messages(messages):
    """
    Extracts S3 file path from event messages

    :param messages: Event messages from SQS
    """
    file_names = []
    for message in messages:
        body = json.loads(message["Body"])
        file_names.append(str(body["detail"]["object"]["key"]))
    return file_names


def get_most_recent_dag_run_time(dag_id):
    """
    Checks timestamp for the previous dag run.

    :param dag_id: (str) Id of the dag to check
    :return: returns timestamp of previous dag or 0
    """
    dag_runs = DagRun.find(dag_id=dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    return dag_runs[1].execution_date if dag_runs and len(dag_runs) >= 2 else 0


def read_conf(ti):
    dag_run = get_current_context()["dag_run"]
    file_key = dag_run.conf.get("file_key", "")
    ti.xcom_push(key="file_key", value=file_key)
    ti.xcom_push(key="file_tenant", value=dag_run.conf.get("file_tenant", ""))
    ti.xcom_push(key="file_source", value=dag_run.conf.get("file_source", ""))
    ti.xcom_push(key="file_format", value=dag_run.conf.get("file_format", ""))
    ti.xcom_push(key="file_type", value=dag_run.conf.get("file_type", ""))
    ti.xcom_push(key="resource_type", value=dag_run.conf.get("resource_type", ""))
    ti.xcom_push(key="file_batch_id", value=dag_run.conf.get("file_batch_id", ""))
    ti.xcom_push(key="src_file_name", value=dag_run.conf.get("src_file_name", ""))
    ti.xcom_push(key="src_organization_id", value=dag_run.conf.get("src_organization_id", ""))
    ti.xcom_push(key="src_location_id", value=dag_run.conf.get("src_location_id", ""))
    ti.xcom_push(key="src_practitioner_id", value=dag_run.conf.get("src_practitioner_id", ""))
    return file_key


def init_files_for_run(ti):
    file_key = ti.xcom_pull(task_ids="parse_config", key="file_key")
    path = Path(file_key)
    processed_key = os.path.join(
        str(path.parent).replace(LANDING_PATH_SUFIX, PROCESSED_PATH_SUFIX),
        path.name,
    )
    error_key = os.path.join(
        str(path.parent).replace(LANDING_PATH_SUFIX, ERROR_PATH_SUFIX),
        path.name,
    )
    ti.xcom_push(key="processed_key", value=processed_key)
    ti.xcom_push(key="error_key", value=error_key)


def init_canonical_files_for_run(ti):
    # initialize target canonical, processed and error keys
    file_tenant = ti.xcom_pull(task_ids="parse_config", key="file_tenant")
    file_source = ti.xcom_pull(task_ids="parse_config", key="file_source")
    file_key = ti.xcom_pull(task_ids="parse_config", key="file_key")
    file_format = ti.xcom_pull(task_ids="parse_config", key="file_format")
    resource_type = ti.xcom_pull(task_ids="parse_config", key="resource_type")

    # load tenant based schema mapping
    schema_mapping_filepath = os.path.join(SCHEMA_SEARCH_PATH, f"{file_tenant}_schema_mapping.yaml")
    schema_mapping = load_yaml_to_dict(schema_mapping_filepath)

    source_config = {}
    if file_source:
        source_config = schema_mapping.get(file_tenant, {}).get(file_source, {})
    else:
        source_config = schema_mapping.get(file_tenant, {}).get("common", {})
    schema_filepath = os.path.join(SCHEMA_SEARCH_PATH, source_config.get(resource_type, {}).get("schema"))
    src_organization_id = source_config.get("src_organization_id", "")

    path = Path(file_key)
    processed_key = os.path.join(
        str(path.parent).replace(LANDING_PATH_SUFIX, PROCESSED_PATH_SUFIX),
        path.name,
    )
    error_key = os.path.join(
        str(path.parent).replace(LANDING_PATH_SUFIX, ERROR_PATH_SUFIX),
        path.name,
    )
    canonical_key = os.path.join(
        str(path.parent).replace(file_format, f"{CANONICAL_PATH_STR}/{file_format}"),
        f'{path.stem}_{datetime.now(timezone.utc).strftime("%Y_%m_%d_%H_%M_%S")}.json.gz',
    )
    ingestion_bucket = Variable.get("DATA_INGESTION_BUCKET")
    src_path = f"s3://{ingestion_bucket}/{file_key}"
    dest_path = f"s3://{ingestion_bucket}/{canonical_key}"

    ti.xcom_push(key="schema", value=schema_filepath)
    ti.xcom_push(key="src_organization_id", value=src_organization_id)
    ti.xcom_push(key="src_path", value=src_path)
    ti.xcom_push(key="dest_path", value=dest_path)
    ti.xcom_push(key="processed_key", value=processed_key)
    ti.xcom_push(key="error_key", value=error_key)


def publish_status(event_message):
    try:
        logging.info(f"publishing status: {event_message}")
        producer = Producer({"bootstrap.servers": Variable.get("KAFKA_SERVERS")})
        producer.produce(Variable.get("DATA_INGESTION_KAFKA_TOPIC"), event_message)
        producer.poll(10)
        producer.flush()
        return
    except KafkaException as e:
        logging.error(e)
        publish_error_code(ProviderDagsErrorCodes.KAFKA_EVENT_PUBLISH_ERROR.value)
        raise AirflowFailException(e)


def publish_start_status(context):
    ti = context["task_instance"]
    event_message = EventMessage(EVENT_START, EVENT_SUCCESS_STATUS)
    event_message = update_event_with_dag_info(ti, event_message)
    event_message.links = {
        "self": f"s3://{Variable.get('DATA_INGESTION_BUCKET')}/{ti.xcom_pull(task_ids='parse_config', key='file_key')}"
    }
    logging.info(f"publishing status: {event_message}")
    # TODO: Enable event post in Kafka topic
    # publish_status(event_message.__str__)
    post_event_message(event_message)


def publish_success_status(context):
    ti = context["task_instance"]
    # move file from landing to processed directory
    file_key = ti.xcom_pull(task_ids="parse_config", key="file_key")
    if file_key:
        landing_bucket = Variable.get("DATA_INGESTION_BUCKET")
        landing_key = ti.xcom_pull(task_ids="parse_config", key="file_key")
        processed_bucket = Variable.get("DATA_INGESTION_BUCKET")
        processed_key = ti.xcom_pull(task_ids="init_dag", key="processed_key")
        move_file_from_s3(
            src_bucket=landing_bucket,
            src_key=landing_key,
            dest_bucket=processed_bucket,
            dest_key=processed_key,
        )
    # publish success event message
    event_message = EventMessage(EVENT_COMPLETE, EVENT_SUCCESS_STATUS)
    event_message = update_event_with_dag_info(ti, event_message)
    if file_key:
        event_message.links = {"self": f"s3://{processed_bucket}/{processed_key}"}
    logging.info(f"publishing status: {event_message}")
    # TODO: Enable event post in Kafka topic
    # publish_status(event_message.__str__)
    post_event_message(event_message)


def publish_failure_status(**context):
    ti = context["task_instance"]
    # move file from landing to error directory
    file_key = ti.xcom_pull(task_ids="parse_config", key="file_key")
    if file_key:
        landing_bucket = Variable.get("DATA_INGESTION_BUCKET")
        landing_key = ti.xcom_pull(task_ids="parse_config", key="file_key")
        error_bucket = Variable.get("DATA_INGESTION_BUCKET")
        error_key = ti.xcom_pull(task_ids="init_dag", key="error_key")
        move_file_from_s3(
            src_bucket=landing_bucket,
            src_key=landing_key,
            dest_bucket=error_bucket,
            dest_key=error_key,
        )
    # publish error event message
    error_code = ti.xcom_pull(key="error_code")
    event_message = EventMessage(EVENT_COMPLETE, EVENT_FAILURE_STATUS)
    event_message = update_event_with_dag_info(ti, event_message)
    event_message.status_code = error_code
    if file_key:
        event_message.links = {"self": f"s3://{error_bucket}/{error_key}"}
    logging.info(f"publishing status: {event_message}")
    # TODO: Enable event post in Kafka topic
    # publish_status(event_message.__str__)
    post_event_message(event_message)
    # set dag state to `failed`
    # dag_run = context["dag_run"]
    # dag_run.set_state(DagRunState.FAILED)


def update_dag_status():
    context = get_current_context()
    dag_run = context["dag_run"]
    task_instances = dag_run.get_task_instances()
    has_failure = False
    for task_instance in task_instances:
        state = task_instance.current_state()
        if state == "failed":
            has_failure = True
            break
    if has_failure:
        publish_failure_status(context)
        dag_run = context["dag_run"]
        dag_run.set_state(DagRunState.FAILED)
    else:
        publish_success_status(context)
        dag_run = context["dag_run"]
        dag_run.set_state(DagRunState.SUCCESS)


def update_event_with_dag_info(ti, event_message: EventMessage):
    src_file_name = ti.xcom_pull(task_ids="parse_config", key="src_file_name")
    file_key = ti.xcom_pull(task_ids="parse_config", key="file_key")
    file_name = src_file_name if src_file_name else os.path.basename(file_key)
    file_format = ti.xcom_pull(task_ids="parse_config", key="file_format")
    file_type = ti.xcom_pull(task_ids="parse_config", key="file_type")
    resource_type = ti.xcom_pull(task_ids="parse_config", key="resource_type")
    if not resource_type:
        resource_type = f"{file_format}_{file_type}" if file_type else file_format
    tenant_id = ti.xcom_pull(task_ids="parse_config", key="file_tenant")
    file_source = ti.xcom_pull(task_ids="parse_config", key="file_source")
    file_batch_id = ti.xcom_pull(task_ids="parse_config", key="file_batch_id")

    resource_id = ""
    if file_source and file_name:
        resource_id = f"{file_source}/{file_name}"
    elif file_name:
        resource_id = file_name

    # update event message with dag run details
    event_message.origin = ti.dag_id
    event_message.trace_id = ti.run_id
    event_message.user_id = AIRFLOW_USER
    event_message.resource_id = resource_id
    event_message.file_name = file_name
    event_message.resource_type = resource_type
    event_message.tenant_id = tenant_id
    event_message.batch_id = file_batch_id
    logging.info(f"event_message: {event_message}")
    return event_message


def get_random_string(size=16):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=size))


def generate_job_definition(file_path, substitute: dict):
    with smart_open.open(file_path, "r") as f:
        content = f.read()
    tmpl = string.Template(content)
    return tmpl.substitute(substitute)


def get_validation_results(ti):
    validation_response = None
    try:
        # required details to get valdation configuration path
        file_tenant = ti.xcom_pull(task_ids="parse_config", key="file_tenant")
        file_source = ti.xcom_pull(task_ids="parse_config", key="file_source")
        file_path = ti.xcom_pull(task_ids="parse_config", key="file_key")
        file_format = ti.xcom_pull(task_ids="parse_config", key="file_format")
        file_type = ti.xcom_pull(task_ids="parse_config", key="file_type")
        schema_type = f"{file_format}_{file_type}" if file_type else file_format

        # Added for tenant based validation schema map
        schema_mapping_filepath = os.path.join(SCHEMA_SEARCH_PATH, f"{file_tenant}_schema_mapping.yaml")
        schema_mapping = load_yaml_to_dict(schema_mapping_filepath)
        format_details = {}
        if file_source:
            format_details = schema_mapping.get(file_tenant, {}).get(file_source, {}).get(schema_type, {})
        else:
            format_details = schema_mapping.get(file_tenant, {}).get("common", {}).get(schema_type, {})
        fhir_script_name = format_details.get("fhir_bundle")
        validation_config_path = os.path.join(SCHEMA_SEARCH_PATH, format_details.get("validation"))

        ingestion_bucket = Variable.get("DATA_INGESTION_BUCKET")
        data_file_path = f"s3a://{ingestion_bucket}/{file_path}"
        validation_response = validate_fields(config_file_path=validation_config_path, data_file_path=data_file_path)
        ti.xcom_push(key="fhir_script_name", value=fhir_script_name)
    except ValueError as error:
        logging.error(f"Validation Error: {error}")
    logging.info(f"Validation status: {validation_response}")
    return validation_response


def encrypt_file(data_key: str, orig_path: str, encrypt_path: str):
    key = bytes(data_key, "utf-8")
    iv = os.urandom(12)
    encryptor = Cipher(
        algorithms.AES(key),
        modes.GCM(iv),
    ).encryptor()
    with smart_open.open(orig_path, "rb") as orig_file, smart_open.open(encrypt_path, "wb") as encrypt_file:
        encoded_iv = b64encode(iv)
        encrypt_file.write(encoded_iv + b"\n")
        while True:
            chunk = orig_file.read(4096)
            if not chunk:
                break
            encrypted_chunk = encryptor.update(chunk)
            encoded_chunk = b64encode(encrypted_chunk)
            encrypt_file.write(encoded_chunk + b"\n")
        final_chunk = encryptor.finalize()
        encrypt_file.write(final_chunk)
    return


def decrypt_file(data_key: str, encrypt_path: str, orig_path: str):
    key = bytes(data_key, "utf-8")
    with smart_open.open(encrypt_path, "rb") as encrypt_file, smart_open.open(orig_path, "wb") as orig_file:
        encoded_iv = encrypt_file.readline()
        iv = b64decode(encoded_iv.rstrip())
        decryptor = Cipher(
            algorithms.AES(key),
            modes.GCM(iv),
        ).decryptor()
        while True:
            line = encrypt_file.readline()
            if not line:
                break
            decoded_text = b64decode(line.rstrip())
            chunk = decryptor.update(decoded_text)
            orig_file.write(chunk)
    return
