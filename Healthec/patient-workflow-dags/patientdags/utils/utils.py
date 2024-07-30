import json
import logging
import os
import random
import string
import time
from base64 import b64decode
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import requests
import smart_open
import yaml
from airflow.exceptions import AirflowFailException
from airflow.models import DagRun, Variable
from airflow.operators.python import get_current_context
from airflow.utils.state import DagRunState, TaskInstanceState
from confluent_kafka import KafkaException, Producer
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from requests.auth import HTTPBasicAuth

from patientdags.utils.api_client import _get_k8s_secret, post_event_message
from patientdags.utils.aws_s3 import get_file_metadata_from_s3, move_file_from_s3
from patientdags.utils.constants import (
    AIRFLOW_ENDPOINT,
    AIRFLOW_USER,
    ANALYTICAL_FINANCIAL_DAG,
    ANALYTICAL_MEASURE_DAG,
    ANALYTICAL_PATIENT_DAG,
    CANONICAL_PATH_STR,
    CCD_FILE_FORMAT,
    DAG_TO_TRIGGER,
    ERROR_PATH_SUFIX,
    EVENT_COMPLETE,
    EVENT_FAILURE_STATUS,
    EVENT_START,
    EVENT_SUCCESS_STATUS,
    FHIRBUNDLE_FILE_FORMAT,
    FHIRBUNDLE_PATH_STR,
    HL7_FILE_FORMAT,
    LANDING_PATH_SUFIX,
    MEASURE_FILE_FORMAT,
    PIPELINE_SECRET_KEY_NAME,
    PROCESSED_PATH_SUFIX,
    SCHEMA_SEARCH_PATH,
    TENANT_MAPPING_FILE_PATH,
)
from patientdags.utils.error_codes import PatientDagsErrorCodes, publish_error_code
from patientdags.utils.models import EventMessage
from patientdags.utils.validation import validate_fields


def load_yaml_to_dict(filepath: str) -> dict:
    with open(filepath, "r", encoding="utf8") as file:
        return yaml.load(file, Loader=yaml.SafeLoader)


def create_run_template(src_file_path, dest_file_path, substitute: dict):
    with open(src_file_path, "r") as f:
        content = f.read()
    tmpl = string.Template(content)
    with open(dest_file_path, "w") as f:
        f.write(tmpl.substitute(substitute))
    return


def get_random_string(size=16):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=size))


def parse_s3_path(path: str) -> tuple:
    return path.replace("s3://", "").replace("s3a://", "").split("/", 1)


def _parse_file_details(file_key: str):
    file_key_split = file_key.replace("files/", "").replace("CANONICAL/", "").split("/landing/")
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


def _get_source_config(tenant, source) -> dict:
    config_file_path = os.path.join(TENANT_MAPPING_FILE_PATH, f"{tenant}_schema_mapping.yaml")
    config = {}
    with open(config_file_path, "r") as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)
    source_config = config.get(tenant, {}).get(source, {})
    required_config = {}
    required_config["src_organization_id"] = source_config.get("src_organization_id", "")
    required_config["src_location_id"] = source_config.get("src_location_id", "")
    required_config["src_practitioner_id"] = source_config.get("src_practitioner_id", "")
    return required_config


def trigger_clinical_dag(file_keys: List, dag_to_trigger: str):
    """
    Triggers clinical dags
    """
    for file_key in file_keys:
        if not file_key.strip().endswith("/"):
            conf = _parse_file_details(file_key)
            file_tenant = conf.get("file_tenant", "")
            file_source = conf.get("file_source", "")
            src_config = _get_source_config(file_tenant, file_source)
            conf.update(src_config)
            conf.update({"src_file_name": os.path.basename(file_key), "file_batch_id": get_random_string()})
            trigger_dag_with_config(dag_to_trigger, conf)


def trigger_financial_dag(file_keys: List, dag_to_trigger: str = ""):
    """
    Triggers financial dags
    """
    for file_key in file_keys:
        if not file_key.strip().endswith("/"):
            conf = _parse_file_details(file_key)
            file_tenant = conf.get("file_tenant", "")
            file_source = conf.get("file_source", "")
            file_format = conf.get("file_format", "")
            src_config = _get_source_config(file_tenant, file_source)
            conf.update(src_config)
            conf.update({"src_file_name": os.path.basename(file_key), "file_batch_id": get_random_string()})
            if not dag_to_trigger and file_format:
                dag_to_trigger = DAG_TO_TRIGGER.get(file_format.upper())
            trigger_dag_with_config(dag_to_trigger, conf)


def trigger_fhirbundle_dag(file_keys: List, dag_to_trigger: str):
    """
    Triggers fhirbundle processing dag
    """
    for file_key in file_keys:
        if not file_key.strip().endswith("/"):
            conf = _parse_file_details(file_key)
            file_tenant = conf.get("file_tenant", "")
            file_source = conf.get("file_source", "")
            src_config = _get_source_config(file_tenant, file_source)
            conf.update(src_config)
            ingestion_bucket = Variable.get("DATA_INGESTION_BUCKET")
            file_metadata = get_file_metadata_from_s3(ingestion_bucket, file_key)
            conf.update(file_metadata)
            trigger_dag_with_config(dag_to_trigger, conf)


def trigger_parsing_dag(file_keys: List, dag_to_trigger: str):
    """
    Triggers specified dag
    """
    for file_key in file_keys:
        if not file_key.strip().endswith("/"):
            file_path = Path(file_key)
            if str(file_key).endswith("/") or len(file_path.parts) not in (5, 6, 7):
                raise ValueError("Invalid file key")

            file_format = file_path.parts[1]
            file_type = ""
            file_source = ""
            if len(file_path.parts) == 7:
                file_type = file_path.parts[2]
                file_tenant = file_path.parts[4]
                file_source = file_path.parts[5]
            elif len(file_path.parts) == 5:
                file_tenant = file_path.parts[3]
            else:
                file_tenant = file_path.parts[3]
                file_source = file_path.parts[4]

            src_config = _get_source_config(file_tenant, file_source)
            conf = {
                "file_key": file_key,
                "file_tenant": file_tenant,
                "file_source": file_source,
                "file_format": file_format,
                "file_type": file_type,
            }
            conf.update(src_config)
            trigger_dag_with_config(dag_to_trigger, conf)


def trigger_dag_with_config(dag_to_trigger: str, conf: Dict):
    response = requests.post(
        url=f'{os.getenv("AIRFLOW_ENDPOINT", AIRFLOW_ENDPOINT)}/api/v1/dags/{dag_to_trigger}/dagRuns',
        auth=HTTPBasicAuth(Variable.get("AIRFLOW_USERNAME"), Variable.get("AIRFLOW_PASSWORD")),
        json={"conf": conf},
    )
    logging.info(response.json())


def list_files_from_messages(messages, restrict_ext=[]):
    """
    Extracts S3 file path from event messages

    :param messages: Event messages from SQS
    """
    file_names = []
    for message in messages:
        body = json.loads(message["Body"])
        file_name = str(body["detail"]["object"]["key"])
        if file_name == "None" or file_name.endswith("/"):
            continue
        restrict = False
        if restrict_ext:
            for ext in restrict_ext:
                if file_name.endswith(ext):
                    restrict = True
                    break
        if restrict:
            continue
        file_names.append(file_name)
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


def initialize_target_files(ti):
    # initialize target fhirbundle, processed and error keys
    file_key = ti.xcom_pull(task_ids="parse_config", key="file_key")
    file_exact_name = Path(file_key).stem
    processed_key = os.path.join(
        os.path.dirname(file_key).replace(LANDING_PATH_SUFIX, PROCESSED_PATH_SUFIX),
        os.path.basename(file_key),
    )
    error_key = os.path.join(
        os.path.dirname(file_key).replace(LANDING_PATH_SUFIX, ERROR_PATH_SUFIX),
        os.path.basename(file_key),
    )
    ti.xcom_push(key="processed_key", value=processed_key)
    ti.xcom_push(key="error_key", value=error_key)
    file_format = ti.xcom_pull(task_ids="parse_config", key="file_format")
    if file_format not in (FHIRBUNDLE_FILE_FORMAT, MEASURE_FILE_FORMAT):
        if file_format == HL7_FILE_FORMAT:
            file_type = ti.xcom_pull(task_ids="parse_config", key="file_type")
            fhirbundle_key = os.path.join(
                os.path.dirname(file_key).replace(f"{file_format}/{file_type}", FHIRBUNDLE_PATH_STR),
                file_exact_name + "_" + datetime.now(timezone.utc).strftime("%Y_%m_%d_%H_%M_%S") + ".json",
            )
        else:
            fhirbundle_key = os.path.join(
                os.path.dirname(file_key).replace(file_format, FHIRBUNDLE_PATH_STR),
                file_exact_name + "_" + datetime.now(timezone.utc).strftime("%Y_%m_%d_%H_%M_%S") + ".json",
            )
        ti.xcom_push(key="fhirbundle_key", value=fhirbundle_key)
    if file_format in (CCD_FILE_FORMAT, HL7_FILE_FORMAT):
        canonical_key = os.path.join(
            os.path.dirname(processed_key).replace(file_format, f"{CANONICAL_PATH_STR}/{file_format}"),
            file_exact_name + "_" + datetime.now(timezone.utc).strftime("%Y_%m_%d_%H_%M_%S") + ".json",
        )
        ti.xcom_push(key="canonical_key", value=canonical_key)


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
        publish_error_code(PatientDagsErrorCodes.KAFKA_EVENT_PUBLISH_ERROR.value)
        raise AirflowFailException(e)


def publish_start_status(context):
    ti = context["task_instance"]
    event_message = EventMessage(EVENT_START, EVENT_SUCCESS_STATUS)
    event_message = update_event_with_dag_info(ti, event_message)
    event_message.links = {}
    file_key = ti.xcom_pull(task_ids="parse_config", key="file_key")
    if file_key:
        event_message.links = {"self": f"s3://{Variable.get('DATA_INGESTION_BUCKET')}/{file_key}"}
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
        if not processed_key:
            processed_key = str(landing_key).replace("landing", "processed")
        move_file_from_s3(
            src_bucket=landing_bucket,
            src_key=landing_key,
            dest_bucket=processed_bucket,
            dest_key=processed_key,
        )
    # publish success event message
    event_message = EventMessage(EVENT_COMPLETE, EVENT_SUCCESS_STATUS)
    event_message = update_event_with_dag_info(ti, event_message)
    event_message.links = {}
    if file_key:
        event_message.links = {"self": f"s3://{processed_bucket}/{processed_key}"}
    logging.info(f"publishing status: {event_message}")
    # TODO: Enable event post in Kafka topic
    # publish_status(event_message.__str__)
    post_event_message(event_message)


def publish_failure_status(context):
    ti = context["task_instance"]
    # move file from landing to error directory
    file_key = ti.xcom_pull(task_ids="parse_config", key="file_key")
    if file_key:
        landing_bucket = Variable.get("DATA_INGESTION_BUCKET")
        landing_key = ti.xcom_pull(task_ids="parse_config", key="file_key")
        error_bucket = Variable.get("DATA_INGESTION_BUCKET")
        error_key = ti.xcom_pull(task_ids="init_dag", key="error_key")
        if not error_key:
            error_key = str(landing_key).replace("landing", "error")
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
    event_message.status_code = error_code if error_code else ""
    event_message.links = {}
    if file_key:
        event_message.links = {"self": f"s3://{error_bucket}/{error_key}"}
    logging.info(f"publishing status: {event_message}")
    # TODO: Enable event post in Kafka topic
    # publish_status(event_message.__str__)
    post_event_message(event_message)
    # # set dag state to `failed`
    # dag_run = context["dag_run"]
    # dag_run.set_state(DagRunState.FAILED)


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
        publish_failure_status(context)
        dag_run = context["dag_run"]
        dag_run.set_state(DagRunState.FAILED)
    else:
        publish_success_status(context)
        dag_run = context["dag_run"]
        dag_run.set_state(DagRunState.SUCCESS)

    # context = get_current_context()
    # dag_run = context["dag_run"]
    # task_instances = dag_run.get_task_instances()
    # has_failure = False
    # for task_instance in task_instances:
    #     print(f"task id: {task_instance.task_id}, status: {task_instance.current_state()}")
    #     state = task_instance.current_state()
    #     if state == "failed":
    #         has_failure = True
    #         break
    # if has_failure:
    #     publish_failure_status(context)
    #     dag_run = context["dag_run"]
    #     dag_run.set_state(DagRunState.FAILED)
    # else:
    #     publish_success_status(context)
    #     dag_run = context["dag_run"]
    #     dag_run.set_state(DagRunState.SUCCESS)


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


def generate_job_definition(file_path, substitute: dict):
    with open(file_path, "r") as f:
        content = f.read()
    tmpl = string.Template(content)
    return tmpl.substitute(substitute)


def trigger_stage_load_dag(file_keys: List):
    """
    Triggers stage load dags based on file format and type
    """
    for file_key in file_keys:
        if not file_key.strip().endswith("/"):
            conf = _parse_file_details(file_key)
            file_format = conf.get("file_format", "")
            dag_to_trigger = DAG_TO_TRIGGER.get(file_format)
            if not dag_to_trigger:
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
        if not file_key.strip().endswith("/"):
            conf = _parse_file_details(file_key)
            file_type = conf.get("file_type", "")
            if file_type.upper() == "PATIENT":
                dag_to_trigger = ANALYTICAL_PATIENT_DAG
            elif file_type.upper() == "FINANCIAL":
                dag_to_trigger = ANALYTICAL_FINANCIAL_DAG
            elif file_type.upper() == "MEASURE":
                dag_to_trigger = ANALYTICAL_MEASURE_DAG
            else:
                raise ValueError("Invalid file type")
            trigger_dag_with_config(dag_to_trigger, conf)


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


def is_empty(value: str):
    if not value or value == "None":
        return True
    return False


def remove_duplicates(data):
    """
    Recursively removes duplicate elements from lists within dictionaries.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, list):
                data[key] = [x for i, x in enumerate(value) if x not in value[:i]]
            elif isinstance(value, dict):
                remove_duplicates(value)
    return data


def get_data_key(file_tenant: str) -> str:
    get_secret_key = _get_k8s_secret(name=f"{file_tenant}-data-key")
    secret_key = b64decode(get_secret_key.get(PIPELINE_SECRET_KEY_NAME, "")).decode("utf-8")
    return secret_key


def decrypt_file(data_key: str, encrypt_path: str, orig_path: str) -> str:
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
    return orig_path
