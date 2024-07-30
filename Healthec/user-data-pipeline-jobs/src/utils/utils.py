import json
import os
import random
import re
import string
import sys
import uuid
import boto3
import logging
import smart_open
import yaml

from typing import Dict, List
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from base64 import b64encode
from utils.constants import MSSQL, MSSQL_DRIVER, POSTGRES, POSTGRES_DRIVER
from utils.enums import ResourceType


def exit_with_error(log, message):
    log.error(message)
    sys.exit(1)


def find_jdbc_driver(db_protocol):
    if db_protocol == POSTGRES:
        return POSTGRES_DRIVER
    elif db_protocol == MSSQL:
        return MSSQL_DRIVER


def remove_none_fields(value):
    """
    Recursively remove all None values from dictionaries and lists, and returns
    the result as a new dictionary or list.
    """
    if isinstance(value, list):
        return [remove_none_fields(x) for x in value if x is not None]
    elif isinstance(value, dict):
        return {key: remove_none_fields(val) for key, val in value.items() if val is not None}
    else:
        return value


def select_query_builder(fields, table):
    return "(SELECT " + ",".join(fields) + " FROM " + table + ") data"


def load_config(file_path: str) -> Dict:
    config = {}
    if file_path and os.path.isfile(file_path):
        with open(file_path, "r") as f:
            config = yaml.load(f, Loader=yaml.SafeLoader)
    return config


def generate_random_string(size: int = 16):
    return "".join(random.choices(string.ascii_letters + string.digits, k=size))


def write_to_file(data: Dict, filepath: str, metadata: str = ""):
    if metadata:
        # add metadata to target S3 file
        transport_params = {"client_kwargs": {"S3.Client.create_multipart_upload": {"Metadata": json.loads(metadata)}}}
        dest_file = smart_open.open(filepath, "w", transport_params=transport_params)
    else:
        dest_file = smart_open.open(filepath, "w")
    json.dump(data, dest_file, indent=4)
    return


def resolve_fhir_resource_references(resources: List[str]):
    updated_resources = []
    reference_uuids = {}
    references = []
    for resource in resources:
        for resource_type in ResourceType:
            search_regex = resource_type.value + r"_\w{8}-\w{4}-\w{4}-\w{4}-\w{12}_\w{1}"
            # search for resource temporary id (ex. "id": "Encounter_74feb26c-61b1-440b-a698-ce71c737af55_1")
            id_search_regex = r"\"id\":\s+\"" + search_regex + r"\""
            if re.search(id_search_regex, resource):
                references.append(re.search(search_regex, resource).group())

    for reference in list(set(references)):
        reference_uuids[reference] = str(uuid.uuid4())
    print(f"reference_uuids: {reference_uuids}")

    for resource in resources:
        for reference, uuid_val in reference_uuids.items():
            resource = resource.replace(reference, uuid_val)

        # clean up references which does not have original resource
        for resource_type in ResourceType:
            replace_regex = resource_type.value + r"/" + resource_type.value + r"_\w{8}-\w{4}-\w{4}-\w{4}-\w{12}_\w{1}"
            resource = re.sub(replace_regex, "", resource)
            resource = json.dumps(remove_none_fields(json.loads(resource)))

        updated_resources.append(resource)
    return updated_resources


def prepare_fhir_bundle(bundle_id: str, resources: List[str], file_dir: str = None, metadata: str = ""):
    bundle = {"resourceType": "Bundle", "id": bundle_id, "type": "batch", "entry": []}
    updated_resources = resolve_fhir_resource_references(resources)

    for resource in updated_resources:
        resource = json.loads(resource)
        entry_uuid = resource.get("id")
        entry = {
            "fullUrl": f"urn:uuid:{entry_uuid}",
            "resource": resource,
            "request": {
                "method": "PUT",
                "url": f"{resource.get('resourceType')}/{entry_uuid}",
            },
        }
        bundle["entry"].append(entry)

    # write the fhir bundle to target location
    if file_dir:
        file_path = os.path.join(file_dir, f"{bundle_id}.json")
        write_to_file(bundle, file_path, metadata)

    return bundle_id


def parse_file_details(file_path: str) -> str:
    split_file_path = file_path.split("/")
    return split_file_path[2], "/".join(split_file_path[3:])


def list_files_from_s3(bucket: str, prefix: str):
    file_list = []
    client = boto3.client("s3")
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if response:
        file_list = response.get("Contents")
    return file_list


def encrypt_file(encrypt_data_key: str, temp_path: str, encrypt_path: str, metadata: dict):
    key = bytes(encrypt_data_key, "utf-8")
    iv = os.urandom(12)
    encryptor = Cipher(
        algorithms.AES(key),
        modes.GCM(iv),
    ).encryptor()
    transport_params = {}
    if metadata:
        # add metadata to target S3 file
        transport_params = {"client_kwargs": {"S3.Client.create_multipart_upload": {"Metadata": metadata}}}
    with smart_open.open(temp_path, "rb") as temp_file, smart_open.open(
        encrypt_path, "wb", transport_params=transport_params
    ) as encrypt_file:
        encoded_iv = b64encode(iv)
        encrypt_file.write(encoded_iv + b"\n")
        while True:
            chunk = temp_file.read(4096)
            if not chunk:
                break
            encrypted_chunk = encryptor.update(chunk)
            encoded_chunk = b64encode(encrypted_chunk)
            encrypt_file.write(encoded_chunk + b"\n")
        final_chunk = encryptor.finalize()
        encrypt_file.write(final_chunk)
    return


def _construct_bundle_file_path(src_bucket: str, dest_bucket: str, temp_file_key: str, dest_prefix: str) -> tuple:
    src_path = f"s3://{src_bucket}/{temp_file_key}"
    dest_key = os.path.join(dest_prefix, f"{str(uuid.uuid4())}.ndjson")
    dest_path = f"s3://{dest_bucket}/{dest_key}"
    return src_path, dest_path


def upload_bundle_files(fhir_bundle_temp_path: str, landing_path: str, metadata: dict, enc_data_key: str) -> bool:
    try:
        logging.warn(f"fhir bundle path--{fhir_bundle_temp_path}-- {landing_path}")
        src_bucket, src_prefix = parse_file_details(fhir_bundle_temp_path)
        dest_bucket, dest_prefix = parse_file_details(landing_path)
        file_list = list_files_from_s3(src_bucket, src_prefix)
        client = boto3.client("s3")
        for file_key in file_list:
            file = file_key.get("Key")
            if not file.endswith(".crc") and not file.endswith("_SUCCESS"):
                src_path, dest_path = _construct_bundle_file_path(
                    src_bucket=src_bucket, dest_bucket=dest_bucket, temp_file_key=file, dest_prefix=dest_prefix
                )
                # Encrypt fhir bundles and write into dest path
                logging.warn(f"Starting file encryption --{dest_path}")
                encrypt_file(
                    encrypt_data_key=enc_data_key, temp_path=src_path, encrypt_path=dest_path, metadata=metadata
                )
                logging.warn(f"Ended file encryption {dest_path}")
            client.delete_object(Bucket=src_bucket, Key=file)
            logging.warn(f"Source file deleted --{src_bucket}--{file}")
        logging.warn("Encrypted Fhir bundle upload successfully processed")
    except Exception as e:
        logging.warn(f"Upload bundle error--{str(e)}")
        return False