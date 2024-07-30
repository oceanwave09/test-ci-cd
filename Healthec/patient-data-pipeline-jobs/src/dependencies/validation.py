import logging
import os

import boto3
import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import RuntimeBatchRequest
from pyspark.sql import DataFrame

from utils.constants import GX_DATA_DOCS_DIR, GX_VAL_DIR


def upload_file_from_local(bucket: str, key: str, local_path: str, metadata: dict = {}) -> bool:
    try:
        # check if the file exists
        if not os.path.exists(local_path):
            raise ValueError(f"Failed to upload the file into S3. File '{local_path}' does not exists.")
        client = boto3.client("s3")
        client.upload_file(Filename=local_path, Bucket=bucket, Key=key, ExtraArgs={"Metadata": metadata})
        return True
    except Exception as e:
        logging.error(f"Error, While upload file -> {str(e)}")


def construct_gx_result_path(file_details: dict) -> str:
    key_prefix = file_details.get("file_format")
    if file_details.get("file_type"):
        key_prefix = f"{key_prefix}/{file_details.get('file_type')}"
    if file_details.get("file_tenant"):
        key_prefix = f"{key_prefix}/{file_details.get('file_tenant')}"
    if file_details.get("file_source"):
        key_prefix = f"{key_prefix}/{file_details.get('file_source')}"
    key_prefix = f"{key_prefix}/{file_details.get('file_batch_id')}"
    return f"{GX_VAL_DIR}/{key_prefix}.json", f"{GX_DATA_DOCS_DIR}/{key_prefix}.html"


def run_validation(df: DataFrame, resource_type: str, opsgenie_api_key: str, file_details: dict):
    # initialize a Great Expectations data context
    context = gx.get_context()
    # create a Great Expectations batch request
    batch_request = RuntimeBatchRequest(
        datasource_name="staging_datastore",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="statging_data",
        batch_identifiers={"batch_id": "default_identifier"},
        runtime_parameters={"batch_data": df},
    )
    # Create a Great Expectations checkpoint
    checkpoint_name = f"{resource_type}_checkpoint"
    expectation_suite_name = file_details.get("file_tenant")
    if file_details.get("file_source"):
        expectation_suite_name = f"{expectation_suite_name}.{file_details.get('file_source')}"
    expectation_suite_name = f"{expectation_suite_name}.{resource_type}"
    action_list = [
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        },
        {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
    ]
    if opsgenie_api_key:
        action_list.append(
            {
                "name": "send_opsgenie_alert_on_validation_result",
                "action": {
                    "class_name": "OpsgenieAlertAction",
                    "notify_on": "all",
                    "api_key": opsgenie_api_key,
                    "priority": "P3",
                    "renderer": {
                        "module_name": "great_expectations.render.renderer.opsgenie_renderer",
                        "class_name": "OpsgenieRenderer",
                    },
                    "tags": ["TestingGreatExpectations"],
                },
            }
        )
    checkpoint = Checkpoint(
        name=checkpoint_name,
        data_context=context,
        run_name_template=resource_type,
        expectation_suite_name=expectation_suite_name,
        action_list=action_list,
    )
    context.add_or_update_checkpoint(checkpoint=checkpoint)
    validation_result = checkpoint.run(batch_request=batch_request)
    validation_result_page = ""
    if validation_result.get("run_results"):
        result_keys = list(validation_result.get("run_results").keys())
        if result_keys:
            validation_result_page = (
                validation_result.get("run_results")
                .get(result_keys[0], {})
                .get("actions_results", {})
                .get("update_data_docs", {})
                .get("local_site")
            )
    if validation_result_page:
        bucket = file_details.get("bucket")
        json_key, html_key = construct_gx_result_path(file_details=file_details)
        val_html_path = validation_result_page.replace("file://", "")
        val_json_path = val_html_path.replace("data_docs/", "").replace("html", "json")
        upload_file_from_local(bucket=bucket, key=html_key, local_path=val_html_path)
        upload_file_from_local(bucket=bucket, key=json_key, local_path=val_json_path)
        html_s3_key = f"s3://{bucket}/{html_key}"
        return json_key, html_s3_key
    return None
