import great_expectations as gx
from great_expectations.checkpoint import Checkpoint
from great_expectations.core.batch import RuntimeBatchRequest
from pyspark.sql import DataFrame


def run_validation(df: DataFrame, resource_type: str, opsgenie_api_key: str):
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
    checkpoint = Checkpoint(
        name=checkpoint_name,
        data_context=context,
        run_name_template=resource_type,
        expectation_suite_name=resource_type,
        action_list=[
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
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
            },
        ],
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
    return validation_result_page
