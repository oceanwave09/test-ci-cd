import os

import click
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from dependencies.spark import add_storage_context, get_column, start_spark
from dependencies.validation import run_validation
from utils.constants import (
    EVENT_COMPLETE,
    EVENT_FAILURE_STATUS,
    EVENT_START,
    EVENT_SUCCESS_STATUS,
    INGESTION_PIPELINE_USER,
)
from utils.enums import RecordStatus as rs
from utils.utils import exit_with_error, generate_random_string, load_config, post_validation_event


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="gap_lab_stage_load_job",
)
@click.option(
    "--config-file-path",
    "-c",
    help="application config file path",
    default=None,
)
def main(app_name, config_file_path=None):
    app_name = os.environ.get("SPARK_APP_NAME", app_name)

    spark_config = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    }

    # start spark session and logger
    spark, log = start_spark(app_name, spark_config)
    spark.sparkContext.addPyFile("/app/python-deps.zip")
    log.warn(f"spark app {app_name} started")

    # if config file is provided, extract details into a dict
    # config file should be a yaml file
    config = {}
    if config_file_path:
        config = load_config(config_file_path)
    log.info(f"config: {config}")

    try:
        # validate environment variables and config parameters
        delta_schema_location = os.environ.get(
            "DELTA_SCHEMA_LOCATION",
            config.get("delta_schema_location", ""),
        )
        if not delta_schema_location:
            exit_with_error(
                log,
                "delta schema location should be provided!",
            )

        delta_table_name = os.environ.get(
            "DELTA_TABLE_NAME",
            config.get("delta_table_name", "gap_lab"),
        )
        if not delta_table_name:
            exit_with_error(
                log,
                "delta table location should be provided!",
            )

        ingestion_bucket = os.environ.get("INGESTION_BUCKET", config.get("ingestion_bucket", ""))
        if not ingestion_bucket:
            exit_with_error(log, "ingestion bucket name should be provided!")

        # construct delta table location
        delta_table_location = os.path.join(delta_schema_location, delta_table_name)
        log.warn(f"delta_table_location: {delta_table_location}")

        canonical_file_path = os.environ.get(
            "CANONICAL_FILE_PATH",
            config.get("canonical_file_path", ""),
        )
        if not canonical_file_path:
            exit_with_error(
                log,
                "canonical file path should be provided!",
            )
        log.warn(f"canonical_file_path: {canonical_file_path}")

        file_batch_id = os.environ.get(
            "FILE_BATCH_ID",
            config.get(
                "file_batch_id",
                f"BATCH_{generate_random_string()}",
            ),
        )
        log.warn(f"file_batch_id: {file_batch_id}")

        file_source = os.environ.get("FILE_SOURCE", config.get("file_source", None))
        log.warn(f"file_source: {file_source}")

        file_name = os.environ.get("FILE_NAME", config.get("file_name", None))
        log.warn(f"file_name: {file_name}")

        file_type = os.environ.get("FILE_TYPE", config.get("file_type", ""))
        log.warn(f"file_type: {file_type}")

        file_format = os.environ.get("FILE_FORMAT", config.get("file_format", ""))
        log.warn(f"file_format: {file_format}")

        resource_type = os.environ.get("RESOURCE_TYPE", config.get("resource_type", ""))
        log.warn(f"resource_type: {resource_type}")

        file_tenant = os.environ.get("FILE_TENANT", config.get("file_tenant", ""))
        log.warn(f"file_tenant: {file_tenant}")

        opsgenie_api_key = os.environ.get("OPSGENIE_API_KEY", config.get("opsgenie_api_key", ""))

        # add storage context in spark session
        spark = add_storage_context(
            spark,
            [canonical_file_path, delta_table_location],
        )

        event_info = {
            "file_name": file_name,
            "file_batch_id": file_batch_id,
            "file_source": file_source,
            "created_user": INGESTION_PIPELINE_USER,
            "resource_id": file_name,
            "origin": app_name,
            "resource_type": resource_type,
            "file_tenant": file_tenant,
            "links": {},
        }

        file_details = {
            "file_tenant": file_tenant,
            "file_source": file_source,
            "file_format": file_format,
            "file_type": file_type,
            "file_batch_id": file_batch_id,
            "bucket": ingestion_bucket,
        }

        # load the record keys file
        log.warn("load canonical file into dataframe")
        canonical_df = spark.read.json(canonical_file_path, multiLine=True)

        # add additional columns and prepare dataframe which match with gap delta lake table structure
        lab_df = (
            canonical_df.withColumn("row_id", get_column(canonical_df, "row_id"))
            .withColumn("row", get_column(canonical_df, "row"))
            .withColumn("domain", get_column(canonical_df, "domain"))
            .withColumn("patient_first_name", get_column(canonical_df, "patient_first_name"))
            .withColumn("patient_last_name", get_column(canonical_df, "patient_last_name"))
            .withColumn("patient_dob", get_column(canonical_df, "patient_dob"))
            .withColumn("external_patient_id", get_column(canonical_df, "external_patient_id"))
            .withColumn("external_practice_id", get_column(canonical_df, "external_practice_id"))
            .withColumn("code", get_column(canonical_df, "code"))
            .withColumn("name", get_column(canonical_df, "name"))
            .withColumn("dictionary_name", get_column(canonical_df, "dictionary_name"))
            .withColumn("date_performed", get_column(canonical_df, "date_performed"))
            .withColumn("date_ordered", get_column(canonical_df, "date_ordered"))
            .withColumn("date_due", get_column(canonical_df, "date_due"))
            .withColumn("value", get_column(canonical_df, "value"))
            .withColumn("medi_unnecessary_reason_code", get_column(canonical_df, "medi_unnecessary_reason_code"))
            .withColumn("medi_unnecessary_reason_name", get_column(canonical_df, "medi_unnecessary_reason_name"))
            .withColumn("medi_unnecessary_reason_dict", get_column(canonical_df, "medi_unnecessary_reason_dict"))
            .withColumn("record_verified", get_column(canonical_df, "record_verified"))
            .withColumn("provider_npi", get_column(canonical_df, "provider_npi"))
            .withColumn("practice_tin", get_column(canonical_df, "practice_tin"))
            .withColumn("provider_name", get_column(canonical_df, "provider_name"))
            .withColumn("note", get_column(canonical_df, "note"))
            .withColumn("note_type", get_column(canonical_df, "note_type"))
            .withColumn("specimen_source", get_column(canonical_df, "specimen_source"))
            .withColumn("specimen_condition", get_column(canonical_df, "specimen_condition"))
            .withColumn("facility_name", get_column(canonical_df, "facility_name"))
            .withColumn("facility_address", get_column(canonical_df, "facility_address"))
            .withColumn("received_value", get_column(canonical_df, "received_value"))
            .withColumn("received_units", get_column(canonical_df, "received_units"))
            .withColumn(
                "low_referance_range",
                f.when(
                    get_column(canonical_df, "reference_range").isNotNull(),
                    f.when(
                        f.size(f.split(get_column(canonical_df, "reference_range"), "-")) > 0,
                        f.split(get_column(canonical_df, "reference_range"), "-")[0],
                    ),
                ),
            )
            .withColumn(
                "high_referance_range",
                f.when(
                    get_column(canonical_df, "reference_range").isNotNull(),
                    f.when(
                        f.size(f.split(get_column(canonical_df, "reference_range"), "-")) > 1,
                        f.split(get_column(canonical_df, "reference_range"), "-")[1],
                    ),
                ),
            )
            .withColumn("abnormal_flag_code", get_column(canonical_df, "abnormal_flag_code"))
            .withColumn("remove_specified_lab_flag", get_column(canonical_df, "remove_specified_lab_flag"))
            .withColumn("vendor", get_column(canonical_df, "vendor"))
            .withColumn("source_type", get_column(canonical_df, "source_type"))
            .withColumn("file_batch_id", f.lit(file_batch_id).cast(StringType()))
            .withColumn("file_name", f.lit(file_name).cast(StringType()))
            .withColumn("file_source_name", f.lit(file_source).cast(StringType()))
            .withColumn("file_status", f.lit(rs.STAGE_LOAD.value))
            .withColumn("created_user", f.lit(INGESTION_PIPELINE_USER).cast(StringType()))
            .withColumn("created_ts", f.current_timestamp())
            .withColumn("updated_user", f.lit(INGESTION_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
            .drop("reference_range")
        )

        # write data into delta lake `gap_lab` table
        lab_df.write.format("delta").mode("append").save(delta_table_location)

        # publish validation start event message
        post_validation_event(event_info=event_info, event=EVENT_START, event_status=EVENT_SUCCESS_STATUS)
        try:
            # validate canonical data using Great Expectations
            validation_result_page = run_validation(canonical_df, "gap_lab", opsgenie_api_key, file_details)
            if validation_result_page:
                log.warn(
                    "The staging data validation completed. \
                    You can navigate to the following link to check detailed report"
                )
                log.warn(f"validation report: {validation_result_page}")
                event_info["links"] = {"validation_results": validation_result_page[1]}
                post_validation_event(event_info=event_info, event=EVENT_COMPLETE, event_status=EVENT_SUCCESS_STATUS)
        except Exception:
            post_validation_event(event_info=event_info, event=EVENT_COMPLETE, event_status=EVENT_FAILURE_STATUS)

        log.warn(
            f"spark job {app_name} completed successfully, "
            f"{canonical_file_path} loaded into stage table path {delta_table_location}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
