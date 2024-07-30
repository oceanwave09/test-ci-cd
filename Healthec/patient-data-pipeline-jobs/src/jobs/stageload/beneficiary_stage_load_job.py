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
from utils.transformation import parse_date, parse_date_time, update_gender
from utils.utils import exit_with_error, generate_random_string, load_config, post_validation_event


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="beneficiary_stage_load_job",
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
        delta_schema_location = os.environ.get("DELTA_SCHEMA_LOCATION", config.get("delta_schema_location", ""))
        if not delta_schema_location:
            exit_with_error(log, "delta schema location should be provided!")

        delta_table_name = os.environ.get("DELTA_TABLE_NAME", config.get("delta_table_name", "beneficiary"))
        if not delta_table_name:
            exit_with_error(log, "delta table location should be provided!")

        # construct delta table location
        delta_table_location = os.path.join(delta_schema_location, delta_table_name)
        log.warn(f"delta_table_location: {delta_table_location}")

        canonical_file_path = os.environ.get("CANONICAL_FILE_PATH", config.get("canonical_file_path", ""))
        if not canonical_file_path:
            exit_with_error(log, "canonical file path should be provided!")
        log.warn(f"canonical_file_path: {canonical_file_path}")

        file_batch_id = os.environ.get("FILE_BATCH_ID", config.get("file_batch_id", f"BATCH_{generate_random_string()}"))
        log.warn(f"file_batch_id: {file_batch_id}")

        file_source = os.environ.get("FILE_SOURCE", config.get("file_source", None))
        log.warn(f"file_source: {file_source}")

        file_name = os.environ.get("FILE_NAME", config.get("file_name", None))
        log.warn(f"file_name: {file_name}")

        resource_type = os.environ.get("RESOURCE_TYPE", config.get("resource_type", ""))
        log.warn(f"resource_type: {resource_type}")

        file_tenant = os.environ.get("FILE_TENANT", config.get("file_tenant", ""))
        log.warn(f"file_tenant: {file_tenant}")

        opsgenie_api_key = os.environ.get("OPSGENIE_API_KEY", config.get("opsgenie_api_key", ""))
        if not opsgenie_api_key:
            exit_with_error(log, "opsgenie api key should be provided!")

        # add storage context in spark session
        spark = add_storage_context(spark, [canonical_file_path, delta_table_location])

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

        # Register function as UDF with Spark
        update_gender_udf = f.udf(update_gender, StringType())
        parse_date_udf = f.udf(parse_date, StringType())
        parse_date_time_udf = f.udf(parse_date_time, StringType())

        # load the record keys file
        log.warn("load canonical file into dataframe")
        canonical_df = spark.read.json(canonical_file_path, multiLine=True)
        # add additional columns and prepare dataframe which match with delta lake table structure
        beneficiary_df = (
            canonical_df.withColumn("row_id", get_column(canonical_df, "row_id"))
            .withColumn("member_id", get_column(canonical_df, "member_id"))
            .withColumn("member_mbi", get_column(canonical_df, "member_mbi"))
            .withColumn("member_medicaid_id", get_column(canonical_df, "member_medicaid_id"))
            .withColumn("subscriber_id", get_column(canonical_df, "subscriber_id"))
            .withColumn("member_id_suffix", get_column(canonical_df, "member_id_suffix"))
            .withColumn("alternate_member_id", get_column(canonical_df, "alternate_member_id"))
            .withColumn("member_ssn", get_column(canonical_df, "member_ssn"))
            .withColumn("member_internal_id", get_column(canonical_df, "member_internal_id"))
            .withColumn("member_last_name", get_column(canonical_df, "member_last_name"))
            .withColumn("member_first_name", get_column(canonical_df, "member_first_name"))
            .withColumn("member_middle_initial", get_column(canonical_df, "member_middle_initial"))
            .withColumn("member_name_prefix", get_column(canonical_df, "member_name_prefix"))
            .withColumn("member_name_suffix", get_column(canonical_df, "member_name_suffix"))
            .withColumn("member_dob", f.to_date(parse_date_udf(get_column(canonical_df, "member_dob"))))
            .withColumn("member_gender", update_gender_udf(get_column(canonical_df, "member_gender")))
            .withColumn("member_relationship_code", get_column(canonical_df, "member_relationship_code"))
            .withColumn("member_address_line_1", get_column(canonical_df, "member_address_line_1"))
            .withColumn("member_address_line_2", get_column(canonical_df, "member_address_line_2"))
            .withColumn("member_city", get_column(canonical_df, "member_city"))
            .withColumn("member_county", get_column(canonical_df, "member_county"))
            .withColumn("member_state", get_column(canonical_df, "member_state"))
            .withColumn("member_zip", get_column(canonical_df, "member_zip"))
            .withColumn("member_country", get_column(canonical_df, "member_country"))
            .withColumn("member_phone_home", get_column(canonical_df, "member_phone_home"))
            .withColumn("member_phone_work", get_column(canonical_df, "member_phone_work"))
            .withColumn("member_phone_mobile", get_column(canonical_df, "member_phone_mobile"))
            .withColumn("member_email", get_column(canonical_df, "member_email"))
            .withColumn("member_deceased_flag", get_column(canonical_df, "member_deceased_flag"))
            .withColumn(
                "member_deceased_date",
                f.to_timestamp(parse_date_time_udf(get_column(canonical_df, "member_deceased_date"))),
            )
            .withColumn("member_primary_language", get_column(canonical_df, "member_primary_language"))
            .withColumn("member_race_code", get_column(canonical_df, "member_race_code"))
            .withColumn("member_race_desc", get_column(canonical_df, "member_race_desc"))
            .withColumn("member_ethnicity_code", get_column(canonical_df, "member_ethnicity_code"))
            .withColumn("member_ethnicity_desc", get_column(canonical_df, "member_ethnicity_desc"))
            .withColumn("mental_health_flag", get_column(canonical_df, "mental_health_flag"))
            .withColumn("pharmacy_flag", get_column(canonical_df, "pharmacy_flag"))
            .withColumn("dual_status_code", get_column(canonical_df, "dual_status_code"))
            .withColumn("medicare_status_code", get_column(canonical_df, "medicare_status_code"))
            .withColumn("esrd_dual_eligibility_status", get_column(canonical_df, "esrd_dual_eligibility_status"))
            .withColumn("esrd_dual_status_code", get_column(canonical_df, "esrd_dual_status_code"))
            .withColumn("member_risk_score", get_column(canonical_df, "member_risk_score"))
            .withColumn("hospice_indicator", get_column(canonical_df, "hospice_indicator"))
            .withColumn(
                "hospice_start_date", f.to_timestamp(parse_date_time_udf(get_column(canonical_df, "hospice_start_date")))
            )
            .withColumn(
                "hospice_end_date", f.to_timestamp(parse_date_time_udf(get_column(canonical_df, "hospice_end_date")))
            )
            .withColumn("insurance_company_name", get_column(canonical_df, "insurance_company_name"))
            .withColumn("insurance_company_id", get_column(canonical_df, "insurance_company_id"))
            .withColumn("insurance_group_name", get_column(canonical_df, "insurance_group_name"))
            .withColumn("insurance_group_id", get_column(canonical_df, "insurance_group_id"))
            .withColumn("insurance_group_plan_name", get_column(canonical_df, "insurance_group_plan_name"))
            .withColumn("insurance_group_plan_id", get_column(canonical_df, "insurance_group_plan_id"))
            .withColumn("line_of_business", get_column(canonical_df, "line_of_business"))
            .withColumn(
                "insurance_effective_date",
                f.to_timestamp(parse_date_time_udf(get_column(canonical_df, "insurance_effective_date"))),
            )
            .withColumn(
                "insurance_end_date", f.to_timestamp(parse_date_time_udf(get_column(canonical_df, "insurance_end_date")))
            )
            .withColumn("insurance_pay_rank", get_column(canonical_df, "insurance_pay_rank"))
            .withColumn("provider_org_tin", get_column(canonical_df, "provider_org_tin"))
            .withColumn("provider_org_name", get_column(canonical_df, "provider_org_name"))
            .withColumn("physician_npi", get_column(canonical_df, "physician_npi"))
            .withColumn("physician_internal_id", get_column(canonical_df, "physician_internal_id"))
            .withColumn("physician_first_name", get_column(canonical_df, "physician_first_name"))
            .withColumn("physician_last_name", get_column(canonical_df, "physician_last_name"))
            .withColumn("physician_taxonomy_code", get_column(canonical_df, "physician_taxonomy_code"))
            .withColumn("physician_specialty_code", get_column(canonical_df, "physician_specialty_code"))
            .withColumn("physician_specialty_desc", get_column(canonical_df, "physician_specialty_desc"))
            .withColumn(
                "physician_effective_date",
                f.to_timestamp(parse_date_time_udf(get_column(canonical_df, "physician_effective_date"))),
            )
            .withColumn(
                "physician_termination_date",
                f.to_timestamp(parse_date_time_udf(get_column(canonical_df, "physician_termination_date"))),
            )
            .withColumn("file_batch_id", f.lit(file_batch_id).cast(StringType()))
            .withColumn("file_name", f.lit(file_name).cast(StringType()))
            .withColumn("file_source_name", f.lit(file_source).cast(StringType()))
            .withColumn("file_status", f.lit(rs.STAGE_LOAD.value))
            .withColumn("created_user", f.lit(INGESTION_PIPELINE_USER).cast(StringType()))
            .withColumn("created_ts", f.current_timestamp())
            .withColumn("updated_user", f.lit(INGESTION_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )

        # write data into delta lake practice table
        beneficiary_df.write.format("delta").mode("append").save(delta_table_location)

        # publish validation start event message
        post_validation_event(event_info=event_info, event=EVENT_START, event_status=EVENT_SUCCESS_STATUS)
        try:
            # validate canonical data using Great Expectations
            validation_result_page = run_validation(canonical_df, "beneficiary", opsgenie_api_key)
            if validation_result_page:
                log.warn(
                    "The staging data validation completed. \
                    You can navigate to the following link to check detailed report"
                )
                log.warn(f"validation report: {validation_result_page}")

                event_info["links"] = {"validation_result_html": validation_result_page}
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
