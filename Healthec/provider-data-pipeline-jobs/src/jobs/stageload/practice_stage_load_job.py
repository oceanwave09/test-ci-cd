import os

import click
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from dependencies.spark import add_storage_context, get_column, start_spark
from dependencies.validation import run_validation
from utils.constants import INGESTION_PIPELINE_USER
from utils.enums import RecordStatus as rs
from utils.utils import exit_with_error, generate_random_string, load_config


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="practice_stage_load_job",
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
            config.get("delta_table_name", "practice"),
        )
        if not delta_table_name:
            exit_with_error(
                log,
                "delta table location should be provided!",
            )

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

        opsgenie_api_key = os.environ.get("OPSGENIE_API_KEY", config.get("opsgenie_api_key", ""))
        if not opsgenie_api_key:
            exit_with_error(log, "opsgenie api key should be provided!")

        # add storage context in spark session
        spark = add_storage_context(
            spark,
            [canonical_file_path, delta_table_location],
        )

        # load the record keys file
        log.warn("load practice canonical file into dataframe")
        canonical_df = spark.read.json(canonical_file_path, multiLine=True)

        # add additional columns and prepare dataframe which match with practice delta lake table structure
        practice_df = (
            canonical_df.withColumn("row_id", get_column(canonical_df, "row_id"))
            .withColumn("provider_org_name", get_column(canonical_df, "provider_org_name"))
            .withColumn("provider_org_dba_Name", get_column(canonical_df, "provider_org_dba_Name"))
            .withColumn("provider_org_type", get_column(canonical_df, "provider_org_type"))
            .withColumn("provider_org_location_rank", get_column(canonical_df, "provider_org_location_rank"))
            .withColumn("provider_org_taxonomy_code", get_column(canonical_df, "provider_org_taxonomy_code"))
            .withColumn("provider_org_ssn", get_column(canonical_df, "provider_org_ssn"))
            .withColumn("provider_org_tin", get_column(canonical_df, "provider_org_tin"))
            .withColumn("provider_org_group_npi", get_column(canonical_df, "provider_org_group_npi"))
            .withColumn("provider_org_internal_id", get_column(canonical_df, "provider_org_internal_id"))
            .withColumn("provider_org_nabp_id", get_column(canonical_df, "provider_org_nabp_id"))
            .withColumn("provider_org_address_line_1", get_column(canonical_df, "provider_org_address_line_1"))
            .withColumn("provider_org_address_line_2", get_column(canonical_df, "provider_org_address_line_2"))
            .withColumn("provider_org_city", get_column(canonical_df, "provider_org_city"))
            .withColumn("provider_org_county", get_column(canonical_df, "provider_org_county"))
            .withColumn("provider_org_state", get_column(canonical_df, "provider_org_state"))
            .withColumn("provider_org_zip", get_column(canonical_df, "provider_org_zip"))
            .withColumn("provider_org_country", get_column(canonical_df, "provider_org_country"))
            .withColumn("provider_org_phone_work", get_column(canonical_df, "provider_org_phone_work"))
            .withColumn("provider_org_email", get_column(canonical_df, "provider_org_email"))
            .withColumn("provider_org_fax", get_column(canonical_df, "provider_org_fax"))
            .withColumn("provider_org_lab_id", get_column(canonical_df, "provider_org_lab_id"))
            .withColumn("provider_org_lab_name", get_column(canonical_df, "provider_org_lab_name"))
            .withColumn("provider_org_contact_title", get_column(canonical_df, "provider_org_contact_title"))
            .withColumn("provider_org_contact_first_name", get_column(canonical_df, "provider_org_contact_first_name"))
            .withColumn("provider_org_contact_last_name", get_column(canonical_df, "provider_org_contact_last_name"))
            .withColumn("provider_org_contact_middle_name", get_column(canonical_df, "provider_org_contact_middle_name"))
            .withColumn("provider_org_contact_phone", get_column(canonical_df, "provider_org_contact_phone"))
            .withColumn("provider_org_contact_email", get_column(canonical_df, "provider_org_contact_email"))
            .withColumn("insurance_company_name", get_column(canonical_df, "insurance_company_name"))
            .withColumn("insurance_company_id", get_column(canonical_df, "insurance_company_id"))
            .withColumn("provider_org_effective_date", get_column(canonical_df, "provider_org_effective_date"))
            .withColumn("provider_org_termination_date", get_column(canonical_df, "provider_org_termination_date"))
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
        practice_df.write.format("delta").mode("append").save(delta_table_location)

        # validate canonical data using Great Expectations
        validation_result_page = run_validation(canonical_df, "practice_roaster", opsgenie_api_key)
        if validation_result_page:
            log.warn(
                "The staging data validation completed. You can navigate to the following link to check detailed report"
            )
            log.warn(f"validation report: {validation_result_page}")

        log.warn(
            f"spark job {app_name} completed successfully, "
            f"{canonical_file_path} loaded into stage table path {delta_table_location}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
