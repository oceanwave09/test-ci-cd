import os

import click
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from dependencies.spark import add_storage_context, get_column, start_spark
from utils.constants import INGESTION_PIPELINE_USER
from utils.enums import RecordStatus as rs
from utils.utils import exit_with_error, generate_random_string, load_config


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="rx_claim_stage_load_job",
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

        delta_table_name = os.environ.get("DELTA_TABLE_NAME", config.get("delta_table_name", "rx_claim"))
        if not delta_table_name:
            exit_with_error(log, "delta table location should be provided!")

        # construct delta table location
        delta_table_location = os.path.join(delta_schema_location, delta_table_name)
        log.warn(f"delta_table_location: {delta_table_location}")

        canonical_file_path = os.environ.get("CANONICAL_FILE_PATH", config.get("canonical_file_path", ""))
        if not canonical_file_path:
            exit_with_error(log, "canonical file path should be provided!")
        log.warn(f"canonical_file_path: {canonical_file_path}")

        file_batch_id = os.environ.get(
            "FILE_BATCH_ID",
            config.get("file_batch_id", f"BATCH_{generate_random_string()}"),
        )
        log.warn(f"file_batch_id: {file_batch_id}")

        file_source = os.environ.get("FILE_SOURCE", config.get("file_source", None))
        log.warn(f"file_source: {file_source}")

        file_name = os.environ.get("FILE_NAME", config.get("file_name", None))
        log.warn(f"file_name: {file_name}")

        # add storage context in spark session
        spark = add_storage_context(spark, [canonical_file_path, delta_table_location])

        # load the record keys file
        log.warn("load canonical file into dataframe")
        canonical_df = spark.read.json(canonical_file_path, multiLine=True)

        # add additional columns and prepare dataframe which match with delta lake table structure
        patient_df = (
            canonical_df.withColumn("row_id", get_column(canonical_df, "row_id"))
            .withColumn("claim_type", get_column(canonical_df, "claim_type"))
            .withColumn("claim_id", get_column(canonical_df, "claim_id"))
            .withColumn("previous_claim_id", get_column(canonical_df, "previous_claim_id"))
            .withColumn("claim_disposition", get_column(canonical_df, "claim_disposition"))
            .withColumn("member_id", get_column(canonical_df, "member_id"))
            .withColumn("member_mbi", get_column(canonical_df, "member_mbi"))
            .withColumn("member_medicaid_id", get_column(canonical_df, "member_medicaid_id"))
            .withColumn("alternate_member_id", get_column(canonical_df, "alternate_member_id"))
            .withColumn("subscriber_id", get_column(canonical_df, "subscriber_id"))
            .withColumn("member_id_suffix", get_column(canonical_df, "member_id_suffix"))
            .withColumn("member_ssn", get_column(canonical_df, "member_ssn"))
            .withColumn("member_mrn", get_column(canonical_df, "member_mrn"))
            .withColumn("member_internal_id", get_column(canonical_df, "member_internal_id"))
            .withColumn("member_last_name", get_column(canonical_df, "member_last_name"))
            .withColumn("member_first_name", get_column(canonical_df, "member_first_name"))
            .withColumn("member_middle_initial", get_column(canonical_df, "member_middle_initial"))
            .withColumn("member_dob", get_column(canonical_df, "member_dob"))
            .withColumn("member_gender", get_column(canonical_df, "member_gender"))
            .withColumn("member_relationship_code", get_column(canonical_df, "member_relationship_code"))
            .withColumn("member_address_line_1", get_column(canonical_df, "member_address_line_1"))
            .withColumn("member_address_line_2", get_column(canonical_df, "member_address_line_2"))
            .withColumn("member_city", get_column(canonical_df, "member_city"))
            .withColumn("member_state", get_column(canonical_df, "member_state"))
            .withColumn("member_zip", get_column(canonical_df, "member_zip"))
            .withColumn("insurance_company_name", get_column(canonical_df, "insurance_company_name"))
            .withColumn("insurance_company_id", get_column(canonical_df, "insurance_company_id"))
            .withColumn("insurance_group_name", get_column(canonical_df, "insurance_group_name"))
            .withColumn("insurance_group_id", get_column(canonical_df, "insurance_group_id"))
            .withColumn("insurance_group_plan_name", get_column(canonical_df, "insurance_group_plan_name"))
            .withColumn("insurance_group_plan_id", get_column(canonical_df, "insurance_group_plan_id"))
            .withColumn("primary_payer_code", get_column(canonical_df, "primary_payer_code"))
            .withColumn("claim_billed_date", get_column(canonical_df, "claim_billed_date"))
            .withColumn("claim_start_date", get_column(canonical_df, "claim_start_date"))
            .withColumn("claim_end_date", get_column(canonical_df, "claim_end_date"))
            .withColumn("claim_total_charges", get_column(canonical_df, "claim_total_charges"))
            .withColumn("claim_adjudication_status", get_column(canonical_df, "claim_adjudication_status"))
            .withColumn("claim_total_paid", get_column(canonical_df, "claim_total_paid"))
            .withColumn("claim_paid_date", get_column(canonical_df, "claim_paid_date"))
            .withColumn("network_paid_ind", get_column(canonical_df, "network_paid_ind"))
            .withColumn("claim_copay_amount", get_column(canonical_df, "claim_copay_amount"))
            .withColumn("claim_patient_paid_amount", get_column(canonical_df, "claim_patient_paid_amount"))
            .withColumn("claim_other_payer_paid", get_column(canonical_df, "claim_other_payer_paid"))
            .withColumn("line_number", get_column(canonical_df, "line_number"))
            .withColumn("line_item_control_number", get_column(canonical_df, "line_item_control_number"))
            .withColumn("service_line_disposition", get_column(canonical_df, "service_line_disposition"))
            .withColumn("service_start_date", get_column(canonical_df, "service_start_date"))
            .withColumn("service_end_date", get_column(canonical_df, "service_end_date"))
            .withColumn("service_line_charges", get_column(canonical_df, "service_line_charges"))
            .withColumn("line_adjudication_status", get_column(canonical_df, "line_adjudication_status"))
            .withColumn("line_amount_paid", get_column(canonical_df, "line_amount_paid"))
            .withColumn(
                "service_line_paid_date",
                get_column(canonical_df, "service_line_paid_date"),
            )
            .withColumn("line_payment_level", get_column(canonical_df, "line_payment_level"))
            .withColumn("line_copay_amount", get_column(canonical_df, "line_copay_amount"))
            .withColumn("line_patient_paid_amount", get_column(canonical_df, "line_patient_paid_amount"))
            .withColumn("mco_paid_amount", get_column(canonical_df, "mco_paid_amount"))
            .withColumn("prescription_number", get_column(canonical_df, "prescription_number"))
            .withColumn("prescription_date", get_column(canonical_df, "prescription_date"))
            .withColumn("dispense_date", get_column(canonical_df, "dispense_date"))
            .withColumn("daw_code", get_column(canonical_df, "daw_code"))
            .withColumn("drug_ndc_code", get_column(canonical_df, "drug_ndc_code"))
            .withColumn("drug_ndc_desc", get_column(canonical_df, "drug_ndc_desc"))
            .withColumn("generic_branded_ind", get_column(canonical_df, "generic_branded_ind"))
            .withColumn("gpi", get_column(canonical_df, "gpi"))
            .withColumn("drug_strength", get_column(canonical_df, "drug_strength"))
            .withColumn("drug_quantity", get_column(canonical_df, "drug_quantity"))
            .withColumn("days_supply", get_column(canonical_df, "days_supply"))
            .withColumn("mail_order_flag", get_column(canonical_df, "mail_order_flag"))
            .withColumn("provider_org_napb_id", get_column(canonical_df, "provider_org_napb_id"))
            .withColumn("line_rendering_participation", get_column(canonical_df, "line_rendering_participation"))
            .withColumn("render_prescriber_npi", get_column(canonical_df, "render_prescriber_npi"))
            .withColumn("render_prescriber_internal_id", get_column(canonical_df, "render_prescriber_internal_id"))
            .withColumn("render_prescriber_first_name", get_column(canonical_df, "render_prescriber_first_name"))
            .withColumn("render_prescriber_last_name", get_column(canonical_df, "render_prescriber_last_name"))
            .withColumn("render_prescriber_street_line_1", get_column(canonical_df, "render_prescriber_street_line_1"))
            .withColumn("render_prescriber_street_line_2", get_column(canonical_df, "render_prescriber_street_line_2"))
            .withColumn("render_prescriber_city", get_column(canonical_df, "render_prescriber_city"))
            .withColumn("render_prescriber_state", get_column(canonical_df, "render_prescriber_state"))
            .withColumn("render_prescriber_zip", get_column(canonical_df, "render_prescriber_zip"))
            .withColumn("claim_paid_status", get_column(canonical_df, "claim_paid_status"))
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
        patient_df.write.format("delta").mode("append").save(delta_table_location)

        log.warn(
            f"spark job {app_name} completed successfully, "
            f"{canonical_file_path} loaded into stage table path {delta_table_location}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
