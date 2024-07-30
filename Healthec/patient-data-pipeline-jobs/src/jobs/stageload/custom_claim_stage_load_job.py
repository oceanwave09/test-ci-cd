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
    default="custom_claim_stage_load_job",
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

        delta_table_name = os.environ.get("DELTA_TABLE_NAME", config.get("delta_table_name", "custom_claim"))
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
        custom_claim_df = (
            canonical_df.withColumn("row_id", get_column(canonical_df, "row_id"))
            .withColumn("member_id", get_column(canonical_df, "member_id"))
            .withColumn("member_mbi", get_column(canonical_df, "member_mbi"))
            .withColumn("member_medicaid_id", get_column(canonical_df, "member_medicaid_id"))
            .withColumn("member_mrn", get_column(canonical_df, "member_mrn"))
            .withColumn("member_ssn", get_column(canonical_df, "member_ssn"))
            .withColumn("member_relationship_code", get_column(canonical_df, "member_relationship_code"))
            .withColumn("member_last_name", get_column(canonical_df, "member_last_name"))
            .withColumn("member_first_name", get_column(canonical_df, "member_first_name"))
            .withColumn("member_middle_name", get_column(canonical_df, "member_middle_name"))
            .withColumn("member_gender", get_column(canonical_df, "member_gender"))
            .withColumn("member_dob", get_column(canonical_df, "member_dob"))
            .withColumn("member_deceased_date", get_column(canonical_df, "member_deceased_date"))
            .withColumn("member_address_line_1", get_column(canonical_df, "member_address_line_1"))
            .withColumn("member_address_line_2", get_column(canonical_df, "member_address_line_2"))
            .withColumn("member_city", get_column(canonical_df, "member_city"))
            .withColumn("member_state", get_column(canonical_df, "member_state"))
            .withColumn("member_zip", get_column(canonical_df, "member_zip"))
            .withColumn("member_phone_home", get_column(canonical_df, "member_phone_home"))
            .withColumn("member_email", get_column(canonical_df, "member_email"))
            .withColumn("attributed_last_name", get_column(canonical_df, "attributed_last_name"))
            .withColumn("attributed_first_name", get_column(canonical_df, "attributed_first_name"))
            .withColumn("attributed_npi", get_column(canonical_df, "attributed_npi"))
            .withColumn("provider_org_name", get_column(canonical_df, "provider_org_name"))
            .withColumn("facility_name", get_column(canonical_df, "facility_name"))
            .withColumn("pharmacy_name", get_column(canonical_df, "pharmacy_name"))
            .withColumn("provider_org_taxid", get_column(canonical_df, "provider_org_taxid"))
            .withColumn("facility_taxid", get_column(canonical_df, "facility_taxid"))
            .withColumn("pharmacy_taxid", get_column(canonical_df, "pharmacy_taxid"))
            .withColumn("provider_org_group_npi", get_column(canonical_df, "provider_org_group_npi"))
            .withColumn("facility_group_npi", get_column(canonical_df, "facility_group_npi"))
            .withColumn("pharmacy_group_npi", get_column(canonical_df, "pharmacy_group_npi"))
            .withColumn("provider_org_internal_id", get_column(canonical_df, "provider_org_internal_id"))
            .withColumn("facility_internal_id", get_column(canonical_df, "facility_internal_id"))
            .withColumn("pharmacy_internal_id", get_column(canonical_df, "pharmacy_internal_id"))
            .withColumn("provider_org_taxonomy_code", get_column(canonical_df, "provider_org_taxonomy_code"))
            .withColumn("facility_taxonomy_code", get_column(canonical_df, "facility_taxonomy_code"))
            .withColumn("pharmacy_taxonomy_code", get_column(canonical_df, "pharmacy_taxonomy_code"))
            .withColumn("claim_type", get_column(canonical_df, "claim_type"))
            .withColumn("claim_id", get_column(canonical_df, "claim_id"))
            .withColumn("previous_claim_id", get_column(canonical_df, "previous_claim_id"))
            .withColumn("insurance_company_name", get_column(canonical_df, "insurance_company_name"))
            .withColumn("insurance_company_id", get_column(canonical_df, "insurance_company_id"))
            .withColumn("insurance_group_id", get_column(canonical_df, "insurance_group_id"))
            .withColumn("insurance_group_plan_id", get_column(canonical_df, "insurance_group_plan_id"))
            .withColumn("payer_lob", get_column(canonical_df, "payer_lob"))
            .withColumn("claim_tob", get_column(canonical_df, "claim_tob"))
            .withColumn("admission_source", get_column(canonical_df, "admission_source"))
            .withColumn("admission_type", get_column(canonical_df, "admission_type"))
            .withColumn("admission_date", get_column(canonical_df, "admission_date"))
            .withColumn("discharge_date", get_column(canonical_df, "discharge_date"))
            .withColumn("discharge_status", get_column(canonical_df, "discharge_status"))
            .withColumn("claim_billed_date", get_column(canonical_df, "claim_billed_date"))
            .withColumn("claim_start_date", get_column(canonical_df, "claim_start_date"))
            .withColumn("claim_end_date", get_column(canonical_df, "claim_end_date"))
            .withColumn("claim_disposition", get_column(canonical_df, "claim_disposition"))
            .withColumn("claim_copay_amount", get_column(canonical_df, "claim_copay_amount"))
            .withColumn("claim_coinsurance_amount", get_column(canonical_df, "claim_coinsurance_amount"))
            .withColumn("claim_deduct_amount", get_column(canonical_df, "claim_deduct_amount"))
            .withColumn("claim_total_charges", get_column(canonical_df, "claim_total_charges"))
            .withColumn("claim_total_units", get_column(canonical_df, "claim_total_units"))
            .withColumn("claim_paid_date", get_column(canonical_df, "claim_paid_date"))
            .withColumn("claim_paid_total_units", get_column(canonical_df, "claim_paid_total_units"))
            .withColumn("claim_network_paid_ind", get_column(canonical_df, "claim_network_paid_ind"))
            .withColumn("claim_allowed_amount", get_column(canonical_df, "claim_allowed_amount"))
            .withColumn("claim_payment_indicator", get_column(canonical_df, "claim_payment_indicator"))
            .withColumn("claim_adjudication_status", get_column(canonical_df, "claim_adjudication_status"))
            .withColumn("attending_last_name", get_column(canonical_df, "attending_last_name"))
            .withColumn("attending_first_name", get_column(canonical_df, "attending_first_name"))
            .withColumn("attending_middle_name", get_column(canonical_df, "attending_middle_name"))
            .withColumn("attending_npi", get_column(canonical_df, "attending_npi"))
            .withColumn("attending_taxonomy_code", get_column(canonical_df, "attending_taxonomy_code"))
            .withColumn("rendering_last_name", get_column(canonical_df, "rendering_last_name"))
            .withColumn("rendering_first_name", get_column(canonical_df, "rendering_first_name"))
            .withColumn("rendering_middle_name", get_column(canonical_df, "rendering_middle_name"))
            .withColumn("rendering_npi", get_column(canonical_df, "rendering_npi"))
            .withColumn("rendering_taxonomy_code", get_column(canonical_df, "rendering_taxonomy_code"))
            .withColumn("svc_facility_name", get_column(canonical_df, "svc_facility_name"))
            .withColumn("svc_facility_group_npi", get_column(canonical_df, "svc_facility_group_npi"))
            .withColumn("svc_facility_taxonomy_code", get_column(canonical_df, "svc_facility_taxonomy_code"))
            .withColumn("icd_Version", get_column(canonical_df, "icd_Version"))
            .withColumn("principal_icd_proc_code", get_column(canonical_df, "principal_icd_proc_code"))
            .withColumn("principal_icd_proc_description", get_column(canonical_df, "principal_icd_proc_description"))
            .withColumn("principal_icd_proc_date", get_column(canonical_df, "principal_icd_proc_date"))
            .withColumn("icd_proc_code_1", get_column(canonical_df, "icd_proc_code_1"))
            .withColumn("icd_proc_code_1_descrip", get_column(canonical_df, "icd_proc_code_1_descrip"))
            .withColumn("icd_proc_code_1_date", get_column(canonical_df, "icd_proc_code_1_date"))
            .withColumn("icd_proc_code_2", get_column(canonical_df, "icd_proc_code_2"))
            .withColumn("icd_proc_code_2_descrip", get_column(canonical_df, "icd_proc_code_2_descrip"))
            .withColumn("icd_proc_code_2_date", get_column(canonical_df, "icd_proc_code_2_date"))
            .withColumn("icd_proc_code_3", get_column(canonical_df, "icd_proc_code_3"))
            .withColumn("icd_proc_code_3_descrip", get_column(canonical_df, "icd_proc_code_3_descrip"))
            .withColumn("icd_proc_code_3_date", get_column(canonical_df, "icd_proc_code_3_date"))
            .withColumn("icd_proc_code_4", get_column(canonical_df, "icd_proc_code_4"))
            .withColumn("icd_proc_code_4_descrip", get_column(canonical_df, "icd_proc_code_4_descrip"))
            .withColumn("icd_proc_code_4_date", get_column(canonical_df, "icd_proc_code_4_date"))
            .withColumn("icd_proc_code_5", get_column(canonical_df, "icd_proc_code_5"))
            .withColumn("icd_proc_code_5_descrip", get_column(canonical_df, "icd_proc_code_5_descrip"))
            .withColumn("icd_proc_code_5_date", get_column(canonical_df, "icd_proc_code_5_date"))
            .withColumn("icd_proc_code_6", get_column(canonical_df, "icd_proc_code_6"))
            .withColumn("icd_proc_code_6_descrip", get_column(canonical_df, "icd_proc_code_6_descrip"))
            .withColumn("icd_proc_code_6_date", get_column(canonical_df, "icd_proc_code_6_date"))
            .withColumn("icd_proc_code_7", get_column(canonical_df, "icd_proc_code_7"))
            .withColumn("icd_proc_code_7_descrip", get_column(canonical_df, "icd_proc_code_7_descrip"))
            .withColumn("icd_proc_code_7_date", get_column(canonical_df, "icd_proc_code_7_date"))
            .withColumn("icd_proc_code_8", get_column(canonical_df, "icd_proc_code_8"))
            .withColumn("icd_proc_code_8_descrip", get_column(canonical_df, "icd_proc_code_8_descrip"))
            .withColumn("icd_proc_code_8_date", get_column(canonical_df, "icd_proc_code_8_date"))
            .withColumn("icd_proc_code_9", get_column(canonical_df, "icd_proc_code_9"))
            .withColumn("icd_proc_code_9_descrip", get_column(canonical_df, "icd_proc_code_9_descrip"))
            .withColumn("icd_proc_code_9_date", get_column(canonical_df, "icd_proc_code_9_date"))
            .withColumn("icd_proc_code_10", get_column(canonical_df, "icd_proc_code_10"))
            .withColumn("icd_proc_code_10_descrip", get_column(canonical_df, "icd_proc_code_10_descrip"))
            .withColumn("icd_proc_code_10_date", get_column(canonical_df, "icd_proc_code_10_date"))
            .withColumn("icd_proc_code_11", get_column(canonical_df, "icd_proc_code_11"))
            .withColumn("icd_proc_code_11_descrip", get_column(canonical_df, "icd_proc_code_11_descrip"))
            .withColumn("icd_proc_code_11_date", get_column(canonical_df, "icd_proc_code_11_date"))
            .withColumn("icd_proc_code_12", get_column(canonical_df, "icd_proc_code_12"))
            .withColumn("icd_proc_code_12_descrip", get_column(canonical_df, "icd_proc_code_12_descrip"))
            .withColumn("icd_proc_code_12_date", get_column(canonical_df, "icd_proc_code_12_date"))
            .withColumn("ms_drg_code", get_column(canonical_df, "ms_drg_code"))
            .withColumn("ap_drg_code", get_column(canonical_df, "ap_drg_code"))
            .withColumn("apr_drg_code", get_column(canonical_df, "apr_drg_code"))
            .withColumn("admitting_diagnosis", get_column(canonical_df, "admitting_diagnosis"))
            .withColumn("admitting_diagnosis_description", get_column(canonical_df, "admitting_diagnosis_description"))
            .withColumn("primary_diagnosis", get_column(canonical_df, "primary_diagnosis"))
            .withColumn("primary_diagnosis_description", get_column(canonical_df, "primary_diagnosis_description"))
        )

        custom_claim_df = (
            custom_claim_df.withColumn(
                "diagnosis_code_1_description", get_column(canonical_df, "diagnosis_code_1_description")
            )
            .withColumn("diagnosis_code_1", get_column(canonical_df, "diagnosis_code_1"))
            .withColumn("diagnosis_code_2", get_column(canonical_df, "diagnosis_code_2"))
            .withColumn("diagnosis_code_2_description", get_column(canonical_df, "diagnosis_code_2_description"))
            .withColumn("diagnosis_code_3", get_column(canonical_df, "diagnosis_code_3"))
            .withColumn("diagnosis_code_3_description", get_column(canonical_df, "diagnosis_code_3_description"))
            .withColumn("diagnosis_code_4", get_column(canonical_df, "diagnosis_code_4"))
            .withColumn("diagnosis_code_4_description", get_column(canonical_df, "diagnosis_code_4_description"))
            .withColumn("diagnosis_code_5", get_column(canonical_df, "diagnosis_code_5"))
            .withColumn("diagnosis_code_5_description", get_column(canonical_df, "diagnosis_code_5_description"))
            .withColumn("diagnosis_code_6", get_column(canonical_df, "diagnosis_code_6"))
            .withColumn("diagnosis_code_6_description", get_column(canonical_df, "diagnosis_code_6_description"))
            .withColumn("diagnosis_code_7", get_column(canonical_df, "diagnosis_code_7"))
            .withColumn("diagnosis_code_7_description", get_column(canonical_df, "diagnosis_code_7_description"))
            .withColumn("diagnosis_code_8", get_column(canonical_df, "diagnosis_code_8"))
            .withColumn("diagnosis_code_8_description", get_column(canonical_df, "diagnosis_code_8_description"))
            .withColumn("diagnosis_code_9", get_column(canonical_df, "diagnosis_code_9"))
            .withColumn("diagnosis_code_9_description", get_column(canonical_df, "diagnosis_code_9_description"))
            .withColumn("diagnosis_code_10", get_column(canonical_df, "diagnosis_code_10"))
            .withColumn("diagnosis_code_10_description", get_column(canonical_df, "diagnosis_code_10_description"))
            .withColumn("diagnosis_code_11", get_column(canonical_df, "diagnosis_code_11"))
            .withColumn("diagnosis_code_11_description", get_column(canonical_df, "diagnosis_code_11_description"))
            .withColumn("diagnosis_code_12", get_column(canonical_df, "diagnosis_code_12"))
            .withColumn("diagnosis_code_12_description", get_column(canonical_df, "diagnosis_code_12_description"))
            .withColumn("line_number", get_column(canonical_df, "line_number"))
            .withColumn("line_item_Control_Number", get_column(canonical_df, "line_item_Control_Number"))
            .withColumn("revenue_code", get_column(canonical_df, "revenue_code"))
            .withColumn("cpt_code", get_column(canonical_df, "cpt_code"))
            .withColumn("hcpsc_code", get_column(canonical_df, "hcpsc_code"))
            .withColumn("cpt_code_description", get_column(canonical_df, "cpt_code_description"))
            .withColumn("hcpsc_code_description", get_column(canonical_df, "hcpsc_code_description"))
            .withColumn("svc_modifier_1", get_column(canonical_df, "svc_modifier_1"))
            .withColumn("svc_modifier_1_descrip", get_column(canonical_df, "svc_modifier_1_descrip"))
            .withColumn("svc_modifier_2", get_column(canonical_df, "svc_modifier_2"))
            .withColumn("svc_modifier_2_descrip", get_column(canonical_df, "svc_modifier_2_descrip"))
            .withColumn("svc_modifier_3", get_column(canonical_df, "svc_modifier_3"))
            .withColumn("svc_modifier_3_descrip", get_column(canonical_df, "svc_modifier_3_descrip"))
            .withColumn("svc_modifier_4", get_column(canonical_df, "svc_modifier_4"))
            .withColumn("svc_modifier_4_descrip", get_column(canonical_df, "svc_modifier_4_descrip"))
            .withColumn("place_of_service", get_column(canonical_df, "place_of_service"))
            .withColumn("place_of_service_descrip", get_column(canonical_df, "place_of_service_descrip"))
            .withColumn("svc_line_billed_date", get_column(canonical_df, "svc_line_billed_date"))
            .withColumn("svc_line_start_date", get_column(canonical_df, "svc_line_start_date"))
            .withColumn("svc_line_end_date", get_column(canonical_df, "svc_line_end_date"))
            .withColumn("service_line_disposition", get_column(canonical_df, "service_line_disposition"))
            .withColumn("svc_line_copay_amount", get_column(canonical_df, "svc_line_copay_amount"))
            .withColumn("svc_line_coinsurance_amount", get_column(canonical_df, "svc_line_coinsurance_amount"))
            .withColumn("svc_line_deduct_amount", get_column(canonical_df, "svc_line_deduct_amount"))
            .withColumn("svc_line_charges", get_column(canonical_df, "svc_line_charges"))
            .withColumn("svc_line_units", get_column(canonical_df, "svc_line_units"))
            .withColumn("svc_line_paid_date", get_column(canonical_df, "svc_line_paid_date"))
            .withColumn("svc_line_amount_paid", get_column(canonical_df, "svc_line_amount_paid"))
            .withColumn("service_paid_units", get_column(canonical_df, "service_paid_units"))
            .withColumn("svc_line_network_paid_ind", get_column(canonical_df, "svc_line_network_paid_ind"))
            .withColumn("svc_line_allowed_amount", get_column(canonical_df, "svc_line_allowed_amount"))
            .withColumn("service_line_payment_indicator", get_column(canonical_df, "service_line_payment_indicator"))
            .withColumn("svc_line_adjudication_status", get_column(canonical_df, "svc_line_adjudication_status"))
            .withColumn("svc_rendering_last_name", get_column(canonical_df, "svc_rendering_last_name"))
            .withColumn("svc_rendering_first_name", get_column(canonical_df, "svc_rendering_first_name"))
            .withColumn("svc_rendering_middle_name", get_column(canonical_df, "svc_rendering_middle_name"))
            .withColumn("svc_rendering_npi", get_column(canonical_df, "svc_rendering_npi"))
            .withColumn("prescribing_npi", get_column(canonical_df, "prescribing_npi"))
            .withColumn("svc_rendering_taxonomy_code", get_column(canonical_df, "svc_rendering_taxonomy_code"))
            .withColumn("prescribing_taxonomy_code", get_column(canonical_df, "prescribing_taxonomy_code"))
            .withColumn("prescription_number", get_column(canonical_df, "prescription_number"))
            .withColumn("dispense_date", get_column(canonical_df, "dispense_date"))
            .withColumn("daw_code", get_column(canonical_df, "daw_code"))
            .withColumn("drug_ndc", get_column(canonical_df, "drug_ndc"))
            .withColumn("days_supply", get_column(canonical_df, "days_supply"))
            .withColumn("drug_quantity", get_column(canonical_df, "drug_quantity"))
            .withColumn("drug_strength", get_column(canonical_df, "drug_strength"))
            .withColumn("rx_norm_drug_name", get_column(canonical_df, "rx_norm_drug_name"))
            .withColumn("gpi", get_column(canonical_df, "gpi"))
            .withColumn("ahfs_drug_class", get_column(canonical_df, "ahfs_drug_class"))
            .withColumn("generic_branded_ind", get_column(canonical_df, "generic_branded_ind"))
            .withColumn("mail_order_flag", get_column(canonical_df, "mail_order_flag"))
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
        custom_claim_df.write.format("delta").mode("append").save(delta_table_location)

        log.warn(
            f"spark job {app_name} completed successfully, "
            f"{canonical_file_path} loaded into stage table path {delta_table_location}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
