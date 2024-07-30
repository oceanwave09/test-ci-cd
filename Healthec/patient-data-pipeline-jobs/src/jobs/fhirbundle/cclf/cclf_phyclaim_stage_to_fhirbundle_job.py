import json
import os

import click
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import Row
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w

from dependencies.spark import add_storage_context, start_spark
from utils.enums import ResourceType
from utils.transformation import to_int, transform_date_time
from utils.utils import exit_with_error, load_config, upload_bundle_files

PAT_JINJA_TEMPLATE = "patient.j2"
COND_JINJA_TEMPLATE = "condition.j2"
PRC_JINJA_TEMPLATE = "practitioner.j2"
CLM_JINJA_TEMPLATE = "claim.j2"


def _transform_patient(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("patient_internal_id", "")
    data_dict["medicare_number"] = row_dict.get("bene_mbi_id", "")
    data_dict["medicare_system"] = "http://hl7.org/fhir/sid/us-mbi"
    data_dict["active"] = "true"
    return _transform_resource(
        data_dict=data_dict,
        template=PAT_JINJA_TEMPLATE,
        resource_type=ResourceType.Patient.value,
        transformer=transformer,
    )


def _transform_condition(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {"patient_id": row_dict.get("patient_internal_id", "")}
    data_dict["internal_id"] = row_dict.get("condition_internal_id", "")
    data_dict["code"] = row_dict.get("diagnosis_code", "")
    data_dict["code_system"] = row_dict.get("diagnosis_code_system", "")
    return _transform_resource(
        data_dict=data_dict,
        template=COND_JINJA_TEMPLATE,
        resource_type=ResourceType.Condition.value,
        transformer=transformer,
    )


def _transform_practitioner(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("practitioner_internal_id", "")
    data_dict["npi"] = row_dict.get("provider_npi", "")
    return _transform_resource(
        data_dict=data_dict,
        template=PRC_JINJA_TEMPLATE,
        resource_type=ResourceType.Practitioner.value,
        transformer=transformer,
    )


def _transform_claim(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("claim_internal_id", "")
    data_dict["patient_id"] = row_dict.get("patient_internal_id", "")
    data_dict["provider_organization_id"] = row_dict.get("claim_provider_organization_id", "")
    data_dict["source_id"] = row_dict.get("cur_clm_uniq_id", "")
    data_dict["use"] = "claim"
    # Testing
    data_dict["status"] = "active"
    # data_dict["status"] = row_dict.get("clm_adjsmt_type_cd", "")
    data_dict["type_code"] = "professional"
    data_dict["type_system"] = "http://terminology.hl7.org/CodeSystem/claim-type"
    data_dict["type_display"] = "Professional"
    data_dict["type_text"] = "Professional"
    # data_dict["type_code"] = row_dict.get("claim_type_code", "")
    # data_dict["type_system"] = "https://bluebutton.cms.gov/resources/variables/nch_clm_type_cd"
    data_dict["billable_period_start"] = row_dict.get("claim_period_start", "")
    data_dict["billable_period_end"] = row_dict.get("claim_period_end", "")

    if row_dict.get("diagnoses"):
        diagnoses = []
        for diag_entry in row_dict.get("diagnoses", []):
            diagnoses.append(
                {
                    "sequence": to_int(str(diag_entry.get("sequence"))),
                    "diagnosis_condition_id": diag_entry.get("condition_internal_id", ""),
                }
            )
        data_dict["diagnoses"] = diagnoses
    if row_dict.get("care_team"):
        care_team = []
        for care_team_entry in row_dict.get("care_team", []):
            care_team.append(
                {
                    "sequence": to_int(str(care_team_entry.get("sequence"))),
                    "provider_practitioner_id": care_team_entry.get("practitioner_internal_id", ""),
                    "role_code": care_team_entry.get("provider_role", ""),
                    "role_system": care_team_entry.get("provider_role_system", ""),
                }
            )
        data_dict["care_team"] = care_team
    if row_dict.get("service_lines"):
        service_lines = []
        for line_entry in row_dict.get("service_lines", []):
            service_line = {"sequence": to_int(str(line_entry.get("sequence")))}

            care_team_sequences = line_entry.get("care_team_sequences", [])
            if care_team_sequences:
                service_line["careteam_sequence"] = ",".join(map(str, care_team_sequences))

            diag_sequences = line_entry.get("diag_sequences", [])
            if diag_sequences:
                service_line["diagnosis_sequence"] = ",".join(map(str, diag_sequences))

            if line_entry.get("service_period_start", ""):
                service_line["service_period_start"] = line_entry.get("service_period_start", "")
            if line_entry.get("service_period_end", ""):
                service_line["service_period_end"] = line_entry.get("service_period_end", "")
            service_line["revenue_code"] = line_entry.get("revenue_code", "")
            service_line["revenue_system"] = line_entry.get("revenue_system", "")
            if line_entry.get("clm_line_srvc_unit_qty", ""):
                service_line["quantity_value"] = line_entry.get("clm_line_srvc_unit_qty")
            if line_entry.get("product_or_service"):
                product_or_service = []
                for prod_entry in line_entry.get("product_or_service", []):
                    product_or_service.append(
                        {
                            "code": prod_entry.get("code", ""),
                            "system": "https://bluebutton.cms.gov/resources/codesystem/hcpcs",
                        }
                    )
                service_line["product_or_service"] = product_or_service
            if line_entry.get("svc_modifiers"):
                svc_modifiers = []
                for mod_entry in line_entry.get("svc_modifiers"):
                    svc_modifiers.append(
                        {
                            "modifier_code": mod_entry.get("modifier_code", ""),
                            "modifier_system": mod_entry.get("modifier_system", ""),
                        }
                    )
                service_line["svc_modifiers"] = svc_modifiers
            service_lines.append(service_line)
        data_dict["service_lines"] = service_lines
    return _transform_resource(
        data_dict=data_dict,
        template=CLM_JINJA_TEMPLATE,
        resource_type=ResourceType.Claim.value,
        transformer=transformer,
    )


def _transform_resource(data_dict: dict, template: str, resource_type, transformer: FHIRTransformer) -> dict:
    transformer.load_template(template)
    return json.loads(transformer.render_resource(resource_type, data_dict))


def render_resources(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    resources = []
    # render resources
    resources.append(_transform_patient(row_dict, transformer))
    for entry in row_dict.get("care_team", []):
        if entry.get("provider_npi"):
            resources.append(_transform_practitioner(entry, transformer))
    for entry in row_dict.get("diagnoses", []):
        if entry.get("diagnosis_code"):
            entry.update({"patient_internal_id": row_dict.get("patient_internal_id", "")})
            resources.append(_transform_condition(entry, transformer))
    resources.append(_transform_claim(row_dict, transformer))

    return Row(
        **{
            "resource_bundle": json.dumps(
                {
                    "resourceType": "Bundle",
                    "id": row_dict.get("bundle_id"),
                    "type": "batch",
                    "entry": resources,
                }
            )
        }
    )


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="cclf_phyclaim_stage_to_fhirbundle_job",
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
            exit_with_error(log, "delta schema location should be provided!")

        # construct delta table location
        cclf5_delta_table_location = os.path.join(delta_schema_location, "cclf5")
        log.warn(f"cclf5_delta_table_location: {cclf5_delta_table_location}")

        file_batch_id = os.environ.get("FILE_BATCH_ID", config.get("file_batch_id", ""))
        if not file_batch_id:
            exit_with_error(log, "file batch id should be provided!")
        log.warn(f"file_batch_id: {file_batch_id}")

        file_source = os.environ.get("FILE_SOURCE", config.get("file_source", ""))
        if not file_source:
            exit_with_error(log, "file source should be provided!")
        log.warn(f"file_source: {file_source}")

        landing_path = os.environ.get("LANDING_PATH", config.get("landing_path", ""))
        if not landing_path:
            exit_with_error(log, "landing path should be provided!")
        log.warn(f"landing_path: {landing_path}")

        pipeline_data_key = os.environ.get("PIPELINE_DATA_KEY", config.get("pipeline_data_key", ""))
        if not pipeline_data_key:
            exit_with_error(log, "pipeline data key should be provided!")

        fhirbundle_landing_path = os.path.join(landing_path, file_source)
        log.warn(f"fhirbundle_landing_path: {fhirbundle_landing_path}")

        file_tenant = os.environ.get("FILE_TENANT", config.get("file_tenant", ""))
        log.warn(f"file_tenant: {file_tenant}")

        resource_type = os.environ.get("RESOURCE_TYPE", config.get("resource_type", ""))
        log.warn(f"resource_type: {resource_type}")

        src_file_name = os.environ.get("SRC_FILE_NAME", config.get("src_file_name", ""))
        log.warn(f"src_file_name: {src_file_name}")

        src_organization_id = os.environ.get("SRC_ORGANIZATION_ID", config.get("src_organization_id", ""))
        log.warn(f"src_organization_id: {src_organization_id}")

        # Change FHIRBUNDLE to FHIRBUNDLE_BULK
        fhirbundle_landing_path = fhirbundle_landing_path.replace("FHIRBUNDLE", "FHIRBUNDLE_BULK")

        # Construct fhir bundle temp path
        fhir_temp_path = fhirbundle_landing_path.replace("landing", "temporary")
        fhir_bundle_temp_path = os.path.join(fhir_temp_path, file_batch_id)

        # construct metadata
        metadata = {
            "file_tenant": file_tenant,
            "file_source": file_source,
            "resource_type": resource_type,
            "file_batch_id": file_batch_id,
            "src_file_name": src_file_name,
            "file_type": "phy_claim",
            "src_organization_id": src_organization_id,
        }

        # add storage context in spark session
        spark = add_storage_context(spark, [cclf5_delta_table_location])

        # load the records from delta table location
        log.warn("load records from cclf5 delta table location")
        cclf5_data_df = (
            spark.read.format("delta")
            .load(cclf5_delta_table_location)
            .filter(f.col("file_batch_id") == file_batch_id)
            .drop(
                "file_batch_id",
                "file_name",
                "file_source_name",
                "file_status",
                "created_user",
                "created_ts",
                "updated_user",
                "updated_ts",
            )
            .fillna("")
        )

        cclf5_data_df = cclf5_data_df.withColumn(
            "bene_mbi_id",
            f.expr(f"aes_decrypt(unhex(bene_mbi_id) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
        ).withColumn(
            "rndrg_prvdr_npi_num",
            f.expr(f"aes_decrypt(unhex(rndrg_prvdr_npi_num) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
        )
        cclf5_data_df.persist()

        # Apply transformations to date time field
        cclf5_data_df = (
            cclf5_data_df.withColumn("claim_period_start", transform_date_time(f.col("clm_from_dt")))
            .withColumn("claim_period_end", transform_date_time(f.col("clm_thru_dt")))
            .withColumn("service_period_start", transform_date_time(f.col("clm_line_from_dt")))
            .withColumn("service_period_end", transform_date_time(f.col("clm_line_thru_dt")))
        )

        # get claim details
        claim_df = cclf5_data_df.select(
            [
                "cur_clm_uniq_id",
                "bene_mbi_id",
                "clm_type_cd",
                "claim_period_start",
                "claim_period_end",
                "clm_carr_pmt_dnl_cd",
                "clm_adjsmt_type_cd",
                "clm_cntl_num",
            ]
        ).drop_duplicates()
        claim_df.persist()

        # collect diagnoses at line level
        log.warn("collect line diagnosis codes")
        line_diag_df = (
            cclf5_data_df.withColumn(
                "diagnosis_code_list",
                f.array(
                    f.col("clm_line_dgns_cd"),
                    f.col("clm_dgns_1_cd"),
                    f.col("clm_dgns_2_cd"),
                    f.col("clm_dgns_3_cd"),
                    f.col("clm_dgns_4_cd"),
                    f.col("clm_dgns_5_cd"),
                    f.col("clm_dgns_6_cd"),
                    f.col("clm_dgns_7_cd"),
                    f.col("clm_dgns_8_cd"),
                    f.col("clm_dgns_9_cd"),
                    f.col("clm_dgns_10_cd"),
                    f.col("clm_dgns_11_cd"),
                    f.col("clm_dgns_12_cd"),
                ),
            )
            .withColumn("diagnosis_code_list", f.array_distinct(f.expr("filter(diagnosis_code_list, x -> x != '')")))
            .withColumn("diagnosis_code", f.explode(f.col("diagnosis_code_list")))
            .withColumn(
                "diagnosis_code_system",
                f.when(f.col("dgns_prcdr_icd_ind") == "0", f.lit("http://hl7.org/fhir/sid/icd-10-cm"))
                .when(f.col("dgns_prcdr_icd_ind") == "9", f.lit("http://hl7.org/fhir/sid/icd-9-cm"))
                .otherwise(f.lit("https://bluebutton.cms.gov/resources/codesystem/diagnosis-type")),
            )
            .select(["cur_clm_uniq_id", "clm_line_num", "diagnosis_code", "diagnosis_code_system"])
        )
        line_diag_df.persist()
        # consolidate claim diagnoses
        claim_diag_df = line_diag_df.select(
            ["cur_clm_uniq_id", "diagnosis_code", "diagnosis_code_system"]
        ).drop_duplicates()
        diag_window_spec = w.partitionBy("cur_clm_uniq_id").orderBy("cur_clm_uniq_id")
        claim_diag_seq_df = claim_diag_df.withColumn("sequence", f.row_number().over(diag_window_spec))
        claim_diag_seq_df.persist()
        # get diagnosis sequences at line level
        line_diag_seq_df = line_diag_df.join(
            claim_diag_seq_df,
            (line_diag_df.cur_clm_uniq_id == claim_diag_seq_df.cur_clm_uniq_id)
            & (line_diag_df.diagnosis_code == claim_diag_seq_df.diagnosis_code),
        ).select(line_diag_df["*"], claim_diag_seq_df["sequence"])
        line_diag_seq_df = (
            line_diag_seq_df.groupBy(["cur_clm_uniq_id", "clm_line_num"])
            .agg(f.collect_list("sequence").alias("diag_sequences"))
            .select(["cur_clm_uniq_id", "clm_line_num", "diag_sequences"])
        )
        line_diag_seq_df.persist()

        claim_diag_final_df = claim_diag_seq_df.withColumn("condition_internal_id", f.expr("uuid()"))
        claim_diag_final_df = claim_diag_final_df.groupBy("cur_clm_uniq_id").agg(
            f.collect_list(
                f.struct(
                    f.col("sequence"),
                    f.col("diagnosis_code"),
                    f.col("diagnosis_code_system"),
                    f.col("condition_internal_id"),
                )
            ).alias("diagnoses")
        )
        claim_diag_final_df.persist()

        # collect modifiers at line level
        line_mod_df = (
            cclf5_data_df.withColumn(
                "modifier_code_list",
                f.array(
                    f.col("hcpcs_1_mdfr_cd"),
                    f.col("hcpcs_2_mdfr_cd"),
                    f.col("hcpcs_3_mdfr_cd"),
                    f.col("hcpcs_4_mdfr_cd"),
                    f.col("hcpcs_5_mdfr_cd"),
                ),
            )
            .withColumn("modifier_code_list", f.array_distinct(f.expr("filter(modifier_code_list, x -> x != '')")))
            .withColumn("modifier_code", f.explode(f.col("modifier_code_list")))
            .withColumn("modifier_system", f.lit("https://bluebutton.cms.gov/resources/codesystem/hcpcs"))
            .select(["cur_clm_uniq_id", "clm_line_num", "modifier_code", "modifier_system"])
        )
        line_mod_df = (
            line_mod_df.groupBy(["cur_clm_uniq_id", "clm_line_num"])
            .agg(f.collect_list(f.struct(f.col("modifier_code"), f.col("modifier_system"))).alias("svc_modifiers"))
            .select(["cur_clm_uniq_id", "clm_line_num", "svc_modifiers"])
        )
        line_mod_df.persist()

        # collect care team at line level
        line_care_team_df = (
            cclf5_data_df.filter(f.col("rndrg_prvdr_npi_num") != "")
            .withColumn("provider_npi", f.col("rndrg_prvdr_npi_num"))
            .withColumn("provider_role", f.lit("operating"))
            .withColumn(
                "provider_role_system", f.lit("http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimCareTeamRole")
            )
            .select(["cur_clm_uniq_id", "clm_line_num", "provider_npi", "provider_role", "provider_role_system"])
        )
        line_care_team_df.persist()
        # consolidate claim care team
        claim_care_team_df = line_care_team_df.select(
            ["cur_clm_uniq_id", "provider_npi", "provider_role", "provider_role_system"]
        ).drop_duplicates()
        care_team_window_spec = w.partitionBy("cur_clm_uniq_id").orderBy("cur_clm_uniq_id")
        claim_care_team_seq_df = claim_care_team_df.withColumn("sequence", f.row_number().over(care_team_window_spec))
        claim_care_team_seq_df.persist()
        # get diagnosis sequences at line level
        line_care_team_seq_df = line_care_team_df.join(
            claim_care_team_seq_df,
            (line_care_team_df.cur_clm_uniq_id == claim_care_team_seq_df.cur_clm_uniq_id)
            & (line_care_team_df.provider_npi == claim_care_team_seq_df.provider_npi),
        ).select(line_care_team_df["*"], claim_care_team_seq_df["sequence"])
        line_care_team_seq_df = (
            line_care_team_seq_df.groupBy(["cur_clm_uniq_id", "clm_line_num"])
            .agg(f.collect_list("sequence").alias("care_team_sequences"))
            .select(["cur_clm_uniq_id", "clm_line_num", "care_team_sequences"])
        )
        line_care_team_seq_df.persist()

        claim_care_team_final_df = claim_care_team_seq_df.withColumn("practitioner_internal_id", f.expr("uuid()"))
        claim_care_team_final_df = claim_care_team_final_df.groupBy("cur_clm_uniq_id").agg(
            f.collect_list(
                f.struct(
                    f.col("sequence"),
                    f.col("provider_npi"),
                    f.col("provider_role"),
                    f.col("provider_role_system"),
                    f.col("practitioner_internal_id"),
                )
            ).alias("care_team")
        )
        claim_care_team_final_df.persist()

        # consolidate claim service lines
        line_df = cclf5_data_df.select(
            [
                "cur_clm_uniq_id",
                "clm_line_num",
                "clm_fed_type_srvc_cd",
                "clm_pos_cd",
                "service_period_start",
                "service_period_end",
                "clm_line_hcpcs_cd",
                "clm_line_srvc_unit_qty",
            ]
        )
        line_df = (
            line_df.join(line_mod_df, on=["cur_clm_uniq_id", "clm_line_num"], how="left")
            .join(line_diag_seq_df, on=["cur_clm_uniq_id", "clm_line_num"], how="left")
            .join(line_care_team_seq_df, on=["cur_clm_uniq_id", "clm_line_num"], how="left")
        )
        claim_line_final_df = line_df.groupBy("cur_clm_uniq_id").agg(
            f.collect_list(
                f.struct(
                    f.col("clm_line_num").alias("sequence"),
                    f.col("service_period_start"),
                    f.col("service_period_end"),
                    f.array(
                        f.struct(
                            f.col("clm_line_hcpcs_cd").alias("code"),
                            f.lit("https://bluebutton.cms.gov/resources/codesystem/hcpcs").alias("system"),
                        )
                    ).alias("product_or_service"),
                    f.col("clm_line_srvc_unit_qty"),
                    f.col("svc_modifiers"),
                    f.col("diag_sequences"),
                    f.col("care_team_sequences"),
                )
            ).alias("service_lines")
        )
        claim_line_final_df.persist()

        # construct final claim dataframe
        claim_final_df = (
            claim_df.join(claim_diag_final_df, on="cur_clm_uniq_id", how="left")
            .join(claim_care_team_final_df, on="cur_clm_uniq_id", how="left")
            .join(claim_line_final_df, on="cur_clm_uniq_id", how="left")
        )

        claim_final_df = (
            claim_final_df.withColumn("claim_internal_id", f.expr("uuid()"))
            .withColumn("patient_internal_id", f.expr("uuid()"))
            .withColumn("claim_provider_organization_id", f.expr("uuid()"))
            .withColumn("bundle_id", f.expr("uuid()"))
        )

        # processing row wise operation
        transformer = FHIRTransformer()
        data_rdd = claim_final_df.rdd.map(lambda row: render_resources(row, transformer))
        resources_df = spark.createDataFrame(data_rdd)
        resources_df.write.mode("overwrite").text(fhir_bundle_temp_path)

        upload_bundle_files(
            fhir_bundle_temp_path=fhir_bundle_temp_path,
            landing_path=fhirbundle_landing_path,
            metadata=metadata,
            enc_data_key=pipeline_data_key,
        )

        log.warn(
            f"spark job {app_name} completed successfully, "
            f"from file with batch id {file_batch_id} transformed "
            f"into fhir bundle and copied to {fhirbundle_landing_path}."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
