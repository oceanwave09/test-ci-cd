import json
import os

import click
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import Row
from pyspark.sql import functions as f
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window as w

from dependencies.spark import add_storage_context, start_spark
from utils.enums import ResourceType
from utils.transformation import to_int, transform_date_time
from utils.utils import exit_with_error, load_config, upload_bundle_files

PAT_JINJA_TEMPLATE = "patient.j2"
ORG_JINJA_TEMPLATE = "organization.j2"
COND_JINJA_TEMPLATE = "condition.j2"
PROC_JINJA_TEMPLATE = "procedure.j2"
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


def _transform_provider_organization(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("organization_internal_id", "")
    data_dict["provider_number"] = row_dict.get("prvdr_oscar_num", "")

    return _transform_resource(
        data_dict=data_dict,
        template=ORG_JINJA_TEMPLATE,
        resource_type=ResourceType.Organization.value,
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


def _transform_procedure(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {"patient_id": row_dict.get("patient_internal_id", "")}
    data_dict["internal_id"] = row_dict.get("procedure_internal_id", "")
    data_dict["code"] = row_dict.get("procedure_code", "")
    data_dict["performed_date_time"] = row_dict.get("performed_date_time", "")
    return _transform_resource(
        data_dict=data_dict,
        template=PROC_JINJA_TEMPLATE,
        resource_type=ResourceType.Procedure.value,
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
    data_dict["provider_organization_id"] = row_dict.get("organization_internal_id", "")
    data_dict["source_id"] = row_dict.get("cur_clm_uniq_id", "")
    data_dict["use"] = "claim"
    # Testing
    # data_dict["status"] = row_dict.get("clm_adjsmt_type_cd", "")
    data_dict["status"] = "active"
    data_dict["type_code"] = "institutional"
    data_dict["type_system"] = "http://terminology.hl7.org/CodeSystem/claim-type"
    data_dict["type_display"] = "Institutional"
    data_dict["type_text"] = "Institutional"
    # data_dict["type_code"] = row.get("clm_type_cd", "")
    # data_dict["type_system"] = "https://bluebutton.cms.gov/resources/variables/nch_clm_type_cd"
    data_dict["billable_period_start"] = row_dict.get("claim_period_start")
    data_dict["billable_period_end"] = row_dict.get("claim_period_end")
    data_dict["total_value"] = row_dict.get("clm_mdcr_instnl_tot_chrg_amt")

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
    if row_dict.get("procedures"):
        procedures = []
        for proc_entry in row_dict.get("procedures", []):
            procedures.append(
                {
                    "sequence": to_int(str(proc_entry.get("sequence"))),
                    "procedure_id": proc_entry.get("procedure_internal_id", ""),
                }
            )
        data_dict["procedures"] = procedures
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
    if row_dict.get("supporting_info"):
        support_info = []
        for support_info_entry in row_dict.get("supporting_info", []):
            support_info.append(
                {
                    "sequence": to_int(str(support_info_entry.get("sequence"))),
                    "code": support_info_entry.get("code", ""),
                    "code_system": support_info_entry.get("code_system", ""),
                }
            )
        data_dict["support_info"] = support_info
    if row_dict.get("service_lines"):
        service_lines = []
        for line_entry in row_dict.get("service_lines", []):
            service_line = {"sequence": to_int(str(line_entry.get("sequence")))}
            care_team_sequences = line_entry.get("careteam_sequence", [])
            if care_team_sequences:
                service_line["careteam_sequence"] = ",".join(map(str, care_team_sequences))
            proc_sequences = line_entry.get("procedure_sequence", [])
            if proc_sequences:
                service_line["procedure_sequence"] = ",".join(map(str, proc_sequences))
            diag_sequences = line_entry.get("diagnosis_sequence", [])
            if diag_sequences:
                service_line["diagnosis_sequence"] = ",".join(map(str, diag_sequences))

            if line_entry.get("service_period_start" ""):
                service_line["service_period_start"] = line_entry.get("service_period_start" "")
            if line_entry.get("service_period_end" ""):
                service_line["service_period_end"] = line_entry.get("service_period_end" "")
            service_line["revenue_code"] = line_entry.get("revenue_code", "")
            service_line["revenue_system"] = line_entry.get("revenue_system", "")
            if line_entry.get("clm_line_srvc_unit_qty", ""):
                service_line["quantity_value"] = line_entry.get("clm_line_srvc_unit_qty")
            if line_entry.get("product_or_service"):
                service_line["product_or_service"] = line_entry.get("product_or_service")
            if line_entry.get("svc_modifiers"):
                svc_modifiers = []
                for mod_entry in line_entry.get("svc_modifiers", []):
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
    if row_dict.get("prvdr_oscar_num"):
        resources.append(_transform_provider_organization(row_dict, transformer))
    for entry in row_dict.get("care_team", []):
        if entry.get("provider_npi"):
            resources.append(_transform_practitioner(entry, transformer))
    for entry in row_dict.get("diagnoses", []):
        if entry.get("diagnosis_code"):
            entry.update({"patient_internal_id": row_dict.get("patient_internal_id", "")})
            resources.append(_transform_condition(entry, transformer))
    for entry in row_dict.get("procedures", []):
        if entry.get("procedure_code"):
            entry.update({"patient_internal_id": row_dict.get("patient_internal_id", "")})
            resources.append(_transform_procedure(entry, transformer))
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
    default="cclf_insclaim_stage_to_fhirbundle_job",
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
        cclf1_delta_table_location = os.path.join(delta_schema_location, "cclf1")
        log.warn(f"cclf1_delta_table_location: {cclf1_delta_table_location}")

        cclf2_delta_table_location = os.path.join(delta_schema_location, "cclf2")
        log.warn(f"cclf2_delta_table_location: {cclf2_delta_table_location}")

        cclf3_delta_table_location = os.path.join(delta_schema_location, "cclf3")
        log.warn(f"cclf3_delta_table_location: {cclf3_delta_table_location}")

        cclf4_delta_table_location = os.path.join(delta_schema_location, "cclf4")
        log.warn(f"cclf4_delta_table_location: {cclf4_delta_table_location}")

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
            "file_type": "ins_claim",
            "src_organization_id": src_organization_id,
        }

        # add storage context in spark session
        spark = add_storage_context(
            spark,
            [
                cclf1_delta_table_location,
                cclf2_delta_table_location,
                cclf3_delta_table_location,
                cclf4_delta_table_location,
            ],
        )

        # load the records from delta table location
        log.warn("load records from cclf1 delta table location")
        cclf1_data_df = (
            spark.read.format("delta")
            .load(cclf1_delta_table_location)
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

        claim_df = (
            cclf1_data_df.withColumn(
                "bene_mbi_id",
                f.expr(f"aes_decrypt(unhex(bene_mbi_id) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "oprtg_prvdr_npi_num",
                f.expr(f"aes_decrypt(unhex(oprtg_prvdr_npi_num) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "atndg_prvdr_npi_num",
                f.expr(f"aes_decrypt(unhex(atndg_prvdr_npi_num) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "othr_prvdr_npi_num",
                f.expr(f"aes_decrypt(unhex(othr_prvdr_npi_num) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
        )
        claim_df.persist()

        # collect claim diagnosis
        log.warn("collect claim diagnosis codes")
        cclf1_diag_df = (
            claim_df.withColumn("diagnosis_code_list", f.array(f.col("prncpl_dgns_cd"), f.col("admtg_dgns_cd")))
            .withColumn("diagnosis_code_list", f.array_distinct(f.expr("filter(diagnosis_code_list, x -> x != '')")))
            .withColumn("diagnosis_code", f.explode(f.col("diagnosis_code_list")))
            .withColumn("clm_val_sqnc_num", f.lit("None"))
            .select(["cur_clm_uniq_id", "clm_val_sqnc_num", "diagnosis_code", "dgns_prcdr_icd_ind"])
        )
        cclf4_data_df = (
            spark.read.format("delta")
            .load(cclf4_delta_table_location)
            .filter(f.col("file_batch_id") == file_batch_id)
            .withColumnRenamed("clm_dgns_cd", "diagnosis_code")
            .select(["cur_clm_uniq_id", "clm_val_sqnc_num", "diagnosis_code", "dgns_prcdr_icd_ind"])
            .fillna("")
        )

        claim_diag_df = (
            cclf4_data_df.union(cclf1_diag_df)
            .filter(f.col("diagnosis_code") != "")
            .withColumn(
                "diagnosis_code_system",
                f.when(f.col("dgns_prcdr_icd_ind") == "0", f.lit("http://hl7.org/fhir/sid/icd-10-cm"))
                .when(f.col("dgns_prcdr_icd_ind") == "9", f.lit("http://hl7.org/fhir/sid/icd-9-cm"))
                .otherwise(f.lit("https://bluebutton.cms.gov/resources/codesystem/diagnosis-type")),
            )
            .dropDuplicates(["diagnosis_code"])
        )
        diag_window_spec = w.partitionBy("cur_clm_uniq_id").orderBy(["cur_clm_uniq_id", "clm_val_sqnc_num"])
        claim_diag_seq_df = (
            claim_diag_df.withColumn("sequence", f.row_number().over(diag_window_spec))
            .withColumn("condition_internal_id", f.expr("uuid()"))
            .select(["cur_clm_uniq_id", "sequence", "diagnosis_code", "diagnosis_code_system", "condition_internal_id"])
        )

        claim_diag_seq_df = (
            claim_diag_seq_df.groupBy("cur_clm_uniq_id")
            .agg(
                f.collect_list(
                    f.struct(
                        f.col("sequence"),
                        f.col("diagnosis_code"),
                        f.col("diagnosis_code_system"),
                        f.col("condition_internal_id"),
                    )
                ).alias("diagnoses")
            )
            .select(["cur_clm_uniq_id", "diagnoses"])
        )
        claim_diag_seq_df.persist()

        # collect claim procedure
        log.warn("collect claim procedure codes")
        cclf3_data_df = (
            spark.read.format("delta")
            .load(cclf3_delta_table_location)
            .filter(f.col("file_batch_id") == file_batch_id)
            .filter(f.col("clm_prcdr_cd").isNotNull())
            .withColumnRenamed("clm_val_sqnc_num", "sequence")
            .withColumnRenamed("clm_prcdr_cd", "procedure_code")
            .withColumnRenamed("clm_prcdr_prfrm_dt", "performed_date_time")
            .withColumn("procedure_internal_id", f.expr("uuid()"))
            .select(
                [
                    "cur_clm_uniq_id",
                    "sequence",
                    "procedure_code",
                    "performed_date_time",
                    "procedure_internal_id",
                ]
            )
            .fillna("")
        )

        cclf3_data_df = cclf3_data_df.withColumn(
            "performed_date_time", transform_date_time(f.col("performed_date_time"))
        )

        claim_proc_df = (
            cclf3_data_df.groupBy("cur_clm_uniq_id")
            .agg(
                f.collect_list(
                    f.struct(
                        f.col("sequence"),
                        f.col("procedure_code"),
                        f.col("performed_date_time"),
                        f.col("procedure_internal_id"),
                    )
                ).alias("procedures")
            )
            .select(["cur_clm_uniq_id", "procedures"])
        )
        claim_proc_df.persist()

        # collect claim service lines
        log.warn("collect claim service lines")
        cclf2_data_df = (
            spark.read.format("delta")
            .load(cclf2_delta_table_location)
            .filter(f.col("file_batch_id") == file_batch_id)
            .select(
                [
                    "cur_clm_uniq_id",
                    "clm_line_num",
                    "clm_line_from_dt",
                    "clm_line_thru_dt",
                    "clm_line_prod_rev_ctr_cd",
                    "clm_line_instnl_rev_ctr_dt",
                    "clm_line_hcpcs_cd",
                    "clm_line_srvc_unit_qty",
                    "hcpcs_1_mdfr_cd",
                    "hcpcs_2_mdfr_cd",
                    "hcpcs_3_mdfr_cd",
                    "hcpcs_4_mdfr_cd",
                    "hcpcs_5_mdfr_cd",
                    "clm_rev_apc_hipps_cd",
                ]
            )
        )

        cclf2_data_df = cclf2_data_df.withColumn(
            "service_period_start",
            f.when(f.col("clm_line_from_dt").isNotNull(), transform_date_time(f.col("clm_line_from_dt")))
            .when(
                f.col("clm_line_instnl_rev_ctr_dt").isNotNull(),
                transform_date_time(f.col("clm_line_instnl_rev_ctr_dt")),
            )
            .otherwise(f.lit("")),
        ).withColumn("service_period_end", transform_date_time(f.col("clm_line_thru_dt")))

        cclf2_data_df.persist()

        # combine modifiers at line level
        line_mod_df = (
            cclf2_data_df.withColumn(
                "modifier_code_list",
                f.array(
                    f.col("hcpcs_1_mdfr_cd"),
                    f.col("hcpcs_2_mdfr_cd"),
                    f.col("hcpcs_3_mdfr_cd"),
                    f.col("hcpcs_4_mdfr_cd"),
                    f.col("hcpcs_5_mdfr_cd"),
                ),
            )
            .withColumn("modifier_code_list", f.array_distinct(f.expr("filter(modifier_code_list, x -> x is not null)")))
            .withColumn("modifier_code", f.explode(f.col("modifier_code_list")))
            .withColumn("modifier_system", f.lit("https://bluebutton.cms.gov/resources/codesystem/hcpcs"))
            .select(["cur_clm_uniq_id", "clm_line_num", "modifier_code", "modifier_system"])
        )
        line_mod_df = (
            line_mod_df.groupBy(["cur_clm_uniq_id", "clm_line_num"])
            .agg(f.collect_list(f.struct(f.col("modifier_code"), f.col("modifier_system"))).alias("svc_modifiers"))
            .select(["cur_clm_uniq_id", "clm_line_num", "svc_modifiers"])
        )
        line_df = cclf2_data_df.join(line_mod_df, on=["cur_clm_uniq_id", "clm_line_num"], how="left")
        claim_line_df = line_df.groupBy("cur_clm_uniq_id").agg(
            f.collect_list(
                f.struct(
                    f.col("clm_line_num").alias("sequence"),
                    f.col("service_period_start"),
                    f.col("service_period_end"),
                    f.when(f.col("clm_line_prod_rev_ctr_cd").isNotNull(), f.col("clm_line_prod_rev_ctr_cd"))
                    .when(f.col("clm_rev_apc_hipps_cd").isNotNull(), f.col("clm_rev_apc_hipps_cd"))
                    .otherwise(f.lit("None"))
                    .alias("revenue_code"),
                    f.lit("https://bluebutton.cms.gov/resources/variables/rev_cntr").alias("revenue_system"),
                    f.array(
                        f.struct(
                            f.col("clm_line_hcpcs_cd").alias("code"),
                            f.lit("https://bluebutton.cms.gov/resources/codesystem/hcpcs").alias("system"),
                        )
                    ).alias("product_or_service"),
                    f.col("clm_line_srvc_unit_qty"),
                    f.col("svc_modifiers"),
                ),
            ).alias("service_lines")
        )

        claim_line_df.persist()

        # collect claim supporting info
        sup_info_schema = StructType(
            [
                StructField("code", StringType(), True),
                StructField("code_system", StringType(), True),
            ]
        )
        sup_info_df = (
            claim_df.withColumn(
                "sup_info_1",
                f.when(
                    f.col("clm_nch_prmry_pyr_cd") != "",
                    f.struct(
                        f.col("clm_nch_prmry_pyr_cd").alias("code"),
                        f.lit("https://bluebutton.cms.gov/resources/variables/nch_prmry_pyr_cd").alias("code_system"),
                    ),
                ).otherwise(f.lit(None).cast(sup_info_schema)),
            )
            .withColumn(
                "sup_info_2",
                f.when(
                    f.col("bene_ptnt_stus_cd") != "",
                    f.struct(
                        f.col("bene_ptnt_stus_cd").alias("code"),
                        f.lit("https://bluebutton.cms.gov/resources/variables/ptnt_dschrg_stus_cd").alias("code_system"),
                    ),
                ).otherwise(f.lit(None).cast(sup_info_schema)),
            )
            .withColumn(
                "sup_info_3",
                f.when(
                    f.col("dgns_drg_cd") != "",
                    f.struct(
                        f.col("dgns_drg_cd").alias("code"),
                        f.lit("https://bluebutton.cms.gov/resources/variables/clm_drg_cd").alias("code_system"),
                    ),
                ).otherwise(f.lit(None).cast(sup_info_schema)),
            )
            .withColumn(
                "sup_info_4",
                f.when(
                    f.col("clm_admsn_type_cd") != "",
                    f.struct(
                        f.col("clm_admsn_type_cd").alias("code"),
                        f.lit("https://bluebutton.cms.gov/resources/variables/clm_ip_admsn_type_cd").alias(
                            "code_system"
                        ),
                    ),
                ).otherwise(f.lit(None).cast(sup_info_schema)),
            )
            .withColumn(
                "sup_info_5",
                f.when(
                    f.col("clm_admsn_src_cd") != "",
                    f.struct(
                        f.col("clm_admsn_src_cd").alias("code"),
                        f.lit("https://bluebutton.cms.gov/resources/variables/clm_src_ip_admsn_cd").alias("code_system"),
                    ),
                ).otherwise(f.lit(None).cast(sup_info_schema)),
            )
            .withColumn(
                "sup_info_6",
                f.when(
                    f.col("clm_bill_freq_cd") != "",
                    f.struct(
                        f.col("clm_bill_freq_cd").alias("code"),
                        f.lit("https://bluebutton.cms.gov/resources/variables/clm_freq_cd").alias("code_system"),
                    ),
                ).otherwise(f.lit(None).cast(sup_info_schema)),
            )
            .withColumn(
                "sup_info",
                f.array(
                    f.col("sup_info_1"),
                    f.col("sup_info_2"),
                    f.col("sup_info_3"),
                    f.col("sup_info_4"),
                    f.col("sup_info_5"),
                    f.col("sup_info_6"),
                ),
            )
            .withColumn("sup_info", f.array_distinct(f.expr("filter(sup_info, x -> x is not null)")))
            .select(["cur_clm_uniq_id", "sup_info"])
        )
        sup_info_df = sup_info_df.select(
            f.col("cur_clm_uniq_id"), f.explode(f.col("sup_info")).alias("sup_info")
        ).select(
            f.col("cur_clm_uniq_id"),
            f.col("sup_info.code").alias("code"),
            f.col("sup_info.code_system").alias("code_system"),
        )
        sup_info_window_spec = w.partitionBy("cur_clm_uniq_id").orderBy("cur_clm_uniq_id")
        claim_sup_info_seq_df = sup_info_df.withColumn("sequence", f.row_number().over(sup_info_window_spec)).select(
            ["cur_clm_uniq_id", "sequence", "code", "code_system"]
        )
        claim_sup_info_seq_df = claim_sup_info_seq_df.groupBy("cur_clm_uniq_id").agg(
            f.collect_list(
                f.struct(
                    f.col("sequence"),
                    f.col("code"),
                    f.col("code_system"),
                )
            ).alias("supporting_info")
        )
        claim_sup_info_seq_df.persist()

        # collect claim care team
        care_team_schema = StructType(
            [
                StructField("provider_npi", StringType(), True),
                StructField("provider_role", StringType(), True),
                StructField("provider_role_system", StringType(), True),
                StructField("practitioner_internal_id", StringType(), True),
            ]
        )
        care_team_df = (
            claim_df.withColumn(
                "care_team_1",
                f.when(
                    f.col("oprtg_prvdr_npi_num").isNotNull(),
                    f.struct(
                        f.col("oprtg_prvdr_npi_num").alias("provider_npi"),
                        f.lit("operating").alias("provider_role"),
                        f.lit("http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimCareTeamRole").alias(
                            "provider_role_system"
                        ),
                        f.expr("uuid()").alias("practitioner_internal_id"),
                    ),
                ).otherwise(f.lit(None).cast(care_team_schema)),
            )
            .withColumn(
                "care_team_2",
                f.when(
                    f.col("atndg_prvdr_npi_num").isNotNull(),
                    f.struct(
                        f.col("atndg_prvdr_npi_num").alias("provider_npi"),
                        f.lit("attending").alias("provider_role"),
                        f.lit("http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimCareTeamRole").alias(
                            "provider_role_system"
                        ),
                        f.expr("uuid()").alias("practitioner_internal_id"),
                    ),
                ).otherwise(f.lit(None).cast(care_team_schema)),
            )
            .withColumn(
                "care_team_3",
                f.when(
                    f.col("othr_prvdr_npi_num").isNotNull(),
                    f.struct(
                        f.col("othr_prvdr_npi_num").alias("provider_npi"),
                        f.lit("otheroperating").alias("provider_role"),
                        f.lit("http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimCareTeamRole").alias(
                            "provider_role_system"
                        ),
                        f.expr("uuid()").alias("practitioner_internal_id"),
                    ),
                ).otherwise(f.lit(None).cast(care_team_schema)),
            )
            .withColumn(
                "care_team",
                f.array(f.col("care_team_1"), f.col("care_team_2"), f.col("care_team_3")),
            )
            .withColumn("care_team", f.array_distinct(f.expr("filter(care_team, x -> x is not null)")))
            .select(["cur_clm_uniq_id", "care_team"])
        )
        care_team_df = care_team_df.select(
            f.col("cur_clm_uniq_id"), f.explode(f.col("care_team")).alias("care_team")
        ).select(
            f.col("cur_clm_uniq_id"),
            f.col("care_team.provider_npi").alias("provider_npi"),
            f.col("care_team.provider_role").alias("provider_role"),
            f.col("care_team.provider_role_system").alias("provider_role_system"),
            f.col("care_team.practitioner_internal_id").alias("practitioner_internal_id"),
        )
        care_team_window_spec = w.partitionBy("cur_clm_uniq_id").orderBy("cur_clm_uniq_id")
        claim_care_team_seq_df = care_team_df.withColumn("sequence", f.row_number().over(care_team_window_spec)).select(
            [
                "cur_clm_uniq_id",
                "sequence",
                "provider_npi",
                "provider_role",
                "provider_role_system",
                "practitioner_internal_id",
            ]
        )

        claim_care_team_seq_df = (
            claim_care_team_seq_df.groupBy("cur_clm_uniq_id")
            .agg(
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
            .select(["cur_clm_uniq_id", "care_team"])
        )
        claim_care_team_seq_df.persist()

        # combine all claim details
        claim_df = (
            claim_df.join(claim_diag_seq_df, on="cur_clm_uniq_id", how="left")
            .join(claim_proc_df, on="cur_clm_uniq_id", how="left")
            .join(claim_line_df, on="cur_clm_uniq_id", how="left")
            .join(claim_sup_info_seq_df, on="cur_clm_uniq_id", how="left")
            .join(claim_care_team_seq_df, on="cur_clm_uniq_id", how="left")
        )
        claim_df.persist()

        claim_df = (
            claim_df.withColumn("organization_internal_id", f.expr("uuid()"))
            .withColumn("claim_internal_id", f.expr("uuid()"))
            .withColumn("patient_internal_id", f.expr("uuid()"))
            .withColumn("claim_period_start", transform_date_time(f.col("clm_from_dt")))
            .withColumn("claim_period_end", transform_date_time(f.col("clm_thru_dt")))
            .withColumn("bundle_id", f.expr("uuid()"))
        )
        # processing row wise operation
        transformer = FHIRTransformer()
        data_rdd = claim_df.rdd.map(lambda row: render_resources(row, transformer))
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
