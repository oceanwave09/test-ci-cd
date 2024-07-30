import json
import os

import click
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import Row
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType
from pyspark.sql.window import Window as w

from dependencies.spark import add_storage_context, start_spark
from utils.enums import ResourceType
from utils.transformation import to_int, transform_date_time, transform_to_float
from utils.utils import exit_with_error, load_config, upload_bundle_files

PAT_JINJA_TEMPLATE = "patient.j2"
ORG_JINJA_TEMPLATE = "organization.j2"
PRC_JINJA_TEMPLATE = "practitioner.j2"
CLM_JINJA_TEMPLATE = "claim.j2"


def _transform_patient(row_dict: dict, transformer: FHIRTransformer) -> Row:
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


def _transform_provider_organization(row_dict: dict, transformer: FHIRTransformer) -> Row:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("provider_organization_id", "")
    data_dict["tax_id"] = row_dict.get("clm_srvc_prvdr_gnrc_id_num", "")
    return _transform_resource(
        data_dict=data_dict,
        template=ORG_JINJA_TEMPLATE,
        resource_type=ResourceType.Organization.value,
        transformer=transformer,
    )


def _transform_provider_practitioner(row_dict: dict, transformer: FHIRTransformer) -> Row:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("provider_practitioner_id", "")
    if row_dict.get("prvdr_srvc_id_qlfyr_cd", "") == "01":
        data_dict["npi"] = row_dict.get("clm_srvc_prvdr_gnrc_id_num", "")
    elif row_dict.get("prvdr_srvc_id_qlfyr_cd", "") == "06":
        data_dict["upin"] = row_dict.get("clm_srvc_prvdr_gnrc_id_num", "")
    elif row_dict.get("prvdr_srvc_id_qlfyr_cd", "") == "08":
        data_dict["state_license"] = row_dict.get("clm_srvc_prvdr_gnrc_id_num", "")
    else:
        data_dict["source_id"] = row_dict.get("clm_srvc_prvdr_gnrc_id_num", "")
    return _transform_resource(
        data_dict=data_dict,
        template=PRC_JINJA_TEMPLATE,
        resource_type=ResourceType.Practitioner.value,
        transformer=transformer,
    )


def _transform_practitioner(row_dict: dict, transformer: FHIRTransformer) -> Row:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("practitioner_internal_id", "")
    if row_dict.get("provider_id_type", "") == "01":
        data_dict["npi"] = row_dict.get("provider_npi", "")
    elif row_dict.get("practitioner_id_type", "") == "06":
        data_dict["upin"] = row_dict.get("provider_npi", "")
    elif row_dict.get("practitioner_id_type", "") == "08":
        data_dict["state_license"] = row_dict.get("provider_npi", "")
    else:
        data_dict["source_id"] = row_dict.get("provider_npi", "")
    return _transform_resource(
        data_dict=data_dict,
        template=PRC_JINJA_TEMPLATE,
        resource_type=ResourceType.Practitioner.value,
        transformer=transformer,
    )


def _transform_claim(row_dict: dict, transformer: FHIRTransformer) -> Row:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("claim_internal_id", "")
    data_dict["patient_id"] = row_dict.get("patient_internal_id", "")
    data_dict["provider_organization_id"] = row_dict.get("provider_organization_id", "")
    data_dict["provider_practitioner_id"] = row_dict.get("provider_practitioner_id", "")
    data_dict["source_id"] = row_dict.get("cur_clm_uniq_id", "")
    data_dict["use"] = "claim"
    # For Testing
    # data_dict["status"] = row_dict.get("clm_adjsmt_type_cd", "")
    data_dict["status"] = "active"
    data_dict["type_code"] = "pharmacy"
    data_dict["type_system"] = "http://terminology.hl7.org/CodeSystem/claim-type"
    data_dict["type_display"] = "Pharmacy"
    data_dict["type_text"] = "Pharmacy"
    # data_dict["type_code"] = row_dict.get("claim_type_code", "")
    # data_dict["type_system"] = "https://bluebutton.cms.gov/resources/variables/nch_clm_type_cd"
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
    if row_dict.get("sup_info"):
        support_info = []
        for support_info_entry in row_dict.get("sup_info", []):
            support_info.append(
                {
                    "sequence": to_int(str(support_info_entry.get("sequence"))),
                    "code": support_info_entry.get("code", ""),
                    "code_system": support_info_entry.get("code_system", ""),
                    "value_string": support_info_entry.get("value_string", ""),
                    "quantity_value": support_info_entry.get("value_quantity", ""),
                }
            )
        data_dict["support_info"] = support_info
    service_lines = []
    service_line = {"sequence": 1}
    care_team_sequences = row_dict.get("careteam_sequence", [])
    if care_team_sequences:
        service_line["careteam_sequence"] = ",".join(map(str, care_team_sequences))
    service_lines.append(service_line)
    if row_dict.get("clm_line_ndc_cd", ""):
        product_or_service = []
        product_or_service.append(
            {
                "code": row_dict.get("clm_line_ndc_cd", ""),
                "system": "https://bluebutton.cms.gov/resources/codesystem/ndccode",
            }
        )
        service_line["product_or_service"] = product_or_service
    if row_dict.get("service_date_time", ""):
        service_line["service_date_time"] = row_dict.get("service_date_time")
    if row_dict.get("clm_line_srvc_unit_qty", ""):
        service_line["quantity_value"] = row_dict.get("clm_line_srvc_unit_qty")
    if row_dict.get("clm_line_bene_pmt_amt", ""):
        service_line["patient_paid_amount"] = row_dict.get("clm_line_bene_pmt_amt")
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
    if row_dict.get("provider_organization_id"):
        resources.append(_transform_provider_organization(row_dict, transformer))
    for entry in row_dict.get("care_team", []):
        if entry.get("provider_npi"):
            resources.append(_transform_practitioner(entry, transformer))
    if row_dict.get("provider_practitioner_id"):
        resources.append(_transform_provider_practitioner(row_dict, transformer))
    resources.append(_transform_patient(row_dict, transformer))
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
    default="cclf_rxclaim_stage_to_fhirbundle_job",
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
        cclf7_delta_table_location = os.path.join(delta_schema_location, "cclf7")
        log.warn(f"cclf7_delta_table_location: {cclf7_delta_table_location}")

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
            "file_type": "rx_claim",
            "src_organization_id": src_organization_id,
        }

        # add storage context in spark session
        spark = add_storage_context(spark, [cclf7_delta_table_location])

        # schema definition
        care_team_schema = ArrayType(
            StructType(
                [
                    StructField("sequence", IntegerType(), True),
                    StructField("provider_npi", StringType(), True),
                    StructField("provider_id_type", StringType(), True),
                    StructField("provider_role", StringType(), True),
                    StructField("provider_role_system", StringType(), True),
                    StructField("practitioner_internal_id", StringType(), True),
                ]
            )
        )
        sup_info_schema = StructType(
            [
                StructField("code", StringType(), True),
                StructField("code_system", StringType(), True),
                StructField("value_string", StringType(), True),
                StructField("value_quantity", StringType(), True),
            ]
        )

        # load the records from delta table location
        log.warn("load records from cclf7 delta table location")
        cclf7_data_df = (
            spark.read.format("delta")
            .load(cclf7_delta_table_location)
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
        cclf7_data_df = (
            cclf7_data_df.withColumn(
                "bene_mbi_id",
                f.expr(f"aes_decrypt(unhex(bene_mbi_id) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "clm_srvc_prvdr_gnrc_id_num",
                f.expr(f"aes_decrypt(unhex(clm_srvc_prvdr_gnrc_id_num) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "prvdr_prsbng_id_qlfyr_cd",
                f.expr(f"aes_decrypt(unhex(prvdr_prsbng_id_qlfyr_cd) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "clm_prsbng_prvdr_gnrc_id_num",
                f.expr(f"aes_decrypt(unhex(clm_prsbng_prvdr_gnrc_id_num) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
        )
        cclf7_data_df.persist()

        cclf7_data_df = cclf7_data_df.withColumn(
            "clm_line_days_suply_qty", transform_to_float(f.col("clm_line_days_suply_qty"))
        ).withColumn("clm_line_rx_fill_num", transform_to_float(f.col("clm_line_rx_fill_num")))

        claim_df = (
            cclf7_data_df.withColumn(
                "provider_organization_id",
                f.when(
                    f.col("prvdr_srvc_id_qlfyr_cd").isin(["11", "07"]),
                    f.expr("uuid()"),
                ).otherwise(f.lit(None)),
            )
            .withColumn(
                "provider_practitioner_id",
                f.when(
                    f.col("prvdr_srvc_id_qlfyr_cd").isin(["01", "06", "08", "99"]),
                    f.expr("uuid()"),
                ).otherwise(f.lit(None)),
            )
            .withColumn(
                "careteam_sequence",
                f.when(
                    f.col("clm_prsbng_prvdr_gnrc_id_num") != "",
                    f.array(f.lit(1).cast(IntegerType())),
                ).otherwise(f.lit(None).cast(ArrayType(IntegerType()))),
            )
            .withColumn(
                "care_team",
                f.when(
                    (f.col("clm_prsbng_prvdr_gnrc_id_num") != "")
                    & (f.col("prvdr_prsbng_id_qlfyr_cd").isin(["01", "06", "08", "99"])),
                    f.array(
                        f.struct(
                            f.lit(1).alias("sequence"),
                            f.col("clm_prsbng_prvdr_gnrc_id_num").alias("provider_npi"),
                            f.col("prvdr_prsbng_id_qlfyr_cd").alias("provider_id_type"),
                            f.lit("prescribing").alias("provider_role"),
                            f.lit("http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimCareTeamRole").alias(
                                "provider_role_system"
                            ),
                            f.expr("uuid()").alias("practitioner_internal_id"),
                        )
                    ),
                ).otherwise(f.array().cast(care_team_schema)),
            )
        )
        claim_df.persist()

        # consolidate claim supporting info
        sup_info_df = (
            claim_df.withColumn(
                "sup_info_1",
                f.when(
                    f.col("clm_daw_prod_slctn_cd") != "",
                    f.struct(
                        f.lit("dawcode").alias("code"),
                        f.lit("http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBSupportingInfoType").alias("code_system"),
                        f.col("clm_daw_prod_slctn_cd").alias("value_string"),
                        f.lit(None).alias("value_quantity"),
                    ),
                ).otherwise(f.lit(None).cast(sup_info_schema)),
            )
            .withColumn(
                "sup_info_2",
                f.when(
                    f.col("clm_line_days_suply_qty") != "",
                    f.struct(
                        f.lit("dayssupply").alias("code"),
                        f.lit("http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBSupportingInfoType").alias("code_system"),
                        f.lit(None).alias("value_string"),
                        f.col("clm_line_days_suply_qty").alias("value_quantity"),
                    ),
                ).otherwise(f.lit(None).cast(sup_info_schema)),
            )
            .withColumn(
                "sup_info_3",
                f.when(
                    f.col("clm_line_rx_fill_num") != "",
                    f.struct(
                        f.lit("refillnum").alias("code"),
                        f.lit("http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBSupportingInfoType").alias("code_system"),
                        f.lit(None).alias("value_string"),
                        f.col("clm_line_rx_fill_num").alias("value_quantity"),
                    ),
                ).otherwise(f.lit(None).cast(sup_info_schema)),
            )
            .withColumn(
                "sup_info",
                f.array(
                    f.col("sup_info_1"),
                    f.col("sup_info_2"),
                    f.col("sup_info_3"),
                ),
            )
            .withColumn("sup_info", f.array_distinct(f.expr("filter(sup_info, x -> x is not null)")))
            .select(["cur_clm_uniq_id", "bene_mbi_id", "sup_info"])
        )
        sup_info_df = sup_info_df.select(
            f.col("cur_clm_uniq_id"),
            f.col("bene_mbi_id"),
            f.explode(f.col("sup_info")).alias("sup_info"),
        ).select(
            f.col("cur_clm_uniq_id"),
            f.col("bene_mbi_id"),
            f.col("sup_info.code").alias("code"),
            f.col("sup_info.code_system").alias("code_system"),
            f.col("sup_info.value_string").alias("value_string"),
            f.col("sup_info.value_quantity").alias("value_quantity"),
        )
        sup_info_window_spec = w.partitionBy(["cur_clm_uniq_id", "bene_mbi_id"]).orderBy(
            ["cur_clm_uniq_id", "bene_mbi_id"]
        )
        claim_sup_info_seq_df = sup_info_df.withColumn("sequence", f.row_number().over(sup_info_window_spec)).select(
            ["cur_clm_uniq_id", "bene_mbi_id", "sequence", "code", "code_system", "value_string", "value_quantity"]
        )
        claim_sup_info_seq_df = claim_sup_info_seq_df.groupBy(["cur_clm_uniq_id", "bene_mbi_id"]).agg(
            f.collect_list(
                f.struct(
                    f.col("sequence"),
                    f.col("code"),
                    f.col("code_system"),
                    f.col("value_string"),
                    f.col("value_quantity"),
                )
            ).alias("sup_info")
        )
        claim_sup_info_seq_df.persist()

        # prepare final claim dataframe
        claim_final_df = claim_df.join(claim_sup_info_seq_df, on=["cur_clm_uniq_id", "bene_mbi_id"], how="left")
        claim_final_df.persist()

        claim_final_df = (
            claim_final_df.withColumn("patient_internal_id", f.expr("uuid()"))
            .withColumn("claim_internal_id", f.expr("uuid()"))
            .withColumn("service_date_time", transform_date_time(f.col("clm_line_from_dt")))
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

        # transform into fhir bundle and write into target location
        # fhir_df = claim_final_df.transform(to_fhir(spark, fhirbundle_landing_path, json.dumps(metadata)))

        # fhir_df.head(1)

        log.warn(
            f"spark job {app_name} completed successfully, "
            f"from file with batch id {file_batch_id} transformed "
            f"into fhir bundle and copied to {fhirbundle_landing_path}."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
