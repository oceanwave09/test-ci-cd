import json
import os
import sys
from datetime import datetime

import click
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, start_spark
from utils.constants import (
    NCQA_CLAIM_ID_SRC_SYSTEM,
    NCQA_MEMBER_ID_SRC_SYSTEM,
    SUPPLEMENTAL_DATA_URL,
    SUPPLEMENTAL_DATA_VALUE_BOOLEAN,
    NCQA_SOURCE_FILE_SYSTEM,
)
from utils.enums import ResourceType
from utils.transformation import transform_code_system_pharm, transform_date_time
from utils.utils import exit_with_error, load_config, upload_bundle_files

PAT_JINJA_TEMPLATE = "patient.j2"
# COV_JINJA_TEMPLATE = "coverage.j2"
PROV_JINJA_TEMPLATE = "practitioner.j2"
MED_REQ_JINJA_TEMPLATE = "medication_request.j2"
CLAIM_JINJA_TEMPLATE = "claim.j2"
CLAIM_RESP_JINJA_TEMPLATE = "claim_response.j2"


def _transform_resource(data_dict: dict, template: str, resource_type, transformer: FHIRTransformer) -> dict:
    transformer.load_template(template)
    return json.loads(transformer.render_resource(resource_type, data_dict))


def fhir_mapper_df(df: DataFrame) -> DataFrame:
    df = df.select(
        "bundle_id",
        f.create_map(f.lit("extensions"), f.col("claim_supplement")).alias("claim_extension"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("member_id"),
                f.lit("member_system"),
                f.lit("assigner_organization_id"),
            ),
            f.array(
                f.col("patient_internal_id"),
                f.col("member_id"),
                f.lit(NCQA_MEMBER_ID_SRC_SYSTEM),
                f.col("src_organization_id"),
            ),
        ).alias("patient_rsc"),
        f.map_from_arrays(
            f.array(f.lit("internal_id"), f.lit("npi")),
            f.array(f.col("practitioner_internal_id"), f.col("provider_npi")).alias("practitioner_rsc"),
        ),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("source_file_id"),
                f.lit("source_file_system"),
                f.lit("patient_id"),
                f.lit("medication_code"),
                f.lit("medication_system"),
                f.lit("authored_date_time"),
                f.lit("days_supply"),
                f.lit("quantity"),
            ),
            f.array(
                f.col("medication_request_internal_id"),
                f.lit("pharm"),
                f.lit(NCQA_SOURCE_FILE_SYSTEM),
                f.col("patient_internal_id"),
                f.col("ndc_drug_code"),
                f.col("medication_req_system"),
                f.col("service_date_time"),
                f.col("medi_req_days_supply"),
                f.col("medi_req_quantity"),
            ),
        ).alias("medi_req_rsc"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("source_file_id"),
                f.lit("source_file_system"),
                f.lit("patient_id"),
                f.lit("source_id"),
                f.lit("source_system"),
                f.lit("use"),
                f.lit("prescription_medication_request_id"),
                f.lit("provider_organization_id"),
                f.lit("type_code"),
                f.lit("type_system"),
                f.lit("type_display"),
                f.lit("type_text"),
                f.lit("created_date_time"),
                f.lit("status"),
            ),
            f.array(
                f.col("claim_internal_id"),
                f.lit("pharm"),
                f.lit(NCQA_SOURCE_FILE_SYSTEM),
                f.col("patient_internal_id"),
                f.col("claim_source_id"),
                f.lit(NCQA_CLAIM_ID_SRC_SYSTEM),
                f.lit("claim"),
                f.col("medication_request_internal_id"),
                f.col("src_organization_id"),
                f.lit("pharmacy"),
                f.lit("http://terminology.hl7.org/CodeSystem/claim-type"),
                f.lit("Pharmacy"),
                f.lit("Pharmacy"),
                f.col("claim_service_date_time"),
                f.col("phm_claim_status"),
            ),
        ).alias("claim_rsc"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("source_file_id"),
                f.lit("source_file_system"),
                f.lit("patient_id"),
                f.lit("claim_id"),
                f.lit("use"),
                f.lit("created_date_time"),
                f.lit("outcome"),
                f.lit("status"),
                f.lit("requestor_organization_id"),
            ),
            f.array(
                f.col("claim_respone_internal_id"),
                f.lit("pharm"),
                f.lit(NCQA_SOURCE_FILE_SYSTEM),
                f.col("patient_internal_id"),
                f.col("claim_internal_id"),
                f.lit("claim"),
                f.col("claim_service_date_time"),
                f.col("claim_rsp_outcome"),
                f.col("claim_rsp_status"),
                f.col("src_organization_id"),
            ),
        ).alias("claim_resp_rsc"),
    )
    return df


# def render_coverage(transformer: FHIRTransformer, metadata: dict, seq: int = 1):
#     transformer.load_template(COV_JINJA_TEMPLATE)
#     render_udf = f.udf(lambda row, resource_type: transformer.render_resource(resource_type, row.asDict()))

#     def inner(df: DataFrame):
#         cov_df = (
#             df.withColumn(
#                 "coverage_internal_id",
#                 f.concat_ws("_", f.lit("Coverage"), f.col("row_id"), f.lit(seq)),
#             )
#             .withColumn("coverage_member_id", f.col("member_id"))
#             .select("row_id", df.colRegex("`coverage_.*`"))
#         )
#         cov_df = (
#             cov_df.withColumnRenamed("coverage_internal_id", "internal_id")
#             .withColumnRenamed("coverage_member_id", "member_id")
#             .withColumn("member_system", f.lit(NCQA_MEMBER_ID_SRC_SYSTEM))
#             .withColumn("assigner_organization_id", f.lit(metadata.get("src_organization_id", "")))
#         )
#         cov_df = cov_df.withColumn(
#             "resources",
#             f.array(
#                 render_udf(
#                     f.struct([cov_df[x] for x in cov_df.columns]),
#                     f.lit(ResourceType.Coverage.value),
#                 )
#             ),
#         )
#         return cov_df.select(["row_id", "resources"])

#     return inner


def render_resources(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    resources = []
    # render resources
    resources.append(
        _transform_resource(
            data_dict=row_dict.get("patient_rsc"),
            resource_type=ResourceType.Patient.value,
            template=PAT_JINJA_TEMPLATE,
            transformer=transformer,
        )
    )

    if row_dict.get("practitioner_rsc", {}).get("npi"):
        resources.append(
            _transform_resource(
                data_dict=row_dict.get("practitioner_rsc"),
                resource_type=ResourceType.Practitioner.value,
                template=PROV_JINJA_TEMPLATE,
                transformer=transformer,
            )
        )

    resources.append(
        _transform_resource(
            data_dict=row_dict.get("medi_req_rsc"),
            resource_type=ResourceType.MedicationRequest.value,
            template=MED_REQ_JINJA_TEMPLATE,
            transformer=transformer,
        )
    )

    # adding claim extension
    row_dict.get("claim_rsc").update(row_dict.get("claim_extension"))
    resources.append(
        _transform_resource(
            data_dict=row_dict.get("claim_rsc"),
            resource_type=ResourceType.Claim.value,
            template=CLAIM_JINJA_TEMPLATE,
            transformer=transformer,
        )
    )

    resources.append(
        _transform_resource(
            data_dict=row_dict.get("claim_resp_rsc"),
            resource_type=ResourceType.ClaimResponse.value,
            template=CLAIM_RESP_JINJA_TEMPLATE,
            transformer=transformer,
        )
    )

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
    default="ncqa_hedis_pharm_stage_to_fhirbundle_job",
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
            config.get("delta_table_name", "ncqa_hedis_pharm"),
        )
        if not delta_table_name:
            exit_with_error(
                log,
                "delta table location should be provided!",
            )

        # construct delta table location
        delta_table_location = os.path.join(delta_schema_location, delta_table_name)
        log.warn(f"delta_table_location: {delta_table_location}")

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

        if file_source:
            fhirbundle_landing_path = os.path.join(landing_path, file_source)
        else:
            fhirbundle_landing_path = landing_path

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
            "src_organization_id": src_organization_id,
        }

        # add storage context in spark session
        spark = add_storage_context(spark, [delta_table_location])

        # load the records from delta table location
        log.warn("load records from delta table location")
        data_df = (
            spark.read.format("delta")
            .load(delta_table_location)
            .filter(f.col("batch_id") == file_batch_id)
            .drop(
                "batch_id",
                "source_system",
                "file_name",
                "status",
                "created_user",
                "created_ts",
                "updated_user",
                "updated_ts",
            )
            .fillna("")
        )

        if data_df.isEmpty():
            log.warn(f"Spark job {app_name} completed successfully, There is lack of adequate rows for processing.")
            sys.exit(0)

        # todo: apply decryption on personal details

        # transformation
        medi_req_day_suply_col = f.when(
            (f.col("days_supply") != "") & (f.col("days_supply").cast("float").isNotNull()), f.col("days_supply")
        ).otherwise("")

        medi_req_quantity_col = f.when(
            (f.col("quantity_dispensed") != "") & (f.col("quantity_dispensed").cast("float").isNotNull()),
            f.col("quantity_dispensed").cast("float"),
        ).otherwise("")

        claim_source_id_col = f.concat(
            f.col("member_id"),
            f.when(f.col("ndc_drug_code") != "", f.concat(f.lit("_"), f.col("ndc_drug_code"))).otherwise(f.lit("")),
            f.when(f.col("service_date") != "", f.concat(f.lit("_"), f.col("service_date"))).otherwise(f.lit("")),
        )

        claim_supplement_col = f.array(
            f.when(
                f.col("supplemental_data") == "Y",
                f.struct(
                    f.lit("value_extensions"),
                    f.array(
                        f.struct(f.lit("url"), f.lit(SUPPLEMENTAL_DATA_URL)),
                        f.struct(f.lit("value_boolean"), f.lit(SUPPLEMENTAL_DATA_VALUE_BOOLEAN)),
                    ),
                ),
            )
        )

        claim_service_date_col = f.when(
            f.col("service_date") != "", transform_date_time(f.col("service_date"))
        ).otherwise(datetime.utcnow().isoformat())

        claim_status_col = f.when(f.col("claim_status") == "1", "active").otherwise("cancelled")

        claim_rsp_outcome_col = (
            f.when(f.col("claim_status") == "1", "complete").when(f.col("claim_status") == "2", "error").otherwise("")
        )

        claim_rsp_status_col = (
            f.when(f.col("claim_status") == "1", "active").when(f.col("claim_status") == "2", "cancelled").otherwise("")
        )

        # apply transformations
        data_df = (
            data_df.withColumn("patient_internal_id", f.expr("uuid()"))
            .withColumn(
                "practitioner_internal_id",
                f.when(f.col("provider_npi") != "", f.expr("uuid()")).otherwise(f.lit("")),
            )
            .withColumn("medication_request_internal_id", f.expr("uuid()"))
            .withColumn("claim_internal_id", f.expr("uuid()"))
            .withColumn("claim_respone_internal_id", f.expr("uuid()"))
            .withColumn("bundle_id", f.col("row_id"))
            .withColumn("service_date_time", transform_date_time(f.col("service_date")))
            .withColumn("claim_service_date_time", claim_service_date_col)
            .withColumn("medi_req_days_supply", medi_req_day_suply_col)
            .withColumn("medi_req_quantity", medi_req_quantity_col)
            .withColumn("medication_req_system", transform_code_system_pharm(f.lit("N")))
            .withColumn("claim_source_id", claim_source_id_col)
            .withColumn("claim_supplement", claim_supplement_col)
            .withColumn("phm_claim_status", claim_status_col)
            .withColumn("claim_rsp_outcome", claim_rsp_outcome_col)
            .withColumn("claim_rsp_status", claim_rsp_status_col)
            .withColumn("file_tenant", f.lit(file_tenant))
            .withColumn("file_source", f.lit(file_source))
            .withColumn("resource_type", f.lit(resource_type))
            .withColumn("file_batch_id", f.lit(file_batch_id))
            .withColumn("src_file_name", f.lit(src_file_name))
            .withColumn("src_organization_id", f.lit(src_organization_id))
            .fillna("")
        )

        # fhir mapper
        data_df = fhir_mapper_df(data_df)

        # processing row wise operation
        transformer = FHIRTransformer()
        data_rdd = data_df.rdd.map(lambda row: render_resources(row, transformer))
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
            f"file with batch id {file_batch_id} transformed "
            f"into fhir bundle and copied to {fhirbundle_landing_path}."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
