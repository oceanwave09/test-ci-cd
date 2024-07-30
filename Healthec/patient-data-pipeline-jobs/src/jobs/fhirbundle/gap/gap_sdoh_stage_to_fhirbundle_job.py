import json
import os
import sys
from string import Template

import click
from fhirclient.resources.organization import Organization
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, start_spark
from utils.api_client import match_core_entity
from utils.constants import PATIENT_MRN_SYSTEM
from utils.enums import ResourceType
from utils.transformation import transform_date
from utils.utils import exit_with_error, load_config, upload_bundle_files

PAT_JINJA_TEMPLATE = "patient.j2"
PRC_JINJA_TEMPLATE = "practitioner.j2"
QRESP_JINJA_TEMPLATE = "questionnaire_response.j2"


def _get_organization_match_attributes(value: str) -> str:
    attributes_template = Template("""{"name": "$name"}""")
    return attributes_template.substitute(name=value)


def _transform_practice(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)
    # prepare matching attribute
    row_dict["attributes"] = (
        _get_organization_match_attributes(row_dict.get("source_name")) if row_dict.get("source_name") else ""
    )
    return Row(**row_dict)


def _transform_resource(data_dict: dict, template: str, resource_type, transformer: FHIRTransformer) -> dict:
    transformer.load_template(template)
    return json.loads(transformer.render_resource(resource_type, data_dict))


def fhir_mapper_df(df: DataFrame) -> DataFrame:
    df = df.select(
        "bundle_id",
        f.create_map(f.lit("response_items"), f.col("response_items")).alias("response_item_col"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("mihin_id"),
                f.lit("mrn"),
                f.lit("mrn_system"),
                f.lit("organization_id"),
                f.lit("assigner_organization_id"),
                f.lit("firstname"),
                f.lit("lastname"),
                f.lit("dob"),
            ),
            f.array(
                f.col("patient_internal_id"),
                f.col("mihin_common_key"),
                f.col("external_patient_id"),
                f.lit(PATIENT_MRN_SYSTEM),
                f.col("organization_id"),
                f.col("organization_id"),
                f.col("patient_first_name"),
                f.col("patient_last_name"),
                f.col("patient_date_of_birth"),
            ),
        ).alias("patient_rsc"),
        f.map_from_arrays(
            f.array(f.lit("internal_id"), f.lit("npi"), f.lit("firstname"), f.lit("lastname"), f.lit("middleinitials")),
            f.array(
                f.col("practitioner_internal_id"),
                f.col("provider_npi"),
                f.col("practitioner_first_name"),
                f.col("practitioner_last_name"),
                f.col("practitioner_middle_name"),
            ),
        ).alias("practitioner_rsc"),
        f.map_from_arrays(
            f.array(f.lit("internal_id"), f.lit("patient_id"), f.lit("practitioner_id")),
            f.array(
                f.col("questionnarie_response_internal_id"),
                f.col("patient_internal_id"),
                f.col("practitioner_internal_id"),
            ),
        ).alias("ques_resp_rsc"),
    )
    return df


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
                template=PRC_JINJA_TEMPLATE,
                transformer=transformer,
            )
        )

    # adding respone items
    row_dict.get("ques_resp_rsc").update(row_dict.get("response_item_col"))
    resources.append(
        _transform_resource(
            data_dict=row_dict.get("ques_resp_rsc"),
            resource_type=ResourceType.QuestionnaireResponse.value,
            template=QRESP_JINJA_TEMPLATE,
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
    default="gap_sdoh_stage_to_fhirbundle_job",
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
        "spark.sql.legacy.timeParserPolicy": "LEGACY",
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
            config.get("delta_table_name", "gap_sdoh"),
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

        file_format = os.environ.get("FILE_FORMAT", config.get("file_format", ""))
        log.warn(f"file_format: {file_format}")

        # Change FHIRBUNDLE to FHIRBUNDLE_BULK
        fhirbundle_landing_path = fhirbundle_landing_path.replace("FHIRBUNDLE", "FHIRBUNDLE_BULK")

        # Construct fhir bundle temp path
        fhir_temp_path = fhirbundle_landing_path.replace("landing", "temporary")
        fhir_bundle_temp_path = os.path.join(fhir_temp_path, file_batch_id)

        # # construct metadata
        metadata = {
            "file_tenant": file_tenant,
            "file_format": file_format,
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

        if data_df.isEmpty():
            log.warn(f"Spark job {app_name} completed successfully, There is lack of adequate rows for processing.")
            sys.exit(0)

        # apply decryption on personal details
        data_df = (
            data_df.withColumn(
                "patient_first_name",
                f.expr(f"aes_decrypt(unhex(patient_first_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "patient_last_name",
                f.expr(f"aes_decrypt(unhex(patient_last_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "patient_date_of_birth",
                f.expr(f"aes_decrypt(unhex(patient_date_of_birth) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "provider_name",
                f.expr(f"aes_decrypt(unhex(provider_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "provider_npi",
                f.expr(f"aes_decrypt(unhex(provider_npi) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
        )

        # validate `source_name`
        val_pract_df = data_df.filter(f.col("source_name") == "")
        if not val_pract_df.isEmpty():
            exit_with_error(log, "source_name must be a not-null value.")
        # validate `external_patient_id`
        val_pat_df = data_df.filter(f.col("external_patient_id") == "")
        if not val_pat_df.isEmpty():
            exit_with_error(log, "external patient id must be a not-null value.")

        # register provider service udf
        prov_service_udf = f.udf(lambda attr: match_core_entity(attr, Organization))

        # filter valid source_name by tenant
        pract_df = data_df.select("source_name").drop_duplicates()
        pract_rdd = pract_df.rdd.map(lambda row: _transform_practice(row))
        pract_df = spark.createDataFrame(pract_rdd)

        # match organization by source_name
        match_pract_df = (
            pract_df.withColumn("organization_id", prov_service_udf(pract_df["attributes"]))
            .filter(f.col("organization_id").isNotNull())
            .select(["source_name", "organization_id"])
        )
        match_pract_df.persist()

        # select rows having organization match
        data_df = data_df.join(match_pract_df, on="source_name", how="inner")

        if data_df.isEmpty():
            log.warn(f"Spark job {app_name} completed successfully, There is lack of adequate rows for processing.")
            sys.exit(0)

        # transformations
        pract_name_parts = f.split(f.col("provider_name"), ",")

        practitioner_last_name = f.when(f.size(pract_name_parts) >= 1, f.trim(f.col("pract_name_parts")[0])).otherwise(
            ""
        )

        practitioner_first_name = f.when(
            f.size(pract_name_parts) == 2, f.split(f.trim(f.col("pract_name_parts")[1]), " ")[0]
        ).otherwise("")

        practitioner_middle_name = f.when(
            f.size(f.split(f.trim(f.col("pract_name_parts")[1]), " ")) == 2,
            f.split(f.trim(f.col("pract_name_parts")[1]), " ")[1],
        ).otherwise("")

        response_items = f.array(
            f.struct(
                f.col("question_group").alias("item_name"),
                f.array(f.struct(f.col("answer").alias("answers"), f.col("question").alias("question"))).alias(
                    "questions"
                ),
            )
        )

        # apply transformations
        data_df = (
            data_df.withColumn("patient_internal_id", f.expr("uuid()"))
            .withColumn("practitioner_internal_id", f.expr("uuid()"))
            .withColumn("questionnarie_response_internal_id", f.expr("uuid()"))
            .withColumn("bundle_id", f.col("row_id"))
            .withColumn("patient_date_of_birth", transform_date(f.col("patient_date_of_birth")))
            .withColumn("pract_name_parts", pract_name_parts)
            .withColumn("practitioner_last_name", practitioner_last_name)
            .withColumn("practitioner_first_name", practitioner_first_name)
            .withColumn("practitioner_middle_name", practitioner_middle_name)
            .withColumn("response_items", response_items)
            .withColumn("file_tenant", f.lit(file_tenant))
            .withColumn("file_format", f.lit(file_format))
            .withColumn("file_source", f.lit(file_source))
            .withColumn("resource_type", f.lit(resource_type))
            .withColumn("file_batch_id", f.lit(file_batch_id))
            .withColumn("src_file_name", f.lit(src_file_name))
            .withColumn("src_organization_id", f.lit(src_organization_id))
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
