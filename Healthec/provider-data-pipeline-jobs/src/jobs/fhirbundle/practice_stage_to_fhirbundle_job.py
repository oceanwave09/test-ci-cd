import json
import os
import sys

import click
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, start_spark
from utils.constants import ACTIVE_STATUS, TYPE_PAY_CODE, TYPE_PAY_DISPLAY, TYPE_PROV_CODE, TYPE_PROV_DISPLAY
from utils.enums import ResourceType
from utils.transformation import transform_date_time
from utils.utils import exit_with_error, load_config, upload_bundle_files

ORG_JINJA_TEMPLATE = "organization.j2"
LOC_JINJA_TEMPLATE = "location.j2"
ORG_AFF_JINJA_TEMPLATE = "organization_affiliation.j2"

ORG_AFF_ROLE_CODE = "provider"
ORG_AFF_ROLE_DISPLAY = "Provider"


def _transform_resource(data_dict: dict, template: str, resource_type, transformer: FHIRTransformer) -> dict:
    transformer.load_template(template)
    return json.loads(transformer.render_resource(resource_type, data_dict))


def fhir_mapper_df(df: DataFrame) -> DataFrame:
    df = df.select(
        "bundle_id",
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("name"),
                f.lit("alias"),
                f.lit("ssn"),
                f.lit("tax_id"),
                f.lit("group_npi"),
                f.lit("source_id"),
                f.lit("nabp_id"),
                f.lit("active"),
                f.lit("type_code"),
                f.lit("type_display"),
                f.lit("street_address_1"),
                f.lit("street_address_2"),
                f.lit("city"),
                f.lit("county_code"),
                f.lit("state"),
                f.lit("zip"),
                f.lit("country"),
                f.lit("phone_work"),
                f.lit("email"),
                f.lit("fax"),
                f.lit("contact_prefix"),
                f.lit("contact_firstname"),
                f.lit("contact_lastname"),
                f.lit("contact_middlename"),
                f.lit("contact_phone"),
                f.lit("contact_email"),
            ),
            f.array(
                f.col("organization_internal_id"),
                f.col("provider_org_name"),
                f.col("provider_org_dba_name"),
                f.col("provider_org_ssn"),
                f.col("provider_org_tin"),
                f.col("provider_org_group_npi"),
                f.col("provider_org_internal_id"),
                f.col("provider_org_nabp_id"),
                f.lit(ACTIVE_STATUS),
                f.lit(TYPE_PROV_CODE),
                f.lit(TYPE_PROV_DISPLAY),
                f.col("provider_org_address_line_1"),
                f.col("provider_org_address_line_2"),
                f.col("provider_org_city"),
                f.col("provider_org_county"),
                f.col("provider_org_state"),
                f.col("provider_org_zip"),
                f.col("provider_org_country"),
                f.col("provider_org_phone_work"),
                f.col("provider_org_email"),
                f.col("provider_org_fax"),
                f.col("provider_org_contact_title"),
                f.col("provider_org_contact_first_name"),
                f.col("provider_org_contact_last_name"),
                f.col("provider_org_contact_middle_name"),
                f.col("provider_org_contact_phone"),
                f.col("provider_org_contact_email"),
            ),
        ).alias("practice_rsc"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("source_id"),
                f.lit("name"),
                f.lit("active"),
                f.lit("type_code"),
                f.lit("type_display"),
            ),
            f.array(
                f.col("payer_internal_id"),
                f.col("insurance_company_id"),
                f.col("insurance_company_name"),
                f.lit(ACTIVE_STATUS),
                f.lit(TYPE_PAY_CODE),
                f.lit(TYPE_PAY_DISPLAY),
            ),
        ).alias("payer_rsc"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("organization_id"),
                f.lit("source_id"),
                f.lit("participating_organization_id"),
                f.lit("active"),
                f.lit("period_start_date"),
                f.lit("period_end_date"),
                f.lit("specialty_code"),
                f.lit("role_code"),
                f.lit("role_display"),
            ),
            f.array(
                f.col("org_affiliation_internal_id"),
                f.col("payer_internal_id"),
                f.col("org_affiliation_source_id"),
                f.col("organization_internal_id"),
                f.lit(ACTIVE_STATUS),
                f.col("provider_org_effective_date"),
                f.col("provider_org_termination_date"),
                f.col("provider_org_taxonomy_code"),
                f.lit(ORG_AFF_ROLE_CODE),
                f.lit(ORG_AFF_ROLE_DISPLAY),
            ),
        ).alias("org_affiliation_rsc"),
    )
    return df


def render_resources(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    resources = []
    # render resources
    resources.append(
        _transform_resource(
            data_dict=row_dict.get("practice_rsc"),
            resource_type=ResourceType.Organization.value,
            template=ORG_JINJA_TEMPLATE,
            transformer=transformer,
        )
    ),
    resources.append(
        _transform_resource(
            data_dict=row_dict.get("payer_rsc"),
            resource_type=ResourceType.Organization.value,
            template=ORG_JINJA_TEMPLATE,
            transformer=transformer,
        )
    ),
    resources.append(
        _transform_resource(
            data_dict=row_dict.get("org_affiliation_rsc"),
            resource_type=ResourceType.OrganizationAffiliation.value,
            template=ORG_AFF_JINJA_TEMPLATE,
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
    default="practice_stage_to_fhirbundle_job",
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

        # validate `provider_org_tin`
        pract_df = data_df.filter(f.col("provider_org_tin") == "")
        if not pract_df.isEmpty():
            exit_with_error(log, "provider organization tin must be a not-null value.")
        # validate `insurance_company_id`
        pay_df = data_df.filter((f.col("insurance_company_id") == ""))
        if not pay_df.isEmpty():
            exit_with_error(log, "insurance company id must be a not-null value.")

        data_df = (
            data_df.withColumn(
                "provider_org_ssn",
                f.expr(f"aes_decrypt(unhex(provider_org_ssn) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "provider_org_tin",
                f.expr(f"aes_decrypt(unhex(provider_org_tin) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "provider_org_group_npi",
                f.expr(f"aes_decrypt(unhex(provider_org_group_npi) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "provider_org_nabp_id",
                f.expr(f"aes_decrypt(unhex(provider_org_nabp_id) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "provider_org_address_line_1",
                f.expr(f"aes_decrypt(unhex(provider_org_address_line_1) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "provider_org_address_line_2",
                f.expr(f"aes_decrypt(unhex(provider_org_address_line_2) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "provider_org_city",
                f.expr(f"aes_decrypt(unhex(provider_org_city) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "provider_org_county",
                f.expr(f"aes_decrypt(unhex(provider_org_county) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "provider_org_state",
                f.expr(f"aes_decrypt(unhex(provider_org_state) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "provider_org_phone_work",
                f.expr(f"aes_decrypt(unhex(provider_org_phone_work) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "provider_org_email",
                f.expr(f"aes_decrypt(unhex(provider_org_email) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "provider_org_fax",
                f.expr(f"aes_decrypt(unhex(provider_org_fax) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "provider_org_contact_first_name",
                f.expr(
                    f"aes_decrypt(unhex(provider_org_contact_first_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')"
                ).cast("string"),
            )
            .withColumn(
                "provider_org_contact_last_name",
                f.expr(f"aes_decrypt(unhex(provider_org_contact_last_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "provider_org_contact_middle_name",
                f.expr(
                    f"aes_decrypt(unhex(provider_org_contact_middle_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')"
                ).cast("string"),
            )
            .withColumn(
                "provider_org_contact_phone",
                f.expr(f"aes_decrypt(unhex(provider_org_contact_phone) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "provider_org_contact_email",
                f.expr(f"aes_decrypt(unhex(provider_org_contact_email) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
        )

        org_affiliation_source_id = f.concat_ws("_", f.col("insurance_company_id"), f.col("provider_org_tin"))

        data_df = (
            data_df.withColumn("organization_internal_id", f.expr("uuid()"))
            .withColumn("payer_internal_id", f.expr("uuid()"))
            .withColumn("org_affiliation_internal_id", f.expr("uuid()"))
            .withColumn("bundle_id", f.expr("uuid()"))
            .withColumn("provider_org_effective_date", transform_date_time(f.col("provider_org_effective_date")))
            .withColumn("provider_org_termination_date", transform_date_time(f.col("provider_org_termination_date")))
            .withColumn("org_affiliation_source_id", org_affiliation_source_id)
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
            f"practices from file with batch id {file_batch_id} transformed "
            f"into fhir bundle and copied to {fhirbundle_landing_path}."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
