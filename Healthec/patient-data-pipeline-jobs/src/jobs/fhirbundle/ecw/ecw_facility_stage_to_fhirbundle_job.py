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
from utils.enums import ResourceType
from utils.utils import exit_with_error, load_config, upload_bundle_files

LOC_JINJA_TEMPLATE = "location.j2"


def _get_organization_match_attributes(value: str) -> str:
    attributes_template = Template(
        """
        {
            "identifier": [
                {
                    "type": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                                "code": "RI",
                                "display": "Resource Identifier"
                            }
                        ]
                    },
                    "system": "http://healthec.com/identifier/external_id",
                    "value": "$external_id"
                }
            ]
        }
        """
    )
    return attributes_template.substitute(external_id=value)


def _transform_practice(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)
    # prepare matching attribute
    row_dict["attributes"] = _get_organization_match_attributes(row_dict.get("apuid")) if row_dict.get("apuid") else ""
    return Row(**row_dict)


def _transform_resource(data_dict: dict, template: str, resource_type, transformer: FHIRTransformer) -> dict:
    transformer.load_template(template)
    return json.loads(transformer.render_resource(resource_type, data_dict))


def fhir_mapper_df(df: DataFrame) -> DataFrame:
    df = df.select(
        "bundle_id",
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("assigner_organization_id"),
                f.lit("organization_id"),
                f.lit("source_id"),
                f.lit("source_file_name"),
                f.lit("name"),
                f.lit("street_address_1"),
                f.lit("street_address_2"),
                f.lit("city"),
                f.lit("state"),
                f.lit("zip"),
            ),
            f.array(
                f.col("location_internal_id"),
                f.col("organization_id"),
                f.col("organization_id"),
                f.col("facility_id"),
                f.col("file_format"),
                f.col("facility_name"),
                f.col("address_line_1"),
                f.col("address_line_2"),
                f.col("address_city"),
                f.col("address_state"),
                f.col("address_zip"),
            ),
        ).alias("location_rsc"),
    )
    return df


def render_resources(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    resources = []
    # render resources
    resources.append(
        _transform_resource(
            data_dict=row_dict.get("location_rsc"),
            resource_type=ResourceType.Location.value,
            template=LOC_JINJA_TEMPLATE,
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
    default="ecw_facility_stage_to_fhirbundle_job",
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
            config.get("delta_table_name", "ecw_facility"),
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

        # validate `apuid`
        val_pract_df = data_df.filter(f.col("apuid") == "")
        if not val_pract_df.isEmpty():
            exit_with_error(log, "apuid must be a not-null value.")

        # register provider service udf
        prov_service_udf = f.udf(lambda attr: match_core_entity(attr, Organization))

        # filter valid apuid by tenant
        pract_df = data_df.select("apuid").drop_duplicates()
        pract_rdd = pract_df.rdd.map(lambda row: _transform_practice(row))
        pract_df = spark.createDataFrame(pract_rdd)

        # match organization by external id
        match_pract_df = (
            pract_df.withColumn("organization_id", prov_service_udf(pract_df["attributes"]))
            .filter(f.col("organization_id").isNotNull())
            .select(["apuid", "organization_id"])
        )
        match_pract_df.persist()

        # select rows having organization match
        data_df = data_df.join(match_pract_df, on="apuid", how="inner")

        if data_df.isEmpty():
            log.warn(f"Spark job {app_name} completed successfully, There is lack of adequate rows for processing.")
            sys.exit(0)

        # apply transformations
        data_df = (
            data_df.withColumn("location_internal_id", f.expr("uuid()"))
            .withColumn("bundle_id", f.col("row_id"))
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
            f"from file with batch id {file_batch_id} transformed "
            f"into fhir bundle and copied to {fhirbundle_landing_path}."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
