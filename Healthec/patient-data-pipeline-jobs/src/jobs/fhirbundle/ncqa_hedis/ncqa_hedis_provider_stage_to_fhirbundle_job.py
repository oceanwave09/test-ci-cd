import json
import os
import sys

import click
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, start_spark
from utils.constants import NCQA_PROVIDER_ID_SRC_SYSTEM, NCQA_SOURCE_FILE_SYSTEM
from utils.enums import ResourceType
from utils.utils import exit_with_error, load_config, upload_bundle_files

PRAT_JINJA_TEMPLATE = "practitioner.j2"
PROV_ROLE_JINJA_TEMPLATE = "practitioner_role.j2"


def _get_specialty_code(code: str, display: str):
    return f.create_map(f.lit("code"), f.lit(code), f.lit("display"), f.lit(display), f.lit("text"), f.lit(display))


def _transform_resource(data_dict: dict, template: str, resource_type, transformer: FHIRTransformer) -> dict:
    transformer.load_template(template)
    return json.loads(transformer.render_resource(resource_type, data_dict))


def fhir_mapper_df(df: DataFrame) -> DataFrame:
    df = df.select(
        "bundle_id",
        f.create_map(f.lit("specialties"), f.col("specialties")).alias("pract_role_specialties"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("source_id"),
                f.lit("firstname"),
                f.lit("lastname"),
                f.lit("source_system"),
                f.lit("source_file_id"),
                f.lit("source_file_system"),
            ),
            f.array(
                f.col("practitioner_internal_id"),
                f.col("provider_id"),
                f.col("provider_id"),
                f.col("provider_id"),
                f.lit(NCQA_PROVIDER_ID_SRC_SYSTEM),
                f.lit("provider"),
                f.lit(NCQA_SOURCE_FILE_SYSTEM),
            ),
        ).alias("practitioner_rsc"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("source_file_id"),
                f.lit("source_file_system"),
                f.lit("practitioner_id"),
            ),
            f.array(
                f.col("practitioner_role_internal_id"),
                f.lit("provider"),
                f.lit(NCQA_SOURCE_FILE_SYSTEM),
                f.col("practitioner_internal_id"),
            ),
        ).alias("pract_role_rsc"),
    )
    return df


def render_resources(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    resources = []
    # render resources
    resources.append(
        _transform_resource(
            data_dict=row_dict.get("practitioner_rsc"),
            resource_type=ResourceType.Practitioner.value,
            template=PRAT_JINJA_TEMPLATE,
            transformer=transformer,
        )
    )
    # adding specialties
    row_dict.get("pract_role_rsc").update(row_dict.get("pract_role_specialties"))
    resources.append(
        _transform_resource(
            data_dict=row_dict.get("pract_role_rsc"),
            resource_type=ResourceType.PractitionerRole.value,
            template=PROV_ROLE_JINJA_TEMPLATE,
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
    default="ncqa_hedis_provider_stage_to_fhirbundle_job",
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
            config.get("delta_table_name", "ncqa_hedis_provider"),
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
        specialties = f.array(
            f.when(f.upper(f.col("pcp")) == "Y", _get_specialty_code("207Q00000X", "Primary Care Physician")),
            f.when(
                f.upper(f.col("obgyn")) == "Y", _get_specialty_code("207Q00000X", "Obstetrics & Gynecology Physician")
            ),
            f.when(f.upper(f.col("mh_provider")) == "Y", _get_specialty_code("103T00000X", "Psychologist")),
            f.when(f.upper(f.col("eye_care_provider")) == "Y", _get_specialty_code("152W00000X", "Optometrist")),
            f.when(f.upper(f.col("dentist")) == "Y", _get_specialty_code("122300000X", "Dentist")),
            f.when(f.upper(f.col("nephrologist")) == "Y", _get_specialty_code("207RN0300X", "Nephrology Physician")),
            f.when(
                f.upper(f.col("anesthesiologist")) == "Y", _get_specialty_code("207L00000X", "Anesthesiology Physician")
            ),
            f.when(f.upper(f.col("npr_provider")) == "Y", _get_specialty_code("363L00000X", "Nurse Practitioner")),
            f.when(f.upper(f.col("pas_provider")) == "Y", _get_specialty_code("363A00000X", "Physician Assistant")),
            f.when(
                f.upper(f.col("provider_prescribing_privileges")) == "Y",
                _get_specialty_code("208D00000X", "General Practice Physician"),
            ),
            f.when(
                f.upper(f.col("clinical_pharmacist")) == "Y", _get_specialty_code("208U00000X", "Clinical Pharmacist")
            ),
            f.when(f.upper(f.col("hospital")) == "Y", _get_specialty_code("208M00000X", "Hospitalist")),
            f.when(f.upper(f.col("snf")) == "Y", _get_specialty_code("314000000X", "Skilled Nursing Facility")),
            f.when(f.upper(f.col("surgeon")) == "Y", _get_specialty_code("208600000X", "Surgeon")),
            f.when(f.upper(f.col("registered_nurse")) == "Y", _get_specialty_code("163W00000X", "Registered Nurse")),
        )

        # apply transformations
        data_df = (
            data_df.withColumn("practitioner_internal_id", f.expr("uuid()"))
            .withColumn("practitioner_role_internal_id", f.expr("uuid()"))
            .withColumn("bundle_id", f.col("row_id"))
            .withColumn("specialties", specialties)
            .withColumn("file_tenant", f.lit(file_tenant))
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
