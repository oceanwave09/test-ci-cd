import json
import os
import sys

import click
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, start_spark
from utils.enums import ResourceType
from utils.transformation import transform_date, transform_date_time, transform_gender, transform_race
from utils.utils import exit_with_error, load_config, upload_bundle_files

PAT_JINJA_TEMPLATE = "patient.j2"
COV_JINJA_TEMPLATE = "coverage.j2"


def _transform_resource(data_dict: dict, template: str, resource_type, transformer: FHIRTransformer) -> dict:
    transformer.load_template(template)
    return json.loads(transformer.render_resource(resource_type, data_dict))


def fhir_mapper_df(df: DataFrame) -> DataFrame:
    df = df.select(
        "bundle_id",
        f.create_map(
            f.lit("extensions"),
            f.array(
                f.col("mdcr_status_code"),
                f.col("dual_status_code"),
                f.col("original_reason_code"),
                f.col("buyin_indicator"),
            ),
        ).alias("cov_extention"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("mbi"),
                f.lit("mbi_system"),
                f.lit("firstname"),
                f.lit("middleinitials"),
                f.lit("lastname"),
                f.lit("dob"),
                f.lit("gender"),
                f.lit("street_address_1"),
                f.lit("street_address_2"),
                f.lit("city"),
                f.lit("state"),
                f.lit("district_code"),
                f.lit("zip"),
                f.lit("race_code"),
                f.lit("deceased_date_time"),
                f.lit("active"),
            ),
            f.array(
                f.col("patient_internal_id"),
                f.col("bene_mbi_id"),
                f.lit("http://hl7.org/fhir/sid/us-mbi"),
                f.col("bene_1st_name"),
                f.col("bene_midl_name"),
                f.col("bene_last_name"),
                f.col("patient_dob"),
                f.col("patient_gender"),
                f.col("bene_line_1_adr"),
                f.col("bene_line_2_adr"),
                f.col("geo_zip_plc_name"),
                f.col("bene_fips_state_cd"),
                f.col("bene_fips_cnty_cd"),
                f.col("bene_zip_cd"),
                f.col("patient_race_code"),
                f.col("deceased_date_time"),
                f.lit("true"),
            ),
        ).alias("patient_rsc"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("medicare_number"),
                f.lit("medicare_system"),
                f.lit("subscriber_patient_id"),
                f.lit("period_start_date"),
                f.lit("period_end_date"),
                f.lit("status"),
                f.lit("kind"),
            ),
            f.array(
                f.col("coverage_internal_id"),
                f.col("bene_mbi_id"),
                f.lit("http://hl7.org/fhir/sid/us-mbi"),
                f.col("patient_internal_id"),
                f.col("coverage_start_date"),
                f.col("coverage_end_date"),
                f.lit("active"),
                f.lit("insurance"),
            ),
        ).alias("coverage_rsc"),
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

    # adding supplemental_data

    row_dict.get("coverage_rsc").update(row_dict.get("cov_extention"))
    resources.append(
        _transform_resource(
            data_dict=row_dict.get("coverage_rsc"),
            resource_type=ResourceType.Coverage.value,
            template=COV_JINJA_TEMPLATE,
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
    default="cclf_beneficiary_stage_to_fhirbundle_job",
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
        cclf8_delta_table_location = os.path.join(delta_schema_location, "cclf8")
        log.warn(f"cclf8_delta_table_location: {cclf8_delta_table_location}")

        cclf9_delta_table_location = os.path.join(delta_schema_location, "cclf9")
        log.warn(f"cclf9_delta_table_location: {cclf9_delta_table_location}")

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
            "file_type": "bene_claim",
            "src_organization_id": src_organization_id,
        }

        # add storage context in spark session
        spark = add_storage_context(spark, [cclf8_delta_table_location, cclf9_delta_table_location])

        # load the records from delta table location
        log.warn("load records from cclf8 delta table location")
        cclf8_data_df = (
            spark.read.format("delta")
            .load(cclf8_delta_table_location)
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

        log.warn("load records from cclf9 delta table location")
        cclf9_data_df = (
            spark.read.format("delta")
            .load(cclf9_delta_table_location)
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

        cclf8_data_df = cclf8_data_df.withColumn(
            "bene_mbi_id",
            f.expr(f"aes_decrypt(unhex(bene_mbi_id) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
        )

        cclf8_data_df = cclf8_data_df.withColumn(
            "bene_dob",
            f.expr(f"aes_decrypt(unhex(bene_dob) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
        )

        cclf8_data_df = cclf8_data_df.withColumn(
            "bene_1st_name",
            f.expr(f"aes_decrypt(unhex(bene_1st_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
        )

        cclf8_data_df = cclf8_data_df.withColumn(
            "bene_midl_name",
            f.expr(f"aes_decrypt(unhex(bene_midl_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
        )

        cclf8_data_df = cclf8_data_df.withColumn(
            "bene_last_name",
            f.expr(f"aes_decrypt(unhex(bene_last_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
        )

        cclf8_data_df = cclf8_data_df.withColumn(
            "bene_line_1_adr",
            f.expr(f"aes_decrypt(unhex(bene_line_1_adr) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
        )

        cclf8_data_df = cclf8_data_df.withColumn(
            "bene_line_2_adr",
            f.expr(f"aes_decrypt(unhex(bene_line_2_adr) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
        )

        cclf9_data_df = cclf9_data_df.withColumn(
            "bene_mbi_id",
            f.expr(f"aes_decrypt(unhex(crnt_num) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
        )

        # join the cclf8 and cclf9 table
        data_df = cclf8_data_df.join(cclf9_data_df, on="bene_mbi_id", how="left").fillna("")

        if data_df.isEmpty():
            log.warn(f"Spark job {app_name} completed successfully, There is lack of adequate rows for processing.")
            sys.exit(0)

        mdcr_status_code = f.when(
            (f.col("bene_mdcr_stus_cd") != "") & (f.col("bene_mdcr_stus_cd") != "NA"),
            f.struct(
                f.lit("https://bluebutton.cms.gov/resources/variables/ms_cd").alias("url"),
                f.col("bene_mdcr_stus_cd").alias("value_code"),
            ),
        )

        dual_status_code = f.when(
            (f.col("bene_dual_stus_cd") != "") & (f.col("bene_dual_stus_cd") != "NA"),
            f.struct(
                f.lit("https://bluebutton.cms.gov/resources/variables/dual_01").alias("url"),
                f.col("bene_dual_stus_cd").alias("value_code"),
            ),
        )

        original_reason_code = f.when(
            (f.col("bene_orgnl_entlmt_rsn_cd") != "") & (f.col("bene_orgnl_entlmt_rsn_cd") != "NA"),
            f.struct(
                f.lit("https://bluebutton.cms.gov/resources/variables/orec").alias("url"),
                f.col("bene_orgnl_entlmt_rsn_cd").alias("value_code"),
            ),
        )
        buyin_indicator = f.when(
            (f.col("bene_entlmt_buyin_ind") != "") & (f.col("bene_entlmt_buyin_ind") != "NA"),
            f.struct(
                f.lit("https://bluebutton.cms.gov/resources/variables/buyin01").alias("url"),
                f.col("bene_entlmt_buyin_ind").alias("value_code"),
            ),
        )

        data_df = (
            data_df.withColumn("patient_internal_id", f.expr("uuid()"))
            .withColumn("coverage_internal_id", f.expr("uuid()"))
            .withColumn("mdcr_status_code", mdcr_status_code)
            .withColumn("dual_status_code", dual_status_code)
            .withColumn("original_reason_code", original_reason_code)
            .withColumn("buyin_indicator", buyin_indicator)
            .withColumn("patient_dob", transform_date(f.col("bene_dob")))
            .withColumn("patient_gender", transform_gender(f.col("bene_sex_cd")))
            .withColumn("patient_race_code", transform_race(f.col("bene_race_cd")))
            .withColumn("deceased_date_time", transform_date_time(f.col("bene_death_dt")))
            .withColumn("coverage_start_date", transform_date_time(f.col("bene_part_a_enrlmt_bgn_dt")))
            .withColumn("coverage_end_date", transform_date_time(f.col("bene_death_dt")))
            .withColumn("bundle_id", f.expr("uuid()"))
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
