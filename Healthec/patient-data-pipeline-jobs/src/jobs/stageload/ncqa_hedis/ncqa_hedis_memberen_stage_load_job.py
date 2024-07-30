import os

import click
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from dependencies.spark import add_storage_context, get_column, start_spark
from utils.constants import DEFAULT_USER_NAME
from utils.enums import RecordStatus as rs
from utils.utils import exit_with_error, generate_random_string, load_config


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="ncqa_hedis_memberen_stage_load_job",
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

        delta_table_name = os.environ.get("DELTA_TABLE_NAME", config.get("delta_table_name", "ncqa_hedis_memberen"))
        if not delta_table_name:
            exit_with_error(log, "delta table location should be provided!")

        # construct delta table location
        delta_table_location = os.path.join(delta_schema_location, delta_table_name)
        log.warn(f"delta_table_location: {delta_table_location}")

        canonical_file_path = os.environ.get(
            "CANONICAL_FILE_PATH",
            config.get("canonical_file_path", ""),
        )
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

        # add additional columns and prepare dataframe which match with ncqa hedis memberen delta lake table structure
        member_en_df = (
            canonical_df.withColumn("row_id", get_column(canonical_df, "row_id"))
            .withColumn("member_id", get_column(canonical_df, "member_id"))
            .withColumn("start_date", get_column(canonical_df, "start_date"))
            .withColumn("disenrollment_date", get_column(canonical_df, "disenrollment_date"))
            .withColumn("dental_benefit", get_column(canonical_df, "dental_benefit"))
            .withColumn("drug_benefit", get_column(canonical_df, "drug_benefit"))
            .withColumn("mental_health_benefit_inpatient", get_column(canonical_df, "mental_health_benefit_inpatient"))
            .withColumn(
                "mental_health_benefit_intensive_outpatient",
                get_column(canonical_df, "mental_health_benefit_intensive_outpatient"),
            )
            .withColumn(
                "mental_health_benefit_outpatient_ed", get_column(canonical_df, "mental_health_benefit_outpatient_ed")
            )
            .withColumn("chemdep_benefit_inpatient", get_column(canonical_df, "chemdep_benefit_inpatient"))
            .withColumn(
                "chemdep_benefit_intensive_outpatient", get_column(canonical_df, "chemdep_benefit_intensive_outpatient")
            )
            .withColumn("chemdep_benefit_outpatient_ed", get_column(canonical_df, "chemdep_benefit_outpatient_ed"))
            .withColumn("payer", get_column(canonical_df, "payer"))
            .withColumn("health_plan_employee_flag", get_column(canonical_df, "health_plan_employee_flag"))
            .withColumn("indicator", get_column(canonical_df, "indicator"))
            .withColumn("batch_id", f.lit(file_batch_id).cast(StringType()))
            .withColumn("source_system", f.lit(file_source).cast(StringType()))
            .withColumn("file_name", f.lit(file_name).cast(StringType()))
            .withColumn("status", f.lit(rs.TO_BE_VALIDATED.value))
            .withColumn("created_user", f.lit(DEFAULT_USER_NAME).cast(StringType()))
            .withColumn("created_ts", f.current_timestamp())
            .withColumn("updated_user", f.lit(DEFAULT_USER_NAME).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )

        # write data into delta lake practice table
        member_en_df.write.format("delta").mode("append").save(delta_table_location)

        log.warn(
            f"spark job {app_name} completed successfully, "
            f"{canonical_file_path} loaded into stage table path {delta_table_location}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
