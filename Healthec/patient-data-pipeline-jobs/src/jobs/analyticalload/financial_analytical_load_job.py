import os

import click
from pyspark.sql import functions as f

from analytical.entities.claim import update_medical_rx_claim_table
from analytical.entities.coverage import update_coverage_table
from analytical.entities.insurance_plan import update_insurance_plan_table
from dependencies.spark import add_storage_context, start_spark
from utils.utils import exit_with_error, load_config


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="financial_analytical_load_job",
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
        "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
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
        delta_schema_location = os.environ.get("DELTA_SCHEMA_LOCATION", config.get("delta_table_location", ""))
        if not delta_schema_location:
            exit_with_error(log, "delta schema location should be provided!")
        log.info(f"delta_schema_location: {delta_schema_location}")

        event_file_path = os.environ.get(
            "EVENT_FILE_PATH",
            config.get("event_file_path", ""),
        )
        if not event_file_path:
            exit_with_error(log, "event file path should be provided!")
        log.info(f"event_file_path: {event_file_path}")

        # add storage context in spark session
        spark = add_storage_context(spark, [event_file_path, delta_schema_location])

        # load the financial event file
        log.warn("load financial event file path into dataframe")
        event_df = spark.read.options(header=True).csv(event_file_path)

        event_fields = ["resourceType", "resourceId", "ownerType", "ownerId"]
        filtered_event_df = (
            event_df.filter(f.col("event").isin(["create", "update"])).dropDuplicates(event_fields).select(event_fields)
        )
        filtered_event_df.persist()

        # update coverage
        log.warn("update analytical coverage table")
        coverage_event_df = filtered_event_df.filter(f.col("resourceType") == "Coverage")
        if not coverage_event_df.isEmpty():
            coverage_df = coverage_event_df.transform(update_coverage_table(spark, delta_schema_location))
            log.warn(f"coverage inserted/updated : {coverage_df.count()}")

        # update claim
        log.warn("update analytical medical_claim_info, medical_claim_service_line and rx_claim_info tables")
        claim_event_df = filtered_event_df.filter(f.col("resourceType") == "Claim")
        if not claim_event_df.isEmpty():
            medical_claim_df = claim_event_df.transform(update_medical_rx_claim_table(spark, delta_schema_location))
            log.warn(f"medical and rx claim inserted/updated : {medical_claim_df.count()}")

        # update insurance plan
        log.warn("update analytical insurance plan table")
        insurance_plan_event_df = filtered_event_df.filter(f.col("resourceType") == "InsurancePlan")
        if not insurance_plan_event_df.isEmpty():
            insurance_plan_df = insurance_plan_event_df.transform(
                update_insurance_plan_table(spark, delta_schema_location)
            )
            log.warn(f"insurance_plan inserted/updated : {insurance_plan_df.count()}")

        log.warn(
            f"spark job {app_name} completed successfully, "
            f"{event_file_path} loaded into analytical delta schema path {delta_schema_location}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
