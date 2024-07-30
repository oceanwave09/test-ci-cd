import os

import click
from pyspark.sql import functions as f

from analytical.entities.practitioner import update_provider_table
from analytical.entities.practitioner_role import update_provider_practice_table
from dependencies.spark import add_storage_context, start_spark
from utils.utils import exit_with_error, load_config


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="user_analytical_load_job",
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
        log.warn(f"delta_schema_location: {delta_schema_location}")

        event_file_path = os.environ.get(
            "EVENT_FILE_PATH",
            config.get("event_file_path", ""),
        )
        if not event_file_path:
            exit_with_error(log, "event file path should be provided!")
        log.warn(f"event_file_path: {event_file_path}")

        # add storage context in spark session
        spark = add_storage_context(spark, [event_file_path, delta_schema_location])

        # load the user event file
        log.warn("load user event file path into dataframe")
        event_df = spark.read.options(header=True).csv(event_file_path)

        event_fields = ["resourceType", "resourceId", "ownerType", "ownerId"]
        filtered_event_df = (
            event_df.filter(f.col("event").isin(["create", "update"])).dropDuplicates(event_fields).select(event_fields)
        )
        filtered_event_df.persist()

        # get practitioner resource, parse fields and update `provider` analytical table
        log.warn("update analytical provider table")
        practitioner_event_df = filtered_event_df.filter(f.col("resourceType") == "Practitioner")
        if not practitioner_event_df.isEmpty():
            prov_df = practitioner_event_df.transform(update_provider_table(spark, delta_schema_location))
            log.warn(f"provider inserted/updated : {prov_df.count()}")

        # get practitioner role resource, parse fields and update `provider_practice` analytical table
        log.warn("update analytical provider practice role table")
        practitioner_role_event_df = filtered_event_df.filter(
            (f.col("resourceType") == "PractitionerRole") & (f.col("ownerType") == "Practitioner")
        )
        if not practitioner_role_event_df.isEmpty():
            provider_role_df = practitioner_role_event_df.transform(
                update_provider_practice_table(spark, delta_schema_location)
            )
            log.warn(f"provider_practice inserted/updated : {provider_role_df.count()}")

        log.warn(
            f"spark job {app_name} completed successfully, "
            f"{event_file_path} loaded into analytical delta schema path {delta_schema_location}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
