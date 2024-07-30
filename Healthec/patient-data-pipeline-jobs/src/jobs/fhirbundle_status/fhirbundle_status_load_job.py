import os

import click
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, start_spark
from utils.utils import exit_with_error, load_config


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="fhirbundle_status_load_job",
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
        delta_schema_location = os.environ.get("DELTA_SCHEMA_LOCATION", config.get("delta_schema_location", ""))
        if not delta_schema_location:
            exit_with_error(log, "delta schema location should be provided!")

        delta_table_name = os.environ.get("DELTA_TABLE_NAME", config.get("delta_table_name", "fhirbundle_status"))
        if not delta_table_name:
            exit_with_error(log, "delta table location should be provided!")

        # construct delta table location
        delta_table_location = os.path.join(delta_schema_location, delta_table_name)
        log.warn(f"delta_table_location: {delta_table_location}")

        event_file_path = os.environ.get(
            "EVENT_FILE_PATH",
            config.get("event_file_path", ""),
        )
        if not event_file_path:
            exit_with_error(log, "event file path should be provided!")
        log.info(f"event_file_path: {event_file_path}")

        # add storage context in spark session
        spark = add_storage_context(spark, [event_file_path, delta_schema_location])

        # load the fhirbundle status event file
        log.warn("load fhirbundle status event file path into dataframe")
        fhirbundle_event_df = spark.read.options(header=True).csv(event_file_path)

        log.warn("update fhirbundle status  table")
        if not fhirbundle_event_df.isEmpty():
            fhirbundle_event_final_df = (
                fhirbundle_event_df.withColumn("organization_ids", f.split(f.col("organization_ids"), ","))
                .withColumn("location_ids", f.split(f.col("location_ids"), ","))
                .withColumn("practitioner_ids", f.split(f.col("practitioner_ids"), ","))
                .withColumn("practitioner_role_ids", f.split(f.col("practitioner_role_ids"), ","))
                .withColumn("encounter_ids", f.split(f.col("encounter_ids"), ","))
                .withColumn("patient_ids", f.split(f.col("patient_ids"), ","))
                .withColumn("condition_ids", f.split(f.col("condition_ids"), ","))
                .withColumn("observation_ids", f.split(f.col("observation_ids"), ","))
                .withColumn("procedure_ids", f.split(f.col("procedure_ids"), ","))
                .withColumn("immunization_ids", f.split(f.col("immunization_ids"), ","))
                .withColumn("allergy_intolerance_ids", f.split(f.col("allergy_intolerance_ids"), ","))
                .withColumn("medication_ids", f.split(f.col("medication_ids"), ","))
                .withColumn("medication_request_ids", f.split(f.col("medication_request_ids"), ","))
                .withColumn("medication_statement_ids", f.split(f.col("medication_statement_ids"), ","))
                .withColumn("claim_ids", f.split(f.col("claim_ids"), ","))
                .withColumn("claim_respone_ids", f.split(f.col("claim_respone_ids"), ","))
                .withColumn("coverage_ids", f.split(f.col("coverage_ids"), ","))
                .withColumn("insurance_plan_ids", f.split(f.col("insurance_plan_ids"), ","))
                .withColumn("service_request_ids", f.split(f.col("service_request_ids"), ","))
                .withColumn("updated_ts", f.to_timestamp(f.col("updated_ts")))
            )

            fhirbundle_event_final_df.write.format("delta").mode("append").save(delta_table_location)
            log.warn(f"fhirbundle status inserted : {fhirbundle_event_final_df.count()}")

            log.warn(
                f"spark job {app_name} completed successfully, "
                f"{event_file_path} loaded into fhirbundle status delta schema path {delta_schema_location}"
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
