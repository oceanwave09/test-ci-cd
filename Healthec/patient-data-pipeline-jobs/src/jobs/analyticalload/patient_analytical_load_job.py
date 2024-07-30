import os

import click
from pyspark.sql import functions as f

from analytical.entities.allergy_intolerance import update_allergy_intolerance_table
from analytical.entities.condition import update_condition_table
from analytical.entities.encounter import update_encounter_table
from analytical.entities.immunization import update_immunization_table
from analytical.entities.medication import update_medication_table
from analytical.entities.medication_request import update_medication_request_table
from analytical.entities.observation import update_observation_table
from analytical.entities.patient import update_patient_table
from analytical.entities.procedure import update_procedure_table
from dependencies.spark import add_storage_context, start_spark
from utils.utils import exit_with_error, load_config


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="patient_analytical_load_job",
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

        # load the patient event file
        log.warn("load patient event file path into dataframe")
        event_df = spark.read.options(header=True).csv(event_file_path)

        event_fields = ["resourceType", "resourceId", "ownerType", "ownerId"]
        filtered_event_df = (
            event_df.filter(f.col("event").isin(["create", "update"])).dropDuplicates(event_fields).select(event_fields)
        )
        filtered_event_df.persist()

        # get patient resource, parse fields and update patient analytical table
        log.warn("update patient analytical table")
        patient_event_df = filtered_event_df.filter(f.col("resourceType") == "Patient")
        if not patient_event_df.isEmpty():
            patient_df = patient_event_df.transform(update_patient_table(spark, delta_schema_location))
            log.warn(f"patients inserted/updated : {patient_df.count()}")

        # get encounter resource, parse fields and update encounter analytical table
        log.warn("update encounter analytical table")
        encounter_event_df = filtered_event_df.filter(
            (f.col("resourceType") == "Encounter") & (f.col("ownerType") == "Patient")
        )
        if not encounter_event_df.isEmpty():
            encounter_df = encounter_event_df.transform(update_encounter_table(spark, delta_schema_location))
            log.warn(f"encounter inserted/updated : {encounter_df.count()}")

        # get observation resource, parse fields and update observation analytical table
        log.warn("update observation analytical table")
        observation_event_df = filtered_event_df.filter(
            (f.col("resourceType") == "Observation") & (f.col("ownerType") == "Patient")
        )
        if not observation_event_df.isEmpty():
            observation_df = observation_event_df.transform(update_observation_table(spark, delta_schema_location))
            log.warn(f"observation inserted/updated : {observation_df.count()}")

        # get allergy intolerance resource, parse fields and update allergy_intolerance analytical table
        log.warn("update allergy_intolerance analytical table")
        allergy_intolerance_event_df = filtered_event_df.filter(
            (f.col("resourceType") == "AllergyIntolerance") & (f.col("ownerType") == "Patient")
        )

        if not allergy_intolerance_event_df.isEmpty():
            allergy_intolerance_df = allergy_intolerance_event_df.transform(
                update_allergy_intolerance_table(spark, delta_schema_location)
            )
            log.warn(f"allergy_intolerance inserted/updated : {allergy_intolerance_df.count()}")

        # get procedure resource, parse fields and update procedure analytical table
        log.warn("update procedure analytical table")
        procedure_event_df = filtered_event_df.filter(
            (f.col("resourceType") == "Procedure") & (f.col("ownerType") == "Patient")
        )
        if not procedure_event_df.isEmpty():
            procedure_df = procedure_event_df.transform(update_procedure_table(spark, delta_schema_location))
            log.warn(f"procedure inserted/updated : {procedure_df.count()}")

        # get condition resource, parse fields and update condition analytical table
        log.warn("update condition analytical table")
        condition_event_df = filtered_event_df.filter(
            (f.col("resourceType") == "Condition") & (f.col("ownerType") == "Patient")
        )
        if not condition_event_df.isEmpty():
            condition_df = condition_event_df.transform(update_condition_table(spark, delta_schema_location))
            log.warn(f"condition inserted/updated : {condition_df.count()}")

        # get medication resource, parse fields and update medication analytical table
        log.warn("update medication analytical table")
        medication_event_df = filtered_event_df.filter(f.col("resourceType") == "Medication")
        if not medication_event_df.isEmpty():
            medication_df = medication_event_df.transform(update_medication_table(spark, delta_schema_location))
            log.warn(f"medication inserted/updated : {medication_df.count()}")

        # get medication request resource, parse fields and update medication_request analytical table
        log.warn("update medication_request analytical table")
        medication_request_event_df = filtered_event_df.filter(
            (f.col("resourceType") == "MedicationRequest") & (f.col("ownerType") == "Patient")
        )
        if not medication_request_event_df.isEmpty():
            medication_request_df = medication_request_event_df.transform(
                update_medication_request_table(spark, delta_schema_location)
            )
            log.warn(f"medication request inserted/updated : {medication_request_df.count()}")

        # get immunization resource, parse fields and update immunization analytical table
        log.warn("update immunization analytical table")
        immunization_event_df = filtered_event_df.filter(
            (f.col("resourceType") == "Immunization") & (f.col("ownerType") == "Patient")
        )
        if not immunization_event_df.isEmpty():
            immunization_df = immunization_event_df.transform(update_immunization_table(spark, delta_schema_location))
            log.warn(f"immunization inserted/updated : {immunization_df.count()}")

        log.warn(
            f"spark job {app_name} completed successfully, "
            f"{event_file_path} loaded into analytical delta schema path {delta_schema_location}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
