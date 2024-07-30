import os
from string import Template

import click
from fhirclient.resources.encounter import Encounter
from fhirclient.resources.patient import Patient
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import Row
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, get_column, start_spark
from utils.api_client import match_core_entity, post_sub_entity
from utils.enums import ResourceType
from utils.transformation import parse_date_time
from utils.utils import exit_with_error, load_config

ENC_JINJA_TEMPLATE = "encounter.j2"


def _get_patient_match_attributes(value: str) -> str:
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
                    "system": "http://healthec.com/identifier/member/health_record_key",
                    "value": "$health_record_key"
                }
            ]
        }
        """
    )
    return attributes_template.substitute(health_record_key=value)


def transform_patient(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)

    # prepare matching attribute
    row_dict["attributes"] = (
        _get_patient_match_attributes(row_dict.get("HealthRecordkey")) if row_dict.get("HealthRecordkey") else ""
    )

    return Row(**row_dict)


def transform_encounter(row: Row, transformer: FHIRTransformer) -> Row:
    data_dict = row.asDict(recursive=True)
    response_dict = {}
    response_dict["row_id"] = data_dict.get("row_id")

    data_dict.pop("row_id")

    if data_dict.get("source_id"):
        data_dict["source_system"] = "http://healthec.com/identifier/hospitalization/id"

    # start date
    data_dict["period_start_date"] = (
        parse_date_time(data_dict.get("period_start_date")) if data_dict.get("period_start_date") else ""
    )
    data_dict["period_end_date"] = (
        parse_date_time(data_dict.get("period_end_date")) if data_dict.get("period_end_date") else ""
    )

    # render FHIR Encounter resource
    resource = transformer.render_resource(ResourceType.Encounter.value, data_dict)

    post_response = post_sub_entity(resource, Encounter, data_dict["patient_id"], "Patient")
    if "failure" in post_response:
        response_dict["post_response"] = post_response
    else:
        response_dict["post_response"] = "success"

    return Row(**response_dict)


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="encounter_migration_job",
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
        data_file_path = os.environ.get(
            "DATA_FILE_PATH",
            config.get("data_file_path", ""),
        )
        if not data_file_path:
            exit_with_error(log, "data file path should be provided!")
        log.warn(f"data_file_path: {data_file_path}")

        error_file_path = os.environ.get(
            "ERROR_FILE_PATH",
            config.get("error_file_path", ""),
        )
        if not error_file_path:
            exit_with_error(log, "error file path should be provided!")
        log.warn(f"error_file_path: {error_file_path}")

        # add storage context in spark session
        spark = add_storage_context(spark, [data_file_path])

        # load the encounter data file
        log.warn("load encounter data file path into dataframe")
        src_df = spark.read.options(header=True).csv(data_file_path)

        # add the row_id
        src_df = src_df.withColumn("row_id", f.expr("uuid()"))
        src_df.persist()

        # apply transformation on patient dataframe
        patient_df = src_df.select("HealthRecordkey").drop_duplicates()
        patient_rdd = patient_df.rdd.map(lambda row: transform_patient(row)).persist()
        patient_df = spark.createDataFrame(patient_rdd)
        patient_df.persist()

        # register patient service udf
        pat_service_udf = f.udf(lambda df: match_core_entity(df, Patient))

        # get patient by member id
        processed_patient_df = patient_df.withColumn("patient_id", pat_service_udf(patient_df["attributes"])).select(
            ["HealthRecordkey", "patient_id"]
        )
        processed_patient_df.persist()

        data_df = src_df.join(processed_patient_df, on="HealthRecordkey", how="left")

        data_df = (
            data_df.withColumn("source_id", get_column(data_df, "HOSPITALIZATION_ID"))
            .withColumn("class_code", get_column(data_df, "HOSPITALIZATION_TYPE_CODE"))
            .withColumn("class_display", get_column(data_df, "HOSPTLIZ_TYPE_DESC"))
            .withColumn("period_start_date", get_column(data_df, "ADMISSION_DATE"))
            .withColumn("period_end_date", get_column(data_df, "DISCHARGE_DATE"))
            .withColumn("discharge_disposition_code", get_column(data_df, "DISCHARGE_DISPOSITION"))
            .select(
                "row_id",
                "source_id",
                "class_code",
                "class_display",
                "period_start_date",
                "period_end_date",
                "discharge_disposition_code",
                "patient_id",
            )
        )
        # fill null with empty value
        data_df = data_df.fillna("")
        data_df.persist()

        # initialize fhirtransformer
        transformer = FHIRTransformer()
        transformer.load_template(ENC_JINJA_TEMPLATE)

        response_rdd = data_df.rdd.map(lambda row: transform_encounter(row, transformer)).persist()
        response_df = spark.createDataFrame(response_rdd)

        encounter_processed = response_df.count()
        error_df = response_df.filter(f.col("post_response") != "success")
        error_df.persist()
        encounter_failure = error_df.count()

        if not error_df.isEmpty():
            failure_pd_df = src_df.join(error_df, on="row_id", how="inner").toPandas()
            failure_pd_df.to_csv(error_file_path, index=False)

        log.warn(f"total number of encounter processed: {encounter_processed}")
        log.warn(f"total number of encounter success: {(encounter_processed - encounter_failure)}")
        log.warn(f"total number of encounter failure: {encounter_failure}")
        log.warn(f"spark job {app_name} completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
