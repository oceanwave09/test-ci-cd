import os
from string import Template

import click
from fhirclient.resources.allergyintolerance import AllergyIntolerance
from fhirclient.resources.encounter import Encounter
from fhirclient.resources.patient import Patient
from fhirclient.resources.practitioner import Practitioner
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import Row
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, get_column, start_spark
from utils.api_client import match_core_entity, match_sub_entity, post_sub_entity
from utils.enums import ResourceType
from utils.transformation import parse_date_time
from utils.utils import exit_with_error, load_config

ALLERGY_JINJA_TEMPLATE = "allergy_intolerance.j2"


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
        _get_patient_match_attributes(row_dict.get("HEALTHRECORDKEY")) if row_dict.get("HEALTHRECORDKEY") else ""
    )

    return Row(**row_dict)


def _get_encounter_match_attributes(value: str) -> str:
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
                    "system": "http://healthec.com/identifier/hospitalization/id",
                    "value": "$hospitalization_id"
                }
            ]
        }
        """
    )
    return attributes_template.substitute(hospitalization_id=value)


def transform_encounter(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)

    # prepare matching attribute
    row_dict["attributes"] = (
        _get_encounter_match_attributes(row_dict.get("HOSPITALIZATION_ID")) if row_dict.get("HOSPITALIZATION_ID") else ""
    )

    return Row(**row_dict)


def _get_practitioner_match_attributes(value: str) -> str:
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
                    "system": "http://healthec.com/identifier/physician/id",
                    "value": "$physician_id"
                }
            ]
        }
        """
    )
    return attributes_template.substitute(physician_id=value)


def transform_practitioner(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)

    # prepare matching attribute
    row_dict["attributes"] = (
        _get_practitioner_match_attributes(row_dict.get("PHYSICIAN_ID")) if row_dict.get("PHYSICIAN_ID") else ""
    )

    return Row(**row_dict)


def transform_allergy_intolerance(row: Row, transformer: FHIRTransformer) -> Row:
    data_dict = row.asDict(recursive=True)
    response_dict = {}
    response_dict["row_id"] = data_dict.get("row_id")

    data_dict.pop("row_id")

    # transform clinical status
    if data_dict.get("clinical_status"):
        if data_dict.get("clinical_status") == "A":
            data_dict["clinical_status"] = "active"
        elif data_dict.get("clinical_status") == "IA":
            data_dict["clinical_status"] = "inactive"

    # transform code system
    data_dict["code_system"] = "http://hl7.org/fhir/sid/icd-10-cm"

    # transform type
    data_dict["type"] = "allergy"

    # parse onset date time
    data_dict["last_occurrence_date_time"] = (
        parse_date_time(data_dict.get("last_occurrence_date_time")) if data_dict.get("last_occurrence_date_time") else ""
    )

    # render FHIR AllergyIntolerance resource
    resource = transformer.render_resource(ResourceType.AllergyIntolerance.value, data_dict)

    post_response = post_sub_entity(resource, AllergyIntolerance, data_dict["patient_id"], "Patient")
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
    default="allergy_migration_job",
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

        # load the allergy data file
        log.warn("load allergy data file path into dataframe")
        src_df = spark.read.options(header=True).csv(data_file_path)

        # add the row_id
        src_df = src_df.withColumn("row_id", f.expr("uuid()"))
        src_df.persist()

        # prepare patient match attribute
        patient_df = src_df.select("HEALTHRECORDKEY").drop_duplicates()
        patient_rdd = patient_df.rdd.map(lambda row: transform_patient(row)).persist()
        patient_df = spark.createDataFrame(patient_rdd)
        patient_df.persist()

        # register patient service udf
        pat_service_udf = f.udf(lambda df: match_core_entity(df, Patient))

        # match patient by healthrecordkey
        processed_patient_df = patient_df.withColumn("patient_id", pat_service_udf(patient_df["attributes"])).select(
            ["HEALTHRECORDKEY", "patient_id"]
        )
        processed_patient_df.persist()
        patient_df.unpersist()

        # prepare practitioner match attribute
        provider_df = src_df.select("PHYSICIAN_ID").drop_duplicates()
        provider_rdd = provider_df.rdd.map(lambda row: transform_patient(row)).persist()
        provider_df = spark.createDataFrame(provider_rdd)
        provider_df = provider_df.na.drop(subset=["PHYSICIAN_ID"])
        provider_df.persist()

        # register practitioner service udf
        pract_service_udf = f.udf(lambda df: match_core_entity(df, Practitioner))

        # match patient by tax id
        processed_provider_df = provider_df.withColumn(
            "provider_id", pract_service_udf(provider_df["attributes"])
        ).select(["PHYSICIAN_ID", "provider_id"])
        processed_provider_df.persist()
        provider_df.unpersist()

        # prepare encounter match attribute
        encounter_df = src_df.select("HOSPITALIZATION_ID", "patient_id").drop_duplicates()
        encounter_rdd = encounter_df.rdd.map(lambda row: transform_encounter(row)).persist()
        encounter_df = spark.createDataFrame(encounter_rdd)
        encounter_df = encounter_df.na.drop(subset=["HOSPITALIZATION_ID"])
        encounter_df.persist()

        # register encounter service udf
        enc_service_udf = f.udf(lambda df, scope_id: match_sub_entity(df, Encounter, scope_id, "Patient"))

        # match encounter by hospitalization id
        processed_encounter_df = encounter_df.withColumn(
            "encounter_id", enc_service_udf(encounter_df["attributes"], encounter_df["patient_id"])
        ).select(["HOSPITALIZATION_ID", "encounter_id"])
        processed_encounter_df.persist()
        encounter_df.unpersist()

        # join and update patient, provider and encounter ids
        data_df = (
            src_df.join(processed_patient_df, on="HEALTHRECORDKEY", how="left")
            .join(processed_provider_df, on="PHYSICIAN_ID", how="left")
            .join(processed_encounter_df, on="HOSPITALIZATION_ID", how="left")
        )

        data_df = (
            data_df.withColumn("clinical_status", get_column(data_df, "STATUS"))
            .withColumn("code", get_column(data_df, "ALLERGIE_CODE"))
            .withColumn("code_display", get_column(data_df, "ALLERGIE_CODE_OTHER"))
            .withColumn("type", get_column(data_df, "ALLERGIE_TYPE_CODE"))
            .withColumn("last_occurrence", get_column(data_df, "ALLERGIE_LASTOCCURED_DATE"))
            .withColumn("manifestation_code", get_column(data_df, "ALLERGIE_REACTION_CODE"))
            .withColumn("manifestation_display", get_column(data_df, "ALLERGIE_REACTION_OTHER"))
            .withColumn("criticality", get_column(data_df, "SERVERITY"))
            .select(
                "row_id",
                "clinical_status",
                "code",
                "code_display",
                "type",
                "patient_id",
                "provider_id",
                "encounter_id",
                "last_occurrence",
                "criticality",
                "manifestation_code",
                "manifestation_display",
            )
        )
        # fill null with empty value
        data_df = data_df.fillna("")
        data_df.persist()

        # initialize fhirtransformer
        transformer = FHIRTransformer()
        transformer.load_template(ALLERGY_JINJA_TEMPLATE)

        response_rdd = data_df.rdd.map(lambda row: transform_allergy_intolerance(row, transformer)).persist()
        response_df = spark.createDataFrame(response_rdd)

        allergyintolerance_processed = response_df.count()
        error_df = response_df.filter(f.col("post_response") != "success")
        error_df.persist()
        allergyintolerance_failure = error_df.count()

        if not error_df.isEmpty():
            failure_pd_df = src_df.join(error_df, on="row_id", how="inner").toPandas()
            failure_pd_df.to_csv(error_file_path, index=False)

        log.warn(f"total number of allergy processed: {allergyintolerance_failure}")
        log.warn(f"total number of allergy success: {(allergyintolerance_processed - allergyintolerance_failure)}")
        log.warn(f"total number of allergy failure: {allergyintolerance_failure}")
        log.warn(f"spark job {app_name} completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
