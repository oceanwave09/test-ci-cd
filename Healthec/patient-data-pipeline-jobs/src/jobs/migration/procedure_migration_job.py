import os
from string import Template

import click
from fhirclient.resources.encounter import Encounter
from fhirclient.resources.patient import Patient
from fhirclient.resources.practitioner import Practitioner
from fhirclient.resources.procedure import Procedure
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import Row
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, get_column, start_spark
from utils.api_client import match_core_entity, match_sub_entity, post_sub_entity
from utils.enums import ResourceType
from utils.transformation import parse_date_time
from utils.utils import exit_with_error, load_config

PRC_JINJA_TEMPLATE = "procedure.j2"


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
                                "code": "NPI"
                            }
                        ]
                    },
                    "system": "urn:oid:2.16.840.1.113883.4.6",
                    "value": "$npi"
                }
            ]
        }
        """
    )
    return attributes_template.substitute(npi=value)


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


def transform_patient(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)

    # prepare matching attribute
    row_dict["attributes"] = (
        _get_patient_match_attributes(row_dict.get("HEALTHRECORDKEY")) if row_dict.get("HEALTHRECORDKEY") else ""
    )

    return Row(**row_dict)


def transform_provider(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)

    # prepare matching attribute
    row_dict["attributes"] = (
        _get_practitioner_match_attributes(row_dict.get("Provider_NPI")) if row_dict.get("Provider_NPI") else ""
    )

    return Row(**row_dict)


def transform_encounter(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)

    # prepare matching attribute
    row_dict["attributes"] = (
        _get_encounter_match_attributes(row_dict.get("HOSPITALIZATION_ID")) if row_dict.get("HOSPITALIZATION_ID") else ""
    )

    return Row(**row_dict)


def transform_procedure(row: Row, transformer: FHIRTransformer) -> Row:
    data_dict = row.asDict(recursive=True)
    response_dict = {}
    response_dict["row_id"] = data_dict.get("row_id")

    data_dict.pop("row_id")

    # type cast performed date time
    data_dict["performed_date_time"] = (
        parse_date_time(data_dict.get("performed_date_time")) if data_dict.get("performed_date_time") else ""
    )
    # update status
    status = data_dict.get("status", "")
    if status and status == "Active":
        data_dict["status"] = "completed"

    # render FHIR Encounter resource
    resource = transformer.render_resource(ResourceType.Procedure.value, data_dict)

    post_response = post_sub_entity(resource, Procedure, data_dict["patient_id"], "Patient")
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
    default="procedure_migration_job",
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
        spark = add_storage_context(spark, [data_file_path, error_file_path])

        # load the procedure data file
        log.warn("load procedure data file path into dataframe")
        src_df = spark.read.options(header=True).csv(data_file_path)

        # add the row_id
        src_df = src_df.withColumn("row_id", f.expr("uuid()"))
        src_df.persist()

        # apply transformation on patient dataframe
        patient_df = src_df.select("HEALTHRECORDKEY").drop_duplicates()
        patient_rdd = patient_df.rdd.map(lambda row: transform_patient(row)).persist()
        patient_df = spark.createDataFrame(patient_rdd)
        patient_df.persist()

        # register patient service udf
        patient_service_udf = f.udf(lambda df: match_core_entity(df, Patient))

        # get patient by member id
        processed_patient_df = patient_df.withColumn("patient_id", patient_service_udf(patient_df["attributes"])).select(
            ["HEALTHRECORDKEY", "patient_id"]
        )
        processed_patient_df.persist()

        data_df = src_df.join(processed_patient_df, on="HEALTHRECORDKEY", how="left")

        # apply transformation on practitioner dataframe
        pract_df = data_df.select("Provider_NPI").drop_duplicates()
        pract_rdd = pract_df.rdd.map(lambda row: transform_provider(row)).persist()
        pract_df = spark.createDataFrame(pract_rdd)
        pract_df.persist()

        # register patient service udf
        pract_service_udf = f.udf(lambda df: match_core_entity(df, Practitioner))

        # get patient by member id
        processed_pract_df = pract_df.withColumn("practitioner_id", pract_service_udf(pract_df["attributes"])).select(
            ["Provider_NPI", "practitioner_id"]
        )
        processed_pract_df.persist()

        data_df = data_df.join(processed_pract_df, on="Provider_NPI", how="left")

        # apply transformation on encounter data dataframe
        encounter_df = data_df.select("HOSPITALIZATION_ID", "patient_id").drop_duplicates()
        encounter_rdd = encounter_df.rdd.map(lambda row: transform_encounter(row)).persist()
        encounter_df = spark.createDataFrame(encounter_rdd)
        encounter_df.persist()

        # register encounter service udf
        encounter_service_udf = f.udf(lambda df, scope_id: match_sub_entity(df, Encounter, scope_id, "Patient"))

        # match encounter by tax id
        processed_encounter_df = encounter_df.withColumn(
            "encounter_id", encounter_service_udf(encounter_df["attributes"], encounter_df["patient_id"])
        ).select(["HOSPITALIZATION_ID", "encounter_id"])
        processed_encounter_df.persist()

        data_df = data_df.join(processed_encounter_df, on="HOSPITALIZATION_ID", how="left")

        data_df = (
            data_df.withColumn("performed_date_time", get_column(data_df, "DATE_OF_SURGERY"))
            .withColumn("reason_code", get_column(data_df, "Reason"))
            .withColumn("code_display", get_column(data_df, "PROCEDURE_DESCRIPTION"))
            .withColumn("code_system", get_column(data_df, "Code"))
            .withColumn("code", get_column(data_df, "PROCEDURE_CODE"))
            .withColumn("body_site_text", get_column(data_df, "BodySite"))
            .withColumn("status", get_column(data_df, "Status"))
            .withColumn("status_reason_text", get_column(data_df, "StatusReason"))
            .select(
                "row_id",
                "performed_date_time",
                "reason_code",
                "code_display",
                "code",
                "code_system",
                "body_site_text",
                "status",
                "status_reason_text",
                "patient_id",
                "encounter_id",
                "practitioner_id",
            )
        )
        # fill null with empty value
        data_df = data_df.fillna("")
        data_df.persist()

        # initialize fhirtransformer
        transformer = FHIRTransformer()
        transformer.load_template(PRC_JINJA_TEMPLATE)

        response_rdd = data_df.rdd.map(lambda row: transform_procedure(row, transformer)).persist()
        response_df = spark.createDataFrame(response_rdd)

        procedure_processed = response_df.count()
        error_df = response_df.filter(f.col("post_response") != "success")
        error_df.persist()
        procedure_failure = error_df.count()

        if not error_df.isEmpty():
            failure_pd_df = src_df.join(error_df, on="row_id", how="inner").toPandas()
            failure_pd_df.to_csv(error_file_path, index=False)

        log.warn(f"total number of procedure processed: {procedure_processed}")
        log.warn(f"total number of procedure success: {(procedure_processed - procedure_failure)}")
        log.warn(f"total number of procedure failure: {procedure_failure}")
        log.warn(f"spark job {app_name} completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
