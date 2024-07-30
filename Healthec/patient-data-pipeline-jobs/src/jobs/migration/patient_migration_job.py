import os
from string import Template

import click
from fhirclient.resources.organization import Organization
from fhirclient.resources.patient import Patient
from fhirclient.resources.practitioner import Practitioner
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import Row
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, get_column, start_spark
from utils.api_client import match_core_entity, post_core_entity
from utils.constants import UNKNOWN_ETHNICITY_CODE, UNKNOWN_ETHNICITY_DISPLAY, UNKNOWN_RACE_CODE, UNKNOWN_RACE_DISPLAY
from utils.enums import ResourceType
from utils.transformation import parse_boolean, parse_date, parse_date_time, parse_marital_status, update_gender
from utils.utils import exit_with_error, load_config

PAT_JINJA_TEMPLATE = "patient.j2"


def _get_organization_match_attributes(value: str) -> str:
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
                                "system": "Resource Identifier"
                            }
                        ]
                    },
                    "system": "",
                    "value": "$sub_group_id"
                }
            ]
        }
        """
    )
    return attributes_template.substitute(sub_group_id=value)


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


def transform_organization(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)

    # prepare matching attribute
    row_dict["attributes"] = (
        _get_organization_match_attributes(row_dict.get("Practice_SubGroupId"))
        if row_dict.get("Practice_SubGroupId")
        else ""
    )

    return Row(**row_dict)


def transform_practitioner(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)

    # prepare matching attribute
    row_dict["attributes"] = (
        _get_practitioner_match_attributes(row_dict.get("Provider_NPI")) if row_dict.get("Provider_NPI") else ""
    )

    return Row(**row_dict)


def transform_patient(row: Row, transformer: FHIRTransformer) -> Row:
    data_dict = row.asDict(recursive=True)
    response_dict = {}
    response_dict["row_id"] = data_dict.get("row_id")

    data_dict.pop("row_id")
    # update gender
    data_dict["gender"] = update_gender(data_dict.get("gender")) if "gender" in data_dict else ""

    # parse birth date
    data_dict["dob"] = parse_date(data_dict.get("dob")) if "dob" in data_dict else ""

    # parse deceased data
    data_dict["deceased_date_time"] = (
        parse_date_time(data_dict.get("deceased_date_time")) if "deceased_date_time" in data_dict else ""
    )
    if not data_dict.get("deceased_date_time"):
        data_dict["deceased_boolean"] = (
            parse_boolean(data_dict.get("deceased_boolean")) if "deceased_boolean" in data_dict else ""
        )

    # parse marital status
    marital_status = parse_marital_status(data_dict.get("marital_text")) if "marital_text" in data_dict else ""
    data_dict.update(marital_status)
    data_dict.pop("marital_text")

    # update general practitioner
    if data_dict.get("practitioner_id"):
        data_dict.update({"practitioners": [{"id": data_dict.get("practitioner_id")}]})
        data_dict.pop("practitioner_id")

    resource = transformer.render_resource(ResourceType.Patient.value, data_dict)
    post_response = post_core_entity(resource, Patient)
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
    default="patient_migration_job",
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

        # load the patient data file
        log.warn("load patient data file path into dataframe")
        src_df = spark.read.options(header=True).csv(data_file_path)

        # add the row_id
        src_df = src_df.withColumn("row_id", f.expr("uuid()"))
        src_df.persist()

        # apply transformation on practice data dataframe
        practice_data_df = src_df.select("Practice_SubGroupId").drop_duplicates()
        practice_rdd = practice_data_df.rdd.map(lambda row: transform_organization(row)).persist()
        practice_df = spark.createDataFrame(practice_rdd)
        practice_df.persist()

        # register organization service udf
        org_service_udf = f.udf(lambda df: match_core_entity(df, Organization))

        # match organization by tax id
        processed_practice_df = practice_df.withColumn(
            "organization_id", org_service_udf(practice_df["attributes"])
        ).select(["Practice_SubGroupId", "organization_id"])
        processed_practice_df.persist()

        data_df = src_df.join(processed_practice_df, on="Practice_SubGroupId", how="left")

        # apply transformation on practitioner data dataframe
        practitioner_df = data_df.select("Provider_NPI").drop_duplicates()
        practitioner_rdd = practitioner_df.rdd.map(lambda row: transform_practitioner(row)).persist()
        practitioner_df = spark.createDataFrame(practitioner_rdd)
        practitioner_df.persist()

        # register practitioner service udf
        prc_service_udf = f.udf(lambda df: match_core_entity(df, Practitioner))

        # match practitioner by tax id
        processed_practitioner_df = practitioner_df.withColumn(
            "practitioner_id", prc_service_udf(practitioner_df["attributes"])
        ).select(["Provider_NPI", "practitioner_id"])
        processed_practitioner_df.persist()

        # join and update practitioner id
        data_df = data_df.join(processed_practitioner_df, on="Provider_NPI", how="left")

        data_df = (
            data_df.withColumn("subscriber_id", get_column(data_df, "SubscriberID"))
            .withColumn("health_record_key", get_column(data_df, "HealthRecordKey"))
            .withColumn("lastname", get_column(data_df, "Lastname"))
            .withColumn("firstname", get_column(data_df, "Firstname"))
            .withColumn("phone_home", get_column(data_df, "Phone_home"))
            .withColumn("phone_work", get_column(data_df, "Phone_work"))
            .withColumn("phone_mobile", get_column(data_df, "Phone_mobile"))
            .withColumn("email", get_column(data_df, "Email"))
            .withColumn("fax", get_column(data_df, "Fax"))
            .withColumn("dob", get_column(data_df, "DateOfBirth"))
            .withColumn("gender", get_column(data_df, "Gender"))
            .withColumn("race", f.lit(UNKNOWN_RACE_DISPLAY))
            .withColumn("race_code", f.lit(UNKNOWN_RACE_CODE))
            .withColumn("race_display", f.lit(UNKNOWN_RACE_DISPLAY))
            .withColumn("ethnicity", f.lit(UNKNOWN_ETHNICITY_DISPLAY))
            .withColumn("ethnicity_code", f.lit(UNKNOWN_ETHNICITY_CODE))
            .withColumn("ethnicity_display", f.lit(UNKNOWN_ETHNICITY_DISPLAY))
            .withColumn("deceased_date_time", get_column(data_df, "DeceasedDate"))
            .withColumn("deceased_boolean", get_column(data_df, "IsDeceased"))
            .withColumn("street_address_1", get_column(data_df, "SteetAddress1"))
            .withColumn("street_address_2", get_column(data_df, "StreetAddress2"))
            .withColumn("city", get_column(data_df, "City"))
            .withColumn("state", get_column(data_df, "State"))
            .withColumn("zip", get_column(data_df, "Zip"))
            .withColumn("county_code", get_column(data_df, "County"))
            .withColumn("marital_text", get_column(data_df, "Maritalstatus"))
            .withColumn("preferred_language_text", get_column(data_df, "Language"))
            .select(
                "row_id",
                "subscriber_id",
                "health_record_key",
                "lastname",
                "firstname",
                "phone_home",
                "phone_work",
                "phone_mobile",
                "email",
                "fax",
                "dob",
                "gender",
                "race",
                "race_code",
                "race_display",
                "ethnicity",
                "ethnicity_code",
                "ethnicity_display",
                "deceased_date_time",
                "deceased_boolean",
                "street_address_1",
                "street_address_2",
                "city",
                "state",
                "zip",
                "county_code",
                "marital_text",
                "preferred_language_text",
                "organization_id",
                "practitioner_id",
            )
        )
        # fill null with empty value
        data_df = data_df.fillna("")
        data_df.persist()

        # initialize fhirtransformer
        transformer = FHIRTransformer()
        transformer.load_template(PAT_JINJA_TEMPLATE)

        response_rdd = data_df.rdd.map(lambda row: transform_patient(row, transformer)).persist()
        response_df = spark.createDataFrame(response_rdd)

        patient_processed = response_df.count()
        error_df = response_df.filter(f.col("post_response") != "success")
        error_df.persist()
        patient_failure = error_df.count()

        if not error_df.isEmpty():
            failure_pd_df = src_df.join(error_df, on="row_id", how="inner").toPandas()
            failure_pd_df.to_csv(error_file_path, index=False)

        log.warn(f"total number of patient processed: {patient_processed}")
        log.warn(f"total number of patient success: {(patient_processed - patient_failure)}")
        log.warn(f"total number of patient failure: {patient_failure}")
        log.warn(f"spark job {app_name} completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
