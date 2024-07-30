import os
import re
from string import Template

import click
from fhirclient.resources.medication import Medication
from fhirclient.resources.medicationrequest import MedicationRequest
from fhirclient.resources.patient import Patient
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import Row
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, start_spark
from utils.api_client import match_core_entity, post_core_entity, post_sub_entity
from utils.enums import ResourceType
from utils.transformation import parse_date_time
from utils.utils import exit_with_error, load_config

MEDI_JINJA_TEMPLATE = "medication.j2"
MEDI_REQ_JINJA_TEMPLATE = "medication_request.j2"
PATTERN = r"^(\d+)\s*(\w+)$"


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


def transform_medication(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    data_dict = {}
    response_dict = {}
    response_dict["PRODUCTCODE"] = row_dict["PRODUCTCODE"]
    match = re.match(PATTERN, row_dict["STRENGTHUNITS"])
    if match:
        row_dict["STRENGTHUNITS"] = match.group(2)
    data_dict["code_display"] = row_dict["DRUGDESCRIPTION"]
    data_dict["code"] = row_dict["PRODUCTCODE"]
    # data_dict["totalVolume_quantity"] = row_dict["QUANTITY"]
    data_dict["form_code"] = row_dict["DOSAGEFORM"]
    data_dict["ingredients"] = [{"num_value": row_dict["STRENGTH"], "num_unit": row_dict["STRENGTHUNITS"]}]
    # transform medication status
    if row_dict.get("STATUS"):
        if row_dict.get("STATUS") == "Y":
            data_dict["status"] = "active"
        elif row_dict.get("STATUS") == "N":
            data_dict["status"] = "inactive"

    # transform code system
    if row_dict.get("PRODUCTCODEQUALIFIER"):
        if row_dict.get("PRODUCTCODEQUALIFIER") == "RxNorm":
            data_dict["code_system"] = "urn:oid:2.16.840.1.113883.6.88"
        elif row_dict.get("PRODUCTCODEQUALIFIER") == "NDC":
            data_dict["code_system"] = "urn:oid:2.16.840.1.113883.6.69"
        elif row_dict.get("PRODUCTCODEQUALIFIER") == "NDC_CODE":
            data_dict["code_system"] = "urn:oid:2.16.840.1.113883.6.69"

    # render FHIR medication resource
    resource = transformer.render_resource(ResourceType.Medication.value, data_dict)
    post_response = post_core_entity(resource, Medication)

    if "failure" in post_response:
        response_dict["medication_id"] = "failure"
    else:
        response_dict["medication_id"] = post_response
    return Row(**response_dict)


def transform_medication_request(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    resource_dict = {}
    response_dict = {}
    response_dict["row_id"] = row_dict.get("row_id")

    row_dict.pop("row_id")

    resource_dict["days_supply"] = row_dict["DAYSSUPPLY"]
    quantity = row_dict["DOSE"].split(" ")
    resource_dict["dosages"] = [
        {
            "quantity": quantity[0],
            "quantity_unit": quantity[1],
            # "frequency": row_dict["FREQUENCY"],
            "route_text": row_dict["ROUTE"],
            "frequency_period_unit": row_dict["frequency_unit"],
        }
    ]
    resource_dict["period_start_date"] = row_dict["MEDI_START_DATE"]
    resource_dict["period_end_date"] = row_dict["MEDI_END_DATE"]
    resource_dict["authored_date_time"] = parse_date_time(row_dict["WRITTENDATE"]) if row_dict["WRITTENDATE"] else ""
    resource_dict["patient_id"] = row_dict["patient_id"]
    resource_dict["medication_id"] = row_dict["medication_id"]

    # render FHIR medication resource
    resource = transformer.render_resource(ResourceType.MedicationRequest.value, resource_dict)

    post_response = post_sub_entity(resource, MedicationRequest, row_dict["patient_id"], "Patient")
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
    default="medication_migration_job",
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

        # load the medication data file
        log.warn("load medication data file path into dataframe")
        src_df = spark.read.options(header=True).csv(data_file_path)

        # add the row_id
        src_df = src_df.withColumn("row_id", f.expr("uuid()"))
        src_df.persist()

        # prepare patient match attribute
        patient_df = src_df.select("HEALTHRECORDKEY").drop_duplicates(["HEALTHRECORDKEY"])
        patient_rdd = patient_df.rdd.map(lambda row: transform_patient(row)).persist()
        patient_df = spark.createDataFrame(patient_rdd)
        patient_df.persist()
        # register patient service udf
        pat_service_udf = f.udf(lambda df: match_core_entity(df, Patient))

        # match patient by tax id
        processed_patient_df = patient_df.withColumn("patient_id", pat_service_udf(patient_df["attributes"])).select(
            ["HEALTHRECORDKEY", "patient_id"]
        )
        processed_patient_df.persist()

        # join and update patient id
        data_df = src_df.join(processed_patient_df, on="HEALTHRECORDKEY", how="left")

        # Apply transformation on medication data dadaframe
        transformer = FHIRTransformer()
        transformer.load_template(MEDI_JINJA_TEMPLATE)
        medication_df = data_df.select(
            [
                "DRUGDESCRIPTION",
                "PRODUCTCODE",
                "PRODUCTCODEQUALIFIER",
                "QUANTITY",
                "QUANTITYQUALIFIER",
                "DOSAGEFORM",
                "STRENGTH",
                "STRENGTHUNITS",
            ]
        ).drop_duplicates()
        medication_rdd = medication_df.rdd.map(lambda row: transform_medication(row, transformer)).persist()
        processed_medication_df = spark.createDataFrame(medication_rdd).filter(f.col("medication_id") != "failure")

        # join and update patient id
        data_df = data_df.join(processed_medication_df, on="PRODUCTCODE", how="left")

        # Apply transformation on medication request data dadaframe
        transformer.load_template(MEDI_REQ_JINJA_TEMPLATE)
        response_rdd = data_df.rdd.map(lambda row: transform_medication_request(row, transformer)).persist()
        response_df = spark.createDataFrame(response_rdd)

        medication_req_processed = response_df.count()
        error_df = response_df.filter(f.col("post_response") != "success")
        error_df.persist()
        medication_req_failure = error_df.count()

        if not error_df.isEmpty():
            failure_pd_df = src_df.join(error_df, on="row_id", how="inner").toPandas()
            failure_pd_df.to_csv(error_file_path, index=False)

        log.warn(f"total number of medication request processed: {medication_req_failure}")
        log.warn(f"total number of medication request success: {(medication_req_processed - medication_req_failure)}")
        log.warn(f"total number of medication request failure: {medication_req_failure}")
        log.warn(f"spark job {app_name} completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
