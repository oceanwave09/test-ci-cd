import os
from string import Template

import click
from fhirclient.resources.encounter import Encounter
from fhirclient.resources.observation import Observation
from fhirclient.resources.patient import Patient
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import Row
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, get_column, start_spark
from utils.api_client import match_core_entity, match_sub_entity, post_sub_entity
from utils.enums import ResourceType
from utils.transformation import parse_date_time, parse_vital_code, to_float
from utils.utils import exit_with_error, load_config

OBS_JINJA_TEMPLATE = "observation.j2"


def _get_patient_match_attribute(value: str) -> str:
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


def _get_encounter_match_attribute(value: str) -> str:
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
        _get_patient_match_attribute(row_dict.get("HealthRecordKey")) if row_dict.get("HealthRecordKey") else ""
    )

    return Row(**row_dict)


def transform_encounter(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)

    # prepare matching attribute
    row_dict["attributes"] = (
        _get_encounter_match_attribute(row_dict.get("HOSPITALIZATION_ID")) if row_dict.get("HOSPITALIZATION_ID") else ""
    )

    return Row(**row_dict)


def _generate_vital_component(type: str, value: float, unit: str) -> dict:
    vital_code = parse_vital_code(type)
    return {
        "code": vital_code.get("code"),
        "code_system": vital_code.get("system"),
        "code_display": vital_code.get("display"),
        "quantity_value": value,
        "quantity_unit": unit,
    }


def transform_observation(row: Row, transformer: FHIRTransformer) -> Row:
    data_dict = row.asDict(recursive=True)
    response_dict = {}
    response_dict["row_id"] = data_dict.get("row_id")

    data_dict.pop("row_id")
    # validate source id
    if data_dict.get("source_id"):
        data_dict["source_system"] = "http://healthec.com/identifier/vital/health_tracker_id"

    data_dict["effective_date_time"] = (
        parse_date_time(data_dict.get("effective_date_time")) if data_dict.get("effective_date_time") else ""
    )
    # add category
    data_dict["category_system"] = "http://terminology.hl7.org/CodeSystem/observation-category"
    data_dict["category_code"] = "vital-signs"
    data_dict["category_display"] = "vital-signs"

    components = []
    units = data_dict.get("units", "")
    # process weight
    weight = to_float(data_dict.get("weight"))
    if weight:
        components.append(_generate_vital_component("weight", weight, units))
    # process height
    height = to_float(data_dict.get("height"))
    if height:
        components.append(_generate_vital_component("height", height, units))
    # process systolic
    systolic = to_float(data_dict.get("systolic"))
    if systolic:
        components.append(_generate_vital_component("systolic", systolic, units))
    # process diastolic
    diastolic = to_float(data_dict.get("diastolic"))
    if diastolic:
        components.append(_generate_vital_component("diastolic", diastolic, units))
    # process bmi
    bmi = to_float(data_dict.get("bmi"))
    if bmi:
        components.append(_generate_vital_component("bmi", bmi, units))

    data_dict["components"] = components
    # render FHIR Observation resource
    resource = transformer.render_resource(ResourceType.Observation.value, data_dict)

    post_response = post_sub_entity(resource, Observation, data_dict["patient_id"], "Patient")
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
    default="vital_migration_job",
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

        # load the observation data file
        log.warn("load observation data file path into dataframe")
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
        patient_service_udf = f.udf(lambda df: match_core_entity(df, Patient))

        # match patient by HealthRecordKey
        processed_patient_df = patient_df.withColumn("patient_id", patient_service_udf(patient_df["attributes"])).select(
            ["HealthRecordKey", "patient_id"]
        )
        processed_patient_df.persist()

        # join and update patient id
        data_df = src_df.join(processed_patient_df, on="HealthRecordKey", how="left")

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

        # join and update encounter id
        data_df = data_df.join(processed_encounter_df, on="HOSPITALIZATION_ID", how="left")

        data_df = (
            data_df.withColumn("source_id", get_column(data_df, "HEALTH_TRACKER_ID"))
            .withColumn("effective_date_time", get_column(data_df, "HEALTH_TRACKER_DATE"))
            .withColumn("code_text", get_column(data_df, "HEALTH_TRACKER_TYPE"))
            .withColumn("weight", get_column(data_df, "WEIGHT"))
            .withColumn("height", get_column(data_df, "HEIGHT"))
            .withColumn("systolic", get_column(data_df, "SYSTOLIC"))
            .withColumn("diastolic", get_column(data_df, "DIASTOLIC"))
            .withColumn("total_cholesterol", get_column(data_df, "TOTAL_CHOLESTEROL"))
            .withColumn("hdl", get_column(data_df, "HDL"))
            .withColumn("ldl", get_column(data_df, "LDL"))
            .withColumn("hours", get_column(data_df, "HOURS"))
            .withColumn("calories", get_column(data_df, "CALORIES"))
            .withColumn("bgqty", get_column(data_df, "BGQTY"))
            .withColumn("triglycerides", get_column(data_df, "TRIGLYCERIDES"))
            .withColumn("spo2", get_column(data_df, "SPO2"))
            .withColumn("bmi", get_column(data_df, "BMI"))
            .withColumn("pulse_rate", get_column(data_df, "PULSE_RATE"))
            .withColumn("units", get_column(data_df, "UNITS"))
            .select(
                "row_id",
                "source_id",
                "effective_date_time",
                "code_text",
                "weight",
                "height",
                "systolic",
                "diastolic",
                "total_cholesterol",
                "hdl",
                "ldl",
                "hours",
                "calories",
                "bgqty",
                "triglycerides",
                "spo2",
                "bmi",
                "pulse_rate",
                "units",
                "patient_id",
                "encounter_id",
            )
        )
        # fill null with empty value
        data_df = data_df.fillna("")
        data_df.persist()

        # initialize fhirtransformer
        transformer = FHIRTransformer()
        transformer.load_template(OBS_JINJA_TEMPLATE)

        vital_rdd = data_df.rdd.map(lambda row: transform_observation(row, transformer)).persist()
        vital_df = spark.createDataFrame(vital_rdd)

        vital_processed = vital_df.count()
        error_df = vital_df.filter(f.col("post_response") != "success")
        error_df.persist()
        vital_failure = error_df.count()

        if not error_df.isEmpty():
            failure_pd_df = src_df.join(error_df, on="row_id", how="inner").toPandas()
            failure_pd_df.to_csv(error_file_path, index=False)

        log.warn(f"total number of vital processed: {vital_processed}")
        log.warn(f"total number of vital success: {(vital_processed - vital_failure)}")
        log.warn(f"total number of vital failure: {vital_failure}")
        log.warn(f"spark job {app_name} completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
