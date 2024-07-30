import os
from copy import copy
from string import Template

import click
from fhirclient.resources.encounter import Encounter
from fhirclient.resources.observation import Observation
from fhirclient.resources.patient import Patient
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import Row
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, get_column, start_spark
from utils.api_client import match_core_entity, match_sub_entity, post_core_entity
from utils.constants import LONIC_CODE_SYSTEM
from utils.enums import ResourceType
from utils.transformation import parse_date_time, parse_interpretation, parse_range_reference, to_float
from utils.utils import exit_with_error, load_config

OBS_JINJA_TEMPLATE = "observation.j2"
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
        _get_encounter_match_attributes(row_dict.get("Hospitalization_ID")) if row_dict.get("Hospitalization_ID") else ""
    )
    return Row(**row_dict)


def transform_observation(row: Row, transformer: FHIRTransformer) -> Row:
    data_dict = row.asDict(recursive=True)
    response_dict = {}
    response_dict["source_id"] = data_dict.get("source_id")

    # transform effective start and end period
    data_dict["effective_period_start"] = (
        parse_date_time(data_dict.get("effective_start")) if data_dict.get("effective_start") else ""
    )
    data_dict["effective_period_end"] = (
        parse_date_time(data_dict.get("effective_end")) if data_dict.get("effective_end") else ""
    )

    # update status
    data_dict["status"] = "final"

    components = data_dict.get("components", [])
    updated_components = []
    for component in components:
        updated_component = copy(component)

        # update code system
        code_system = updated_component.get("code_system")
        if code_system and str(code_system).upper() == "LN":
            code_system = LONIC_CODE_SYSTEM
        else:
            code_system = ""
        updated_component["code_system"] = code_system

        # update reference range
        reference_range = updated_component.get("reference_range")
        if reference_range:
            (
                updated_component["reference_range_low_value"],
                updated_component["reference_range_high_value"],
            ) = parse_range_reference(reference_range)
        updated_component.pop("reference_range")

        # update interpretation
        interpretation_text = updated_component.get("interpretation_text")
        if interpretation_text:
            interpretation = parse_interpretation(interpretation_text)
            updated_component["interpretation_code"] = interpretation.get("code")
            updated_component["interpretation_system"] = interpretation.get("system")
            updated_component["interpretation_display"] = interpretation.get("display")
        updated_component.pop("interpretation_text")

        # type cast to float
        quantity_value = updated_component.get("quantity_value")
        if to_float(quantity_value):
            updated_component["quantity_value"] = to_float(quantity_value)
        else:
            updated_component.pop("quantity_value")
            updated_component.pop("quantity_unit")

        updated_components.append(updated_component)

    data_dict["components"] = updated_components

    # render FHIR Location resource
    resource = transformer.render_resource(ResourceType.Observation.value, data_dict)
    post_response = post_core_entity(resource, Observation)
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
    default="labresult_migration_job",
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

        # load the location data file
        log.warn("load location data file path into dataframe")
        src_df = spark.read.options(header=True).csv(data_file_path)

        # add the row_id
        src_df = src_df.withColumn("row_id", f.expr("uuid()"))
        src_df.persist()

        # apply transformation on patient data dataframe
        patient_df = src_df.select("HealthRecordkey").drop_duplicates()
        patient_rdd = patient_df.rdd.map(lambda row: transform_patient(row)).persist()
        patient_df = spark.createDataFrame(patient_rdd)
        patient_df.persist()

        # register patient service udf
        patient_service_udf = f.udf(lambda df: match_core_entity(df, Patient))

        # match patient by HealthRecordKey
        processed_patient_df = patient_df.withColumn("patient_id", patient_service_udf(patient_df["attributes"])).select(
            ["HealthRecordkey", "patient_id"]
        )
        processed_patient_df.persist()

        data_df = src_df.join(processed_patient_df, on="HealthRecordkey", how="left")

        # apply transformation on encounter data dataframe
        encounter_df = data_df.select("Hospitalization_ID", "patient_id").drop_duplicates()
        encounter_rdd = encounter_df.rdd.map(lambda row: transform_encounter(row)).persist()
        encounter_df = spark.createDataFrame(encounter_rdd)
        encounter_df.persist()

        # register encounter service udf
        encounter_service_udf = f.udf(lambda df, scope_id: match_sub_entity(df, Encounter, scope_id, "Patient"))

        # match encounter by tax id
        processed_encounter_df = encounter_df.withColumn(
            "encounter_id", encounter_service_udf(encounter_df["attributes"], encounter_df["patient_id"])
        ).select(["Hospitalization_ID", "encounter_id"])
        processed_encounter_df.persist()

        data_df = data_df.join(processed_encounter_df, on="Hospitalization_ID", how="left")

        # group lab results by lab test id
        data_df = (
            data_df.withColumnRenamed("Labtest_ID", "source_id")
            .withColumnRenamed("LABTEST_TYPE_DESC", "code_text")
            .groupBy(["patient_id", "encounter_id", "source_id", "code_text"])
            .agg(
                f.min(get_column(data_df, "Labtest_Date")).alias("effective_start"),
                f.max(get_column(data_df, "Labtest_Result_Date")).alias("effective_end"),
                f.collect_list(
                    f.struct(
                        get_column(data_df, "Labtest_Code").alias("code"),
                        get_column(data_df, "LABTEST_CODING_SYSTEM_NAME").alias("code_system"),
                        get_column(data_df, "LABTEST_RESULT_DESC").alias("code_display"),
                        get_column(data_df, "LABTEST_RESULT_VALUE").alias("quantity_value"),
                        get_column(data_df, "Units").alias("quantity_unit"),
                        get_column(data_df, "LABTEST_REFERENCE_RANGE").alias("reference_range"),
                        get_column(data_df, "ABNORMAL_TEST_TYPE").alias("interpretation_text"),
                    )
                ).alias("components"),
            )
            .select(
                "source_id",
                "code_text",
                "effective_start",
                "effective_end",
                "components",
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

        labresult_rdd = data_df.rdd.map(lambda row: transform_observation(row, transformer)).persist()
        labresult_df = spark.createDataFrame(labresult_rdd)

        labresult_processed = labresult_df.count()
        error_df = labresult_df.filter(f.col("post_response") != "success")
        error_df.persist()
        labresult_failure = error_df.count()

        if not error_df.isEmpty():
            failure_pd_df = (
                src_df.join(error_df, src_df.Labtest_ID == error_df.source_id, how="inner")
                .select(src_df["*"], error_df["post_response"])
                .toPandas()
            )
            failure_pd_df.to_csv(error_file_path, index=False)

        log.warn(f"total number of labresult processed: {labresult_processed}")
        log.warn(f"total number of labresult success: {(labresult_processed - labresult_failure)}")
        log.warn(f"total number of labresult failure: {labresult_failure}")
        log.warn(f"spark job {app_name} completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
