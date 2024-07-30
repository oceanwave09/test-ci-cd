import json
import os
import sys
from string import Template

import click
from fhirclient.resources.organization import Organization
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, start_spark
from utils.api_client import match_core_entity
from utils.enums import ResourceType
from utils.transformation import parse_date_time
from utils.utils import exit_with_error, load_config, prepare_fhir_bundle

PAT_JINJA_TEMPLATE = "patient.j2"
ENC_JINJA_TEMPLATE = "encounter.j2"
QRESP_JINJA_TEMPLATE = "questionnaire_response.j2"
PRAC_JINJA_TEMPLATE = "practitioner.j2"


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
                                "display": "Resource Identifier"
                            }
                        ]
                    },
                    "system": "http://healthec.com/identifier/external_id",
                    "value": "$external_id"
                }
            ]
        }
        """
    )
    return attributes_template.substitute(external_id=value)


def _transform_practice(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)
    # prepare matching attribute
    row_dict["attributes"] = _get_organization_match_attributes(row_dict.get("apuid")) if row_dict.get("apuid") else ""
    return Row(**row_dict)


def _transform_patient(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("patient_internal_id", "")
    data_dict["mrn"] = row_dict.get("patient_mrn", "")
    data_dict["assigner_organization_id"] = row_dict.get("patient_organization_id", "")
    data_dict["organization_id"] = row_dict.get("patient_organization_id", "")
    resource_dict = {"resources": [transformer.render_resource(ResourceType.Patient.value, data_dict)]}
    return Row(**resource_dict)


def _transform_provider(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("practitioner_internal_id", "")
    data_dict["npi"] = row_dict.get("practitioner_npi", "")
    resource_dict = {"resources": [transformer.render_resource(ResourceType.Practitioner.value, data_dict)]}
    return Row(**resource_dict)


def _transform_encounter(row: Row, transformer: FHIRTransformer, metadata) -> Row:
    row_dict = row.asDict(recursive=True)
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("encounter_internal_id", "")
    data_dict["patient_id"] = row_dict.get("encounter_patient_id", "")
    data_dict["practitioner_id"] = row_dict.get("encounter_practitioner_id", "")
    data_dict["source_id"] = row_dict.get("encounter_source_id", "")
    data_dict["source_file_name"] = metadata.get("file_format", "")
    data_dict["assigner_organization_id"] = row_dict.get("encounter_organization_id", "")
    data_dict["class_code"] = "AMB"
    data_dict["class_code"] = "ambulatory"
    data_dict["period_start_date"] = (
        parse_date_time(row_dict.get("encounter_period_start_date"))
        if row_dict.get("encounter_period_start_date")
        else ""
    )
    data_dict["period_end_date"] = (
        parse_date_time(row_dict.get("encounter_period_end_date")) if row_dict.get("encounter_period_end_date") else ""
    )
    resource_dict = {"resources": [transformer.render_resource(ResourceType.Encounter.value, data_dict)]}
    return Row(**resource_dict)


def _transform_questionnaire_response(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("quesresponse_internal_id", "")
    data_dict["patient_id"] = row_dict.get("quesresponse_patient_id", "")
    data_dict["encounter_id"] = row_dict.get("quesresponse_encounter_id", "")
    data_dict["practitioner_id"] = row_dict.get("quesresponse_practitioner_id", "")
    data_dict["authored_date_time"] = (
        parse_date_time(row_dict.get("quesresponse_authored_date"))
        if row_dict.get("quesresponse_authored_date")
        else ""
    )
    data_dict["response_items"] = row_dict.get("quesresponse_response_items", {})
    resource_dict = {"resources": [transformer.render_resource(ResourceType.QuestionnaireResponse.value, data_dict)]}
    return Row(**resource_dict)


def render_patient(spark: SparkSession, transformer: FHIRTransformer):
    transformer.load_template(PAT_JINJA_TEMPLATE)

    def inner(df: DataFrame):
        pat_df = (
            df.withColumn("patient_mrn", f.col("patient_id"))
            .withColumn("patient_organization_id", f.col("organization_id"))
            .select(df.colRegex("`patient_.*`"))
        )
        pat_rdd = pat_df.rdd.map(lambda row: _transform_patient(row, transformer))
        pat_df = spark.createDataFrame(pat_rdd)
        return pat_df

    return inner


def render_provider(spark: SparkSession, transformer: FHIRTransformer):
    transformer.load_template(PRAC_JINJA_TEMPLATE)

    def inner(df: DataFrame):
        prov_df = df.withColumn("practitioner_npi", f.col("provider_npi")).select(df.colRegex("`practitioner_.*`"))
        prov_rdd = prov_df.rdd.map(lambda row: _transform_provider(row, transformer))
        prov_df = spark.createDataFrame(prov_rdd)
        return prov_df

    return inner


def render_encounter(spark: SparkSession, transformer: FHIRTransformer, metadata):
    transformer.load_template(ENC_JINJA_TEMPLATE)

    def inner(df: DataFrame):
        enc_df = (
            df.withColumnRenamed("patient_internal_id", "encounter_patient_id")
            .withColumnRenamed("practitioner_internal_id", "encounter_practitioner_id")
            .withColumn("encounter_organization_id", f.col("organization_id"))
            .withColumn("encounter_source_id", f.col("encounter_id"))
            .withColumn(
                "encounter_period_start_date",
                f.when(
                    f.col("encounter_date") != "",
                    f.col("encounter_date"),
                ),
            )
            .withColumn(
                "encounter_period_end_date",
                f.when(
                    f.col("encounter_date") != "",
                    f.col("encounter_date"),
                ),
            )
            .select(df.colRegex("`encounter_.*`"))
        )
        enc_rdd = enc_df.rdd.map(lambda row: _transform_encounter(row, transformer, metadata))
        enc_df = spark.createDataFrame(enc_rdd)
        return enc_df

    return inner


def render_questionnaire_response(spark: SparkSession, transformer: FHIRTransformer, seq: int = 1):
    transformer.load_template(QRESP_JINJA_TEMPLATE)

    def inner(df: DataFrame):
        qresp_df = (
            df.withColumn(
                "quesresponse_internal_id",
                f.concat_ws("_", f.lit("QuestionnaireResponse"), f.col("row_id"), f.lit(seq)),
            )
            .withColumnRenamed("patient_internal_id", "quesresponse_patient_id")
            .withColumnRenamed("encounter_internal_id", "quesresponse_encounter_id")
            .withColumnRenamed("practitioner_internal_id", "quesresponse_practitioner_id")
            .withColumn("quesresponse_authored_date", f.col("encounter_date"))
            .withColumn("quesresponse_response_items", f.col("response_items"))
            .select(df.colRegex("`quesresponse_.*`"))
        )
        qresp_rdd = qresp_df.rdd.map(lambda row: _transform_questionnaire_response(row, transformer))
        qresp_df = spark.createDataFrame(qresp_rdd)
        return qresp_df

    return inner


def to_fhir(spark: SparkSession, fhirbundle_landing_path: str, metadata: dict = {}):
    def inner(df: DataFrame):
        # initialize fhir transformer
        transformer = FHIRTransformer()

        # prepares fhir:Patient
        pat_cols = ["patient_id", "organization_id"]
        src_pat_df = (
            df.select(pat_cols)
            .drop_duplicates()
            .withColumn("patient_internal_id", f.concat_ws("_", f.lit("Patient"), f.expr("uuid()"), f.lit(1)))
        )
        resources_df = src_pat_df.transform(render_patient(spark, transformer))
        df = df.join(src_pat_df, on=pat_cols, how="left").fillna("")

        # prepares fhir:Practitioner (provider)
        prov_cols = ["provider_npi"]
        src_prov_cols = (
            df.filter(f.col("provider_npi") != "")
            .select(prov_cols)
            .drop_duplicates()
            .withColumn(
                "practitioner_internal_id",
                f.concat_ws("_", f.lit("Practitioner"), f.expr("uuid()"), f.lit(1)),
            )
        )
        if not src_prov_cols.isEmpty():
            prov_df = src_prov_cols.transform(render_provider(spark, transformer))
            resources_df = resources_df.union(prov_df)
            df = df.join(src_prov_cols, on=prov_cols, how="left").fillna("")

        # prepares fhir:Encounter
        enc_cols = [
            "encounter_id",
            "encounter_date",
            "patient_internal_id",
            "organization_id",
            "practitioner_internal_id",
        ]
        uniq_encounter_df = (
            df.filter(f.col("encounter_id") != "")
            .select(enc_cols)
            .drop_duplicates()
            .withColumn("encounter_internal_id", f.concat_ws("_", f.lit("Encounter"), f.expr("uuid()"), f.lit(1)))
        )
        if not uniq_encounter_df.isEmpty():
            enc_df = uniq_encounter_df.transform(render_encounter(spark, transformer, metadata))
            resources_df = resources_df.union(enc_df)
            df = df.join(uniq_encounter_df, on=enc_cols, how="left").fillna("")

        # prepares fhir:QuestionnaireResponse (examination)
        qresp_df = df.transform(render_questionnaire_response(spark, transformer))
        resources_df = resources_df.union(qresp_df)

        # collect all resources into a list and create fhir bundle for each file
        prepare_bundle_udf = f.udf(prepare_fhir_bundle)
        resources_df = resources_df.withColumn("resources", f.explode("resources")).agg(
            f.collect_list("resources").alias("resources")
        )
        resources_df = resources_df.withColumn("bundle_id", f.col("row_id"))
        fhir_df = resources_df.withColumn(
            "bundle",
            prepare_bundle_udf(
                f.col("bundle_id"),
                f.col("resources"),
                f.lit(fhirbundle_landing_path),
                f.lit(json.dumps(metadata)),
            ),
        ).drop("resources")

        return fhir_df

    return inner


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="ecw_examination_stage_to_fhirbundle_job",
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
        delta_schema_location = os.environ.get(
            "DELTA_SCHEMA_LOCATION",
            config.get("delta_schema_location", ""),
        )
        if not delta_schema_location:
            exit_with_error(
                log,
                "delta schema location should be provided!",
            )

        delta_table_name = os.environ.get(
            "DELTA_TABLE_NAME",
            config.get("delta_table_name", "ecw_examination"),
        )
        if not delta_table_name:
            exit_with_error(
                log,
                "delta table location should be provided!",
            )

        # construct delta table location
        delta_table_location = os.path.join(delta_schema_location, delta_table_name)
        log.warn(f"delta_table_location: {delta_table_location}")

        file_batch_id = os.environ.get("FILE_BATCH_ID", config.get("file_batch_id", ""))
        if not file_batch_id:
            exit_with_error(log, "file batch id should be provided!")
        log.warn(f"file_batch_id: {file_batch_id}")

        file_source = os.environ.get("FILE_SOURCE", config.get("file_source", ""))
        log.warn(f"file_source: {file_source}")

        landing_path = os.environ.get("LANDING_PATH", config.get("landing_path", ""))
        if not landing_path:
            exit_with_error(log, "landing path should be provided!")
        log.warn(f"landing_path: {landing_path}")

        pipeline_data_key = os.environ.get("PIPELINE_DATA_KEY", config.get("pipeline_data_key", ""))
        if not pipeline_data_key:
            exit_with_error(log, "pipeline data key should be provided!")

        if file_source:
            fhirbundle_landing_path = os.path.join(landing_path, file_source)
        else:
            fhirbundle_landing_path = landing_path
        log.warn(f"fhirbundle_landing_path: {fhirbundle_landing_path}")

        file_tenant = os.environ.get("FILE_TENANT", config.get("file_tenant", ""))
        log.warn(f"file_tenant: {file_tenant}")

        resource_type = os.environ.get("RESOURCE_TYPE", config.get("resource_type", ""))
        log.warn(f"resource_type: {resource_type}")

        src_file_name = os.environ.get("SRC_FILE_NAME", config.get("src_file_name", ""))
        log.warn(f"src_file_name: {src_file_name}")

        src_organization_id = os.environ.get("SRC_ORGANIZATION_ID", config.get("src_organization_id", ""))
        log.warn(f"src_organization_id: {src_organization_id}")

        file_format = os.environ.get("FILE_FORMAT", config.get("file_format", ""))
        log.warn(f"file_format: {file_format}")

        # construct metadata
        metadata = {
            "file_tenant": file_tenant,
            "file_format": file_format,
            "file_source": file_source,
            "resource_type": resource_type,
            "file_batch_id": file_batch_id,
            "src_file_name": src_file_name,
            "src_organization_id": src_organization_id,
        }

        # add storage context in spark session
        spark = add_storage_context(spark, [delta_table_location])

        # load the records from delta table location
        log.warn("load records from delta table location")
        data_df = (
            spark.read.format("delta")
            .load(delta_table_location)
            .filter(f.col("file_batch_id") == file_batch_id)
            .drop(
                "file_batch_id",
                "file_name",
                "file_source_name",
                "file_status",
                "created_user",
                "created_ts",
                "updated_user",
                "updated_ts",
            )
            .fillna("")
        )

        if data_df.isEmpty():
            log.warn(f"Spark job {app_name} completed successfully, There is lack of adequate rows for processing.")
            sys.exit(0)

        # apply decryption on personal details
        data_df = data_df.withColumn(
            "provider_npi",
            f.expr(f"aes_decrypt(unhex(provider_npi) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
        )

        # validate `apuid`
        val_pract_df = data_df.filter(f.col("apuid") == "")
        if not val_pract_df.isEmpty():
            exit_with_error(log, "apuid must be a not-null value.")
        # validate `patient_id`
        val_pat_df = data_df.filter(f.col("patient_id") == "")
        if not val_pat_df.isEmpty():
            exit_with_error(log, "patient id must be a not-null value.")
        # # validate `provider_npi`
        # val_prov_df = data_df.filter(f.col("provider_npi") == "")
        # if len(val_prov_df.head(1)) > 0:
        #     exit_with_error(log, "provider npi must be a not-null value.")

        # register provider service udf
        prov_service_udf = f.udf(lambda attr: match_core_entity(attr, Organization))

        # filter valid apuid by tenant
        pract_df = data_df.select("apuid").drop_duplicates()
        pract_rdd = pract_df.rdd.map(lambda row: _transform_practice(row))
        pract_df = spark.createDataFrame(pract_rdd)

        # match organization by external id
        match_pract_df = (
            pract_df.withColumn("organization_id", prov_service_udf(pract_df["attributes"]))
            .filter(f.col("organization_id").isNotNull())
            .select(["apuid", "organization_id"])
        )

        # select rows having organization match
        data_df = data_df.join(match_pract_df, on="apuid", how="inner")

        # select required columns
        data_df = data_df.select(
            [
                "patient_id",
                "organization_id",
                "encounter_id",
                "encounter_date",
                "provider_npi",
                "main_category_name",
                "main_category_id",
                "category_name",
                "category_id",
                "item_name",
                "item_id",
                "question",
                "question_id",
                "answers",
                "answers_id",
            ]
        )

        # group by item
        questions_df = data_df.groupBy(
            [
                "patient_id",
                "organization_id",
                "encounter_id",
                "encounter_date",
                "provider_npi",
                "main_category_name",
                "main_category_id",
                "category_name",
                "category_id",
                "item_name",
                "item_id",
            ]
        ).agg(f.collect_list(f.struct("question_id", "question", "answers_id", "answers")).alias("questions"))

        # group by category
        response_items_df = questions_df.groupBy(
            [
                "patient_id",
                "organization_id",
                "encounter_id",
                "encounter_date",
                "provider_npi",
                "main_category_name",
                "main_category_id",
                "category_name",
                "category_id",
            ]
        ).agg(f.collect_list(f.struct("item_id", "item_name", "questions")).alias("response_items"))

        # include uuid in all rows
        response_items_df = response_items_df.withColumn("row_id", f.expr("uuid()"))

        if not response_items_df.isEmpty():
            # transform into fhir bundle and write into target location
            fhir_df = response_items_df.transform(to_fhir(spark, fhirbundle_landing_path, metadata))
            fhir_df.head(1)

        log.warn(
            f"spark job {app_name} completed successfully, "
            f"from file with batch id {file_batch_id} transformed "
            f"into fhir bundle and copied to {fhirbundle_landing_path}."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
