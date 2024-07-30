import os

import click
from fhirclient.resources.practitioner import Practitioner
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import Row
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, get_column, start_spark
from utils.api_client import post_core_entity
from utils.enums import ResourceType
from utils.utils import exit_with_error, load_config

PRC_JINJA_TEMPLATE = "practitioner.j2"


def transform_practitioner(row: Row, transformer: FHIRTransformer) -> Row:
    data_dict = row.asDict(recursive=True)
    response_dict = {}
    response_dict["row_id"] = data_dict.get("row_id")

    data_dict.pop("row_id")

    # render FHIR Practitioner resource
    resource = transformer.render_resource(ResourceType.Practitioner.value, data_dict)

    post_response = post_core_entity(resource, Practitioner)
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
    default="practitioner_migration_job",
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

        # load the practitioner data file
        log.warn("load practitioner data file path into dataframe")
        src_df = spark.read.options(header=True).csv(data_file_path)
        # add the row_id
        src_df = src_df.withColumn("row_id", f.expr("uuid()"))
        src_df.persist()

        data_df = (
            src_df.withColumn("npi", get_column(src_df, "NationalProviderId"))
            .withColumn("upin", get_column(src_df, "UpinNumber"))
            .withColumn("state_license", get_column(src_df, "StateLicenseID"))
            .withColumn("lastname", get_column(src_df, "LastName"))
            .withColumn("firstname", get_column(src_df, "FirstName"))
            .withColumn("middleinitials", get_column(src_df, "MiddleName"))
            .withColumn("qualification_text", get_column(src_df, "Degree"))
            .withColumn("street_address_1", get_column(src_df, "StreetAddress1"))
            .withColumn("street_address_2", get_column(src_df, "StreetAddress2"))
            .withColumn("city", get_column(src_df, "City"))
            .withColumn("state", get_column(src_df, "State"))
            .withColumn("zip", get_column(src_df, "Zip"))
            .withColumn("phone_work", get_column(src_df, "Telephone"))
            .withColumn("fax", get_column(src_df, "Fax"))
            .withColumn("email", get_column(src_df, "Email"))
            .withColumn("active", f.when(get_column(src_df, "Status") == "Y", f.lit("true")).otherwise("false"))
            .select(
                "row_id",
                "npi",
                "upin",
                "state_license",
                "lastname",
                "firstname",
                "middleinitials",
                "qualification_text",
                "street_address_1",
                "street_address_2",
                "city",
                "state",
                "zip",
                "phone_work",
                "fax",
                "email",
                "active",
            )
        )
        # fill null with empty value
        data_df = data_df.fillna("")
        data_df.persist()

        # initialize fhirtransformer
        transformer = FHIRTransformer()
        transformer.load_template(PRC_JINJA_TEMPLATE)

        practitioner_rdd = data_df.rdd.map(lambda row: transform_practitioner(row, transformer)).persist()
        practitioner_df = spark.createDataFrame(practitioner_rdd)
        practitioner_df.persist()

        practitioner_processed = practitioner_df.count()
        error_df = practitioner_df.filter(f.col("post_response") != "success")
        error_df.persist()
        practitioner_failure = error_df.count()

        if not error_df.isEmpty():
            # Attribute 'isEmpty' is new on spark 3.3.0. enables after spark version upgrade to 3.4.0
            # if not error_df.isEmpty():
            failure_pd_df = src_df.join(error_df, on="row_id", how="inner").toPandas()
            failure_pd_df.to_csv(error_file_path, index=False)

        log.warn(f"total number of practitioner processed: {practitioner_processed}")
        log.warn(f"total number of practitioner success: {(practitioner_processed - practitioner_failure)}")
        log.warn(f"total number of practitioner failure: {practitioner_failure}")
        log.warn(f"spark job {app_name} completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
