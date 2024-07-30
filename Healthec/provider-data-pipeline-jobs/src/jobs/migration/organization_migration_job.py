import os

import click
from fhirclient.resources.organization import Organization
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import Row
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, get_column, start_spark
from utils.api_client import post_core_entity
from utils.constants import TYPE_PROV_CODE, TYPE_PROV_DISPLAY
from utils.enums import ResourceType
from utils.utils import exit_with_error, load_config

ORG_JINJA_TEMPLATE = "organization.j2"


def transform_organization(row: Row, transformer: FHIRTransformer) -> Row:
    data_dict = row.asDict(recursive=True)
    response_dict = {}
    response_dict["row_id"] = data_dict.get("row_id")

    # remove 'row_id', 'account_type'
    data_dict.pop("row_id")
    data_dict.pop("account_type")

    # assign organization type as 'prov'
    data_dict["type_code"] = TYPE_PROV_CODE
    data_dict["type_display"] = TYPE_PROV_DISPLAY

    # add active 'true'
    data_dict["active"] = "true"

    # render FHIR Organization resource
    resource = transformer.render_resource(ResourceType.Organization.value, data_dict)

    post_response = post_core_entity(resource, Organization)
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
    default="organization_migration_job",
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

        # load the organization data file
        log.warn("load organization data file path into dataframe")
        src_df = spark.read.options(header=True).csv(data_file_path)

        # add the row_id
        src_df = src_df.withColumn("row_id", f.expr("uuid()"))
        src_df.persist()

        data_df = (
            src_df.withColumn("tax_id", get_column(src_df, "FedTaxId"))
            .withColumn("sub_group_id", get_column(src_df, "SubGroupId"))
            .withColumn("group_npi", get_column(src_df, "GroupNPI"))
            .withColumn("name", get_column(src_df, "SubGroupName"))
            .withColumn("account_type", get_column(src_df, "Account_Type"))
            .withColumn("street_address_1", get_column(src_df, "StreetAddress1"))
            .withColumn("street_address_2", get_column(src_df, "StreetAddress2"))
            .withColumn("city", get_column(src_df, "City"))
            .withColumn("state", get_column(src_df, "State"))
            .withColumn("zip", get_column(src_df, "Zip"))
            .withColumn("phone_work", get_column(src_df, "Telephone"))
            .select(
                "row_id",
                "tax_id",
                "sub_group_id",
                "group_npi",
                "name",
                "account_type",
                "street_address_1",
                "street_address_2",
                "city",
                "state",
                "zip",
                "phone_work",
            )
        )
        # fill null with empty value
        data_df = data_df.fillna("")
        data_df.persist()

        # initialize fhirtransformer
        transformer = FHIRTransformer()
        transformer.load_template(ORG_JINJA_TEMPLATE)

        # transform each row into Organization resource and post with provider service
        organization_rdd = data_df.rdd.map(lambda row: transform_organization(row, transformer)).persist()
        organization_df = spark.createDataFrame(organization_rdd)
        organization_df.persist()

        organization_processed = organization_df.count()
        error_df = organization_df.filter(f.col("post_response") != "success")
        error_df.persist()
        organization_failure = error_df.count()

        if not error_df.isEmpty():
            # Attribute 'isEmpty' is new on spark 3.3.0. enables after spark version upgrade to 3.4.0
            # if not error_df.isEmpty():
            failure_pd_df = src_df.join(error_df, on="row_id", how="inner").toPandas()
            failure_pd_df.to_csv(error_file_path, index=False)

        log.warn(f"total number of organization processed: {organization_processed}")
        log.warn(f"total number of organization success: {(organization_processed - organization_failure)}")
        log.warn(f"total number of organization failure: {organization_failure}")
        log.warn(f"spark job {app_name} completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
