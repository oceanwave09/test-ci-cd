import os
from string import Template

import click
from fhirclient.resources.location import Location
from fhirclient.resources.organization import Organization
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import Row
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, get_column, start_spark
from utils.api_client import match_core_entity, post_core_entity
from utils.enums import ResourceType
from utils.utils import exit_with_error, load_config

LOC_JINJA_TEMPLATE = "location.j2"


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
                    "system": "http://healthec.com/identifier/practice/sub_group_id",
                    "value": "$sub_group_id"
                }
            ]
        }
        """
    )
    return attributes_template.substitute(sub_group_id=value)


def transform_organization(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)

    # prepare matching attribute
    row_dict["attributes"] = (
        _get_organization_match_attributes(row_dict.get("SubGroupId")) if row_dict.get("SubGroupId") else ""
    )

    return Row(**row_dict)


def transform_location(row: Row, transformer: FHIRTransformer) -> Row:
    data_dict = row.asDict(recursive=True)
    response_dict = {}
    response_dict["row_id"] = data_dict.get("row_id")

    # remove 'row_id'
    data_dict.pop("row_id")

    # add mode as 'kind' and status as 'active'
    data_dict["mode"] = "kind"
    data_dict["status"] = "active"

    # render FHIR Location resource
    resource = transformer.render_resource(ResourceType.Location.value, data_dict)

    post_response = post_core_entity(resource, Location)
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
    default="location_migration_job",
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

        # apply transformation on practice data dataframe
        practice_data_df = src_df.select("SubGroupId").drop_duplicates()
        practice_rdd = practice_data_df.rdd.map(lambda row: transform_organization(row)).persist()
        practice_df = spark.createDataFrame(practice_rdd)
        practice_df.persist()

        # register organization service udf
        org_match_udf = f.udf(lambda df: match_core_entity(df, Organization))

        # match organization by tax id
        processed_practice_df = practice_df.withColumn(
            "organization_id", org_match_udf(practice_df["attributes"])
        ).select(["SubGroupId", "organization_id"])
        processed_practice_df.persist()

        # join and update organization id
        data_df = src_df.join(processed_practice_df, on="SubGroupId", how="left")

        data_df = (
            data_df.withColumn("group_npi", get_column(data_df, "Npi"))
            .withColumn("name", get_column(data_df, "FacilityName"))
            .withColumn("phone_work", get_column(data_df, "Telecom"))
            .withColumn("street_address_1", get_column(data_df, "Address1"))
            .withColumn("street_address_2", get_column(data_df, "Address2"))
            .withColumn("city", get_column(data_df, "City"))
            .withColumn("county_code", get_column(data_df, "County"))
            .withColumn("state", get_column(data_df, "State_Code"))
            .withColumn("zip", get_column(data_df, "Zip"))
            .select(
                "row_id",
                "group_npi",
                "name",
                "phone_work",
                "street_address_1",
                "street_address_2",
                "city",
                "county_code",
                "state",
                "zip",
                "organization_id",
            )
        )
        # fill null with empty value
        data_df = data_df.fillna("")
        data_df.persist()

        # initialize fhirtransformer
        transformer = FHIRTransformer()
        transformer.load_template(LOC_JINJA_TEMPLATE)

        location_rdd = data_df.rdd.map(lambda row: transform_location(row, transformer)).persist()
        location_df = spark.createDataFrame(location_rdd)
        location_df.persist()

        location_processed = location_df.count()
        error_df = location_df.filter(f.col("post_response") != "success")
        error_df.persist()
        location_failure = error_df.count()

        if not error_df.isEmpty():
            # Attribute 'isEmpty' is new on spark 3.3.0. enables after spark version upgrade to 3.4.0
            # if not error_df.isEmpty():
            failure_pd_df = src_df.join(error_df, on="row_id", how="inner").toPandas()
            failure_pd_df.to_csv(error_file_path, index=False)

        log.warn(f"total number of location processed: {location_processed}")
        log.warn(f"total number of location success: {(location_processed - location_failure)}")
        log.warn(f"total number of location failure: {location_failure}")
        log.warn(f"spark job {app_name} completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
