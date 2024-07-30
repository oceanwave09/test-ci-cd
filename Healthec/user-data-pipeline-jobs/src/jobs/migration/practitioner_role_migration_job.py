import os
from string import Template

import click
from fhirclient.resources.organization import Organization
from fhirclient.resources.practitioner import Practitioner
from fhirclient.resources.practitionerrole import PractitionerRole
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import Row
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, get_column, start_spark
from utils.api_client import match_core_entity, post_sub_entity
from utils.enums import ResourceType
from utils.utils import exit_with_error, load_config

PRC_ROL_JINJA_TEMPLATE = "practitioner_role.j2"


def _get_match_attributes_org(value: str) -> str:
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


def _get_match_attributes_pract(value: str) -> str:
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
    row_dict["attributes"] = _get_match_attributes_org(row_dict.get("SubGroupId")) if row_dict.get("SubGroupId") else ""

    return Row(**row_dict)


def transform_practitioner(row: Row) -> Row:
    row_dict = row.asDict(recursive=True)

    # prepare matching attribute
    row_dict["attributes"] = (
        _get_match_attributes_pract(row_dict.get("NationalProviderId")) if row_dict.get("NationalProviderId") else ""
    )

    return Row(**row_dict)


def transform_practitioner_role(row: Row, transformer: FHIRTransformer) -> Row:
    data_dict = row.asDict(recursive=True)
    response_dict = {}
    response_dict["row_id"] = data_dict.get("row_id")

    data_dict.pop("row_id")

    # render FHIR PractitionerRole resource
    resource = transformer.render_resource(ResourceType.PractitionerRole.value, data_dict)

    post_response = post_sub_entity(resource, PractitionerRole, data_dict["practitioner_id"], "Practitioner")
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
    default="practitioner_role_migration_job",
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

        # load the practitioner role data file
        log.warn("load practitioner_role data file path into dataframe")
        src_df = spark.read.options(header=True).csv(data_file_path)
        # add the row_id
        src_df = src_df.withColumn("row_id", f.expr("uuid()"))
        src_df.persist()

        # apply transformation on practitioner role data dataframe
        organization_df = src_df.select("SubGroupId").drop_duplicates()
        organization_rdd = organization_df.rdd.map(lambda row: transform_organization(row)).persist()
        organization_df = spark.createDataFrame(organization_rdd)
        organization_df.persist()

        # register organization service udf
        org_service_udf = f.udf(lambda df: match_core_entity(df, Organization))

        # match organization by tax id
        processed_organization_df = organization_df.withColumn(
            "organization_id", org_service_udf(organization_df["attributes"])
        ).select(["SubGroupId", "organization_id"])
        processed_organization_df.persist()

        # join and update organization id
        data_df = src_df.join(processed_organization_df, on="SubGroupId", how="left")

        # apply transformation on practitioner data dataframe
        practitioner_df = data_df.select("NationalProviderId").drop_duplicates()
        practitioner_rdd = practitioner_df.rdd.map(lambda row: transform_practitioner(row)).persist()
        practitioner_df = spark.createDataFrame(practitioner_rdd)
        practitioner_df.persist()

        # register practitioner service udf
        prc_service_udf = f.udf(lambda df: match_core_entity(df, Practitioner))

        # match practitioner by tax id
        processed_practitioner_df = practitioner_df.withColumn(
            "practitioner_id", prc_service_udf(practitioner_df["attributes"])
        ).select(["NationalProviderId", "practitioner_id"])
        processed_practitioner_df.persist()

        # join and update practitioner id
        data_df = data_df.join(processed_practitioner_df, on="NationalProviderId", how="left")

        data_df = (
            data_df.withColumn("role_code", get_column(data_df, "Specility"))
            .withColumn("specialty_code", get_column(data_df, "TaxonomyCode"))
            .withColumn("active", f.when(get_column(data_df, "Status") == "Y", f.lit("true")).otherwise("false"))
            .select(
                "row_id",
                "role_code",
                "specialty_code",
                "practitioner_id",
                "organization_id",
                "active",
            )
        )
        # fill null with empty value
        data_df = data_df.fillna("")
        data_df.persist()

        # initialize fhirtransformer
        transformer = FHIRTransformer()
        transformer.load_template(PRC_ROL_JINJA_TEMPLATE)

        practitioner_role_rdd = data_df.rdd.map(lambda row: transform_practitioner_role(row, transformer)).persist()
        practitioner_role_df = spark.createDataFrame(practitioner_role_rdd)
        practitioner_role_df.persist()

        practitioner_role_processed = practitioner_role_df.count()
        error_df = practitioner_role_df.filter(f.col("post_response") != "success")
        error_df.persist()
        practitioner_role_failure = error_df.count()

        if not error_df.isEmpty():
            # Attribute 'isEmpty' is new on spark 3.3.0. enables after spark version upgrade to 3.4.0
            # if not error_df.isEmpty():
            failure_pd_df = src_df.join(error_df, on="row_id", how="inner").toPandas()
            failure_pd_df.to_csv(error_file_path, index=False)

        log.warn(f"total number of practitioner_role processed: {practitioner_role_processed}")
        log.warn(
            f"total number of practitioner_role success: {(practitioner_role_processed - practitioner_role_failure)}"
        )
        log.warn(f"total number of practitioner_role failure: {practitioner_role_failure}")
        log.warn(f"spark job {app_name} completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
