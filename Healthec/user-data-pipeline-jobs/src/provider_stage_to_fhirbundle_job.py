import json
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as f

from utils.constants import (
    ACTIVE_STATUS,
    ORG_ROLE_CODE,
    ORG_ROLE_DISPLAY,
    TYPE_PAY_CODE,
    TYPE_PAY_DISPLAY,
    TYPE_PROV_CODE,
    TYPE_PROV_DISPLAY,
)
from utils.enums import ResourceType
from utils.transformation import transform_date_time


def transform_extenstion(extentions):
    transformed_data = []
    for key, value in extentions.items():
        url = f"PRACTICE_EXTENSTION_SYSTEM/{key}"
        transformed_data.append({"url": url, "value": value})
    return transformed_data

ORG_JINJA_TEMPLATE = "organization.j2"
ORG_AFF_JINJA_TEMPLATE = "organization_affiliation.j2"
PRC_JINJA_TEMPLATE = "practitioner.j2"
PRC_RL_JINJA_TEMPLATE = "practitioner_role.j2"
LOC_JINJA_TEMPLATE = "location.j2"


def _transform_resource(data_dict: dict, template: str, resource_type, transformer: FHIRTransformer) -> dict:
    transformer.load_template(template)
    return json.loads(transformer.render_resource(resource_type, data_dict))


def fhir_mapper_df(df: DataFrame) -> DataFrame:
    df = df.select(
        "bundle_id",
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("name"),
                f.lit("tax_id"),
                f.lit("group_npi"),
                f.lit("source_id"),
                f.lit("active"),
                f.lit("type_code"),
                f.lit("type_display"),
            ),
            f.array(
                f.col("practice_organization_internal_id"),
                f.col("provider_org_name"),
                f.col("provider_org_tin"),
                f.col("provider_org_group_npi"),
                f.col("provider_org_internal_id"),
                f.lit(ACTIVE_STATUS),
                f.lit(TYPE_PROV_CODE),
                f.lit(TYPE_PROV_DISPLAY),
            ),
        ).alias("practice_rsc"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("source_id"),
                f.lit("name"),
                f.lit("active"),
                f.lit("type_code"),
                f.lit("type_display"),
            ),
            f.array(
                f.col("payer_organization_internal_id"),
                f.col("insurance_company_name"),
                f.col("insurance_company_id"),
                f.lit(ACTIVE_STATUS),
                f.lit(TYPE_PAY_CODE),
                f.lit(TYPE_PAY_DISPLAY),
            ),
        ).alias("payer_rsc"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("npi"),
                f.lit("source_id"),
                f.lit("firstname"),
                f.lit("lastname"),
                f.lit("middleinitials"),
                f.lit("qualification_text"),
                f.lit("phone_work"),
                f.lit("phone_mobile"),
                f.lit("email"),
                f.lit("active"),
                f.lit("extentions"),
            ),
            f.array(
                f.col("practitioner_internal_id"),
                f.col("practitioner_npi"),
                f.col("practitioner_internal_id"),
                f.col("practitioner_first_name"),
                f.col("practitioner_last_name"),
                f.col("practitioner_middle_initial"),
                f.col("practitioner_degree"),
                f.col("practitioner_phone_work"),
                f.col("practitioner_phone_mobile"),
                f.col("practitioner_email"),
                f.lit(ACTIVE_STATUS),
                f.col("extentions"),
            ),
        ).alias("provider_rsc"),
        f.create_map(
            f.lit("specialties"),
            f.when(
                f.col("practitioner_specialty_code") != "",
                f.array(
                    f.struct(
                        f.col("practitioner_specialty_code").alias("code"),
                        f.col("practitioner_specialty_description").alias("display"),
                    )
                ),
            ),
        ).alias("practitioner_specialty"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("organization_id"),
                f.lit("practitioner_id"),
                f.lit("role_code"),
                f.lit("period_start_date"),
                f.lit("period_end_date"),
                f.lit("active"),
            ),
            f.array(
                f.col("practitioner_role_internal_id"),
                f.col("practice_organization_internal_id"),
                f.col("practitioner_internal_id"),
                f.col("practitioner_taxonomy_code"),
                f.col("practitioner_effective_date"),
                f.col("practitioner_termination_date"),
                f.lit(ACTIVE_STATUS),
            ),
        ).alias("provider_role_rsc"),
        f.map_from_arrays(
            f.array(
                f.lit("internal_id"),
                f.lit("organization_id"),
                f.lit("participating_organization_id"),
                f.lit("source_id"),
                f.lit("role_code"),
                f.lit("role_display"),
                f.lit("active"),
            ),
            f.array(
                f.col("org_affiliation_internal_id"),
                f.col("payer_organization_internal_id"),
                f.col("practice_organization_internal_id"),
                f.concat_ws("_", f.col("insurance_company_id"), f.col("provider_org_tin")),
                f.lit(ORG_ROLE_CODE),
                f.lit(ORG_ROLE_DISPLAY),
                f.lit(ACTIVE_STATUS),
            ),
        ).alias("organization_affiliation_rsc"),
    )

    return df


def render_resources(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)
    resources = []

    get_extensions = transform_extenstion(row_dict.get("additional_properties"))
    get_provider_rsc = row_dict.get("provider_rsc", {})
    get_provider_rsc.update({"extensions": get_extensions})

    # render resources
    resources.append(
        _transform_resource(
            data_dict=row_dict.get("practice_rsc"),
            resource_type=ResourceType.Organization.value,
            template=ORG_JINJA_TEMPLATE,
            transformer=transformer,
        )
    )

    resources.append(
        _transform_resource(
            data_dict=row_dict.get("payer_rsc"),
            resource_type=ResourceType.Organization.value,
            template=ORG_JINJA_TEMPLATE,
            transformer=transformer,
        )
    )

    resources.append(
        _transform_resource(
            data_dict=get_provider_rsc,
            resource_type=ResourceType.Practitioner.value,
            template=PRC_JINJA_TEMPLATE,
            transformer=transformer,
        )
    )

    # add practitioner speciality to provider resource
    practitioner_specialty = (
        row_dict.get("practitioner_specialty") if row_dict.get("practitioner_specialty").get("specialties") else []
    )
    provider_role_rsc = row_dict.get("provider_role_rsc", {})
    provider_role_rsc.update(practitioner_specialty)
    resources.append(
        _transform_resource(
            data_dict=provider_role_rsc,
            resource_type=ResourceType.PractitionerRole.value,
            template=PRC_RL_JINJA_TEMPLATE,
            transformer=transformer,
        )
    )

    resources.append(
        _transform_resource(
            data_dict=row_dict.get("organization_affiliation_rsc"),
            resource_type=ResourceType.OrganizationAffiliation.value,
            template=ORG_AFF_JINJA_TEMPLATE,
            transformer=transformer,
        )
    )

    return Row(
        **{
            "resource_bundle": json.dumps(
                {
                    "resourceType": "Bundle",
                    "id": row_dict.get("bundle_id"),
                    "type": "batch",
                    "entry": resources,
                }
            )
        }
    )



def main():

    spark = SparkSession.builder.master("local[1]") \
                        .appName('SparkByExamples.com') \
                        .getOrCreate()

    try:
        # construct metadata
        # metadata = {
        #     "file_tenant": "file_tenant",
        #     "file_source": "file_source",
        #     "resource_type": "resource_type",
        #     "file_batch_id": "file_batch_id",
        #     "src_file_name": "src_file_name",
        # }

        data_df = spark.read.option("header", True).csv("/home/ubuntu/Downloads/provider_samp.csv")
        data_df.printSchema()

        # data_df = (
        #     data_df.withColumn("practice_organization_internal_id", f.expr("uuid()"))
        #     .withColumn("payer_organization_internal_id", f.expr("uuid()"))
        #     .withColumn("practitioner_internal_id", f.expr("uuid()"))
        #     .withColumn("practitioner_role_internal_id", f.expr("uuid()"))
        #     .withColumn("org_affiliation_internal_id", f.expr("uuid()"))
        #     .withColumn("practitioner_effective_date", transform_date_time(f.col("practitioner_effective_date")))
        #     .withColumn("practitioner_termination_date", transform_date_time(f.col("practitioner_termination_date")))
        #     .withColumn("bundle_id", f.expr("uuid()"))
        # )
        # fhir mapper
        # data_df = fhir_mapper_df(data_df)

        # # processing row wise operation
        # transformer = FHIRTransformer()
        # data_rdd = data_df.rdd.map(lambda row: render_resources(row, transformer))
        # resources_df = spark.createDataFrame(data_rdd)
        # resources_df.write.mode("overwrite").text("./fhir_bundle_temp_path")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
