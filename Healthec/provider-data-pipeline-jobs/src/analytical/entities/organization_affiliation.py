import os

from fhirclient.resources.organizationaffiliation import OrganizationAffiliation
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import BooleanType, StringType

from analytical.fhirschema.organization_affiliation import ORGANIZATION_AFFILIATION_SCHEMA
from dependencies.spark import flatten
from utils.api_client import get_core_entity
from utils.constants import ANALYTICAL_PIPELINE_USER
from utils.utils import generate_random_string


def update_payer_practice_table(spark: SparkSession, delta_schema_location: str):
    org_affiliation_service_udf = f.udf(lambda resource_id: get_core_entity(resource_id, entity=OrganizationAffiliation))

    def inner(df: DataFrame):
        # get organization resource using organization service with resource id
        resource_df = df.withColumn("resource", org_affiliation_service_udf(df["resourceId"])).select("resource")

        # flatten resource json content
        resource_df = resource_df.withColumn("resource", f.from_json(f.col("resource"), ORGANIZATION_AFFILIATION_SCHEMA))
        flatten_df = flatten(resource_df)

        # construct analytical base table fields from resource dataframe
        org_affiliation_df = (
            flatten_df.withColumn("payer_fhir_id", f.col("organization").getField("id"))
            .withColumn(
                "external_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'RI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn("practice_fhir_id", f.col("participatingOrganization").getField("id"))
            .withColumn("taxonomy_code", f.col("code").getItem(0).getField("coding").getItem(0).getField("code"))
            .withColumn("taxonomy_system", f.col("code").getItem(0).getField("coding").getItem(0).getField("system"))
            .withColumn(
                "taxonomy_desc",
                f.when(
                    f.col("code").getItem(0).getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("code").getItem(0).getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("code").getItem(0).getField("text")),
            )
            .withColumn("specialty_code", f.col("specialty").getItem(0).getField("coding").getItem(0).getField("code"))
            .withColumn(
                "specialty_system", f.col("specialty").getItem(0).getField("coding").getItem(0).getField("system")
            )
            .withColumn(
                "specialty_desc",
                f.when(
                    f.col("specialty").getItem(0).getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("specialty").getItem(0).getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("specialty").getItem(0).getField("text")),
            )
            .withColumn("location_fhir_id", f.col("location").getField("id"))
            .withColumn("active", f.col("active").cast(BooleanType()))
            .withColumn("effective_start", f.col("period").getField("start"))
            .withColumn("effective_end", f.col("period").getField("end"))
            .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )
        org_affiliation_df.persist()

        payer_practice_df = org_affiliation_df.select(
            "external_id",
            "payer_fhir_id",
            "practice_fhir_id",
            "taxonomy_code",
            "taxonomy_system",
            "taxonomy_desc",
            "specialty_code",
            "specialty_system",
            "specialty_desc",
            "location_fhir_id",
            "active",
            "effective_start",
            "effective_end",
            "updated_user",
            "updated_ts",
        )

        # expose practice dataframe into temp view
        temp_view = f"payer_practice_updates_{generate_random_string()}"
        payer_practice_df.createOrReplaceTempView(temp_view)

        payer_pract_delta_table_location = os.path.join(delta_schema_location, "organization_affiliation")

        # build and run merge query
        practice_merge_query = f"""MERGE INTO delta.`{payer_pract_delta_table_location}`
        USING {temp_view}
        ON delta.`{payer_pract_delta_table_location}`.practice_fhir_id = {temp_view}.practice_fhir_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
        """
        spark.sql(practice_merge_query)

        return payer_practice_df

    return inner
