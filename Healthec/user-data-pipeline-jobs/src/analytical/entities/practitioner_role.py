import os

from fhirclient.resources.practitionerrole import PractitionerRole
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import BooleanType, StringType

from analytical.fhirschema.practitioner_role import PRACTITIONER_ROLE_SCHEMA
from dependencies.spark import flatten
from utils.api_client import get_sub_entities
from utils.constants import ANALYTICAL_PIPELINE_USER
from utils.utils import generate_random_string


def update_provider_practice_table(spark: SparkSession, delta_schema_location: str):
    practitioner_role_service_udf = f.udf(
        lambda resource_id, practitioner_id: get_sub_entities(resource_id, practitioner_id, entity=PractitionerRole)
    )

    def inner(df: DataFrame):
        # get patient resource using patient service with resource id
        resource_df = df.withColumn("resource", practitioner_role_service_udf(df["resourceId"], df["ownerId"])).select(
            "resource"
        )

        # flatten resource json content
        resource_df = resource_df.withColumn("resource", f.from_json(f.col("resource"), PRACTITIONER_ROLE_SCHEMA))
        flatten_df = flatten(resource_df)

        # construct analytical base table fields from resource dataframe
        provider_practice_df = (
            flatten_df.withColumn("provider_practice_fhir_id", f.col("id"))
            .withColumn(
                "external_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'RI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn("active", f.col("active").cast(BooleanType()))
            .withColumn("start_date", f.to_timestamp(f.col("period.start")))
            .withColumn("end_date", f.to_timestamp(f.col("period.end")))
            .withColumn("provider_fhir_id", f.col("practitioner").getField("id"))
            .withColumn("practice_fhir_id", f.col("organization").getField("id"))
            .withColumn(
                "role_code",
                f.col("code").getItem(0).getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "role_system",
                f.col("code").getItem(0).getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "role_desc",
                f.when(
                    f.col("code").getItem(0).getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("code").getItem(0).getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("code").getItem(0).getField("text")),
            )
            .withColumn("specialty_code", f.col("specialty").getItem(0).getField("coding").getItem(0).getField("code"))
            .withColumn(
                "specialty_systems", f.col("specialty").getItem(0).getField("coding").getItem(0).getField("system")
            )
            .withColumn(
                "specialty_desc",
                f.when(
                    f.col("specialty")
                    .getItem(0)
                    .getField("code")
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("specialty").getItem(0).getField("code").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("specialty").getItem(0).getField("code").getField("text")),
            )
            .withColumn("facility_fhir_id", f.col("location").getItem(0).getField("id"))
            .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )

        provider_practice_final_df = provider_practice_df.select(
            "provider_practice_fhir_id",
            "external_id",
            "active",
            "start_date",
            "end_date",
            "provider_fhir_id",
            "practice_fhir_id",
            "role_code",
            "role_system",
            "role_desc",
            "specialty_code",
            "specialty_system",
            "specialty_desc",
            "facility_fhir_id",
            "updated_user",
            "updated_ts",
        )

        # expose provider dataframe into temp view
        temp_view = f"provider_practice_updates_{generate_random_string()}"
        provider_practice_final_df.createOrReplaceTempView(temp_view)

        prov_delta_table_location = os.path.join(delta_schema_location, "provider_practice")

        # build and run merge query
        provider_practice_merge_query = f"""MERGE INTO delta.`{prov_delta_table_location}`
        USING {temp_view}
        ON delta.`{prov_delta_table_location}`.provider_practice_fhir_id = {temp_view}.provider_practice_fhir_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
        """

        spark.sql(provider_practice_merge_query)

        return provider_practice_final_df

    return inner
