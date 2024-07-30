import os

from fhirclient.resources.practitioner import Practitioner
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import BooleanType, StringType

from analytical.fhirschema.practitioner import PRACTITIONER_SCHEMA
from dependencies.spark import flatten
from utils.api_client import get_core_entity
from utils.constants import ANALYTICAL_PIPELINE_USER
from utils.utils import generate_random_string


def update_provider_table(spark: SparkSession, delta_schema_location: str):
    provider_service_udf = f.udf(lambda resource_id: get_core_entity(resource_id, Practitioner))

    def inner(df: DataFrame):
        # get patient resource using patient service with resource id
        resource_df = df.withColumn("resource", provider_service_udf(df["resourceId"])).select("resource")

        # flatten resource json content
        resource_df = resource_df.withColumn("resource", f.from_json(f.col("resource"), PRACTITIONER_SCHEMA))
        flatten_df = flatten(resource_df)

        # construct analytical base table fields from resource dataframe
        provider_df = (
            flatten_df.withColumn("provider_fhir_id", f.col("id"))
            .withColumn(
                "external_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'RI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn(
                "ssn",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'SS')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn(
                "tin",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'TAX')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn(
                "npi",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'NPI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn(
                "state_license",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'SL')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn("active", f.col("active").cast(BooleanType()))
            .withColumn("city", f.col("address").getItem(0).getField("city"))
            .withColumn("county", f.col("address").getItem(0).getField("district"))
            .withColumn("state", f.col("address").getItem(0).getField("state"))
            .withColumn("zip", f.col("address").getItem(0).getField("postalCode"))
            .withColumn("country", f.col("address").getItem(0).getField("country"))
            .withColumn("gender", f.col("gender"))
            .withColumn("birth_date", f.trunc(f.col("birthDate"), "month"))
            .withColumn(
                "qualification_code", f.col("qualification").getItem(0).getField("coding").getItem(0).getField("code")
            )
            .withColumn(
                "qualification_system",
                f.col("qualification").getItem(0).getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "qualification_desc",
                f.when(
                    f.col("qualification")
                    .getItem(0)
                    .getField("code")
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("qualification").getItem(0).getField("code").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("qualification").getItem(0).getField("code").getField("text")),
            )
            .withColumn("qualification_desc", f.expr("transform(qualification, q -> q.code.coding[0].display)"))
            .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )

        provider_final_df = provider_df.select(
            "provider_fhir_id",
            "external_id",
            "ssn",
            "tin",
            "npi",
            "state_license",
            "active",
            "city",
            "county",
            "state",
            "zip",
            "country",
            "gender",
            "birth_date",
            "qualification_code",
            "qualification_system",
            "qualification_desc",
            "updated_user",
            "updated_ts",
        )

        # expose provider dataframe into temp view
        temp_view = f"provider_updates_{generate_random_string()}"
        provider_final_df.createOrReplaceTempView(temp_view)

        prov_delta_table_location = os.path.join(delta_schema_location, "provider")

        # build and run merge query
        provider_merge_query = f"""MERGE INTO delta.`{prov_delta_table_location}`
        USING {temp_view}
        ON delta.`{prov_delta_table_location}`.provider_fhir_id = {temp_view}.provider_fhir_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
        """

        spark.sql(provider_merge_query)

        return provider_final_df

    return inner
