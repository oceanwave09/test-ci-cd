import os

from fhirclient.resources.medication import Medication
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from analytical.fhirschema.medication import MEDICATION_SCHEMA
from dependencies.spark import flatten
from utils.api_client import get_core_entity
from utils.constants import ANALYTICAL_PIPELINE_USER
from utils.utils import generate_random_string


def update_medication_table(spark: SparkSession, delta_schema_location: str):
    medication_service_udf = f.udf(lambda resource_id: get_core_entity(resource_id, entity=Medication))

    def inner(df: DataFrame):
        # get medication resource using medication service.
        resource_df = df.withColumn("resource", medication_service_udf(df["resourceId"])).select("resource")

        # flatten resource json content
        resource_df = resource_df.withColumn("resource", f.from_json(f.col("resource"), MEDICATION_SCHEMA))
        flatten_df = flatten(resource_df)

        # construct analytical base table fields from resource dataframe
        medication_df = (
            flatten_df.withColumn("medication_fhir_id", f.col("id"))
            .withColumn(
                "external_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'RI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn(
                "medication_code",
                f.col("code").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "medication_code_system",
                f.col("code").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "medication_code_desc",
                f.when(
                    f.col("code").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("code").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("code").getField("text")),
            )
            .withColumn("status", f.col("status"))
            .withColumn("form_code", f.col("form").getField("coding").getItem(0).getField("code"))
            .withColumn("form_code_system", f.col("form").getField("coding").getItem(0).getField("system"))
            .withColumn(
                "form_code_desc",
                f.when(
                    f.col("form").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("form").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("form").getField("text")),
            )
            .withColumn("lot_number", f.col("batch.lotNumber"))
            .withColumn("expiration_date", f.to_timestamp(f.col("batch.expirationDate")))
            .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )

        medication_final_df = (
            medication_df.select(
                "medication_fhir_id",
                "external_id",
                "medication_code",
                "medication_code_system",
                "medication_code_desc",
                "status",
                "form_code",
                "form_code_system",
                "form_code_desc",
                "lot_number",
                "expiration_date",
                "updated_user",
                "updated_ts",
                f.explode_outer("ingredient").alias("ingredient"),
            )
            .select("*", "ingredient.*")
            .withColumn(
                "numerator_value",
                f.col("ingredient").getField("strength").getField("numerator").getField("value").cast(StringType()),
            )
            .withColumn(
                "numerator_unit", f.col("ingredient").getField("strength").getField("numerator").getField("unit")
            )
            .withColumn(
                "numerator_system", f.col("ingredient").getField("strength").getField("numerator").getField("system")
            )
            .withColumn(
                "numerator_code", f.col("ingredient").getField("strength").getField("numerator").getField("code")
            )
            .withColumn(
                "denominator_value",
                f.col("ingredient").getField("strength").getField("denominator").getField("value").cast(StringType()),
            )
            .withColumn(
                "denominator_unit", f.col("ingredient").getField("strength").getField("denominator").getField("unit")
            )
            .withColumn(
                "denominator_system",
                f.col("ingredient").getField("strength").getField("denominator").getField("system"),
            )
            .withColumn(
                "denominator_code", f.col("ingredient").getField("strength").getField("denominator").getField("code")
            )
            .drop("ingredient")
            .drop("strength")
            .withColumnRenamed("medication_code", "code")
            .withColumnRenamed("medication_code_system", "code_system")
            .withColumnRenamed("medication_code_desc", "code_desc")
        )

        # expose final dataframe into temp view
        temp_view = f"medication_updates_{generate_random_string()}"
        medication_final_df.createOrReplaceTempView(temp_view)

        delta_table_location = os.path.join(delta_schema_location, "medication")

        # build and run merge query
        merge_query = f"""MERGE INTO delta.`{delta_table_location}`
        USING {temp_view}
        ON delta.`{delta_table_location}`.medication_fhir_id = {temp_view}.medication_fhir_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
        """
        spark.sql(merge_query)

        return medication_final_df

    return inner
