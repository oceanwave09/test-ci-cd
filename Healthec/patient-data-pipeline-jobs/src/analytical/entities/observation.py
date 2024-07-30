import os

from fhirclient.resources.observation import Observation
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from analytical.fhirschema.observation import OBSERVATION_SCHEMA
from dependencies.spark import flatten
from utils.api_client import get_patient_sub_entities
from utils.constants import ANALYTICAL_PIPELINE_USER
from utils.utils import generate_random_string


def update_observation_table(spark: SparkSession, delta_schema_location: str):
    observation_service_udf = f.udf(
        lambda resource_id, patient_id: get_patient_sub_entities(resource_id, patient_id, entity=Observation)
    )

    def inner(df: DataFrame):
        # get observation sub resource using observation service.
        resource_df = df.withColumn("resource", observation_service_udf(df["resourceId"], df["ownerId"])).select(
            "resource"
        )
        # flatten resource json content
        resource_df = resource_df.withColumn("resource", f.from_json(f.col("resource"), OBSERVATION_SCHEMA))
        flatten_df = flatten(resource_df)

        # construct analytical base table fields from resource dataframe
        observation_df = (
            flatten_df.withColumn("observation_fhir_id", f.col("id"))
            .withColumn(
                "external_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'RI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn(
                "external_system",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'RI')")
                .getItem(0)
                .getField("system"),
            )
            .withColumn("status", f.col("status"))
            .withColumn(
                "category_code",
                f.col("category").getItem(0).getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "category_system",
                f.col("category").getItem(0).getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "category_desc",
                f.when(
                    f.col("category").getItem(0).getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("category").getItem(0).getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("category").getItem(0).getField("text")),
            )
            .withColumn(
                "observation_code",
                f.col("code").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "observation_code_system",
                f.col("code").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "observation_code_desc",
                f.when(
                    f.col("code").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("code").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("code").getField("text")),
            )
            .withColumn("patient_fhir_id", f.col("subject.id"))
            .withColumn("encounter_fhir_id", f.col("encounter.id"))
            .withColumn("effective_date_time", f.to_timestamp(f.col("effectiveDateTime")))
            .withColumn(
                "effective_start_date",
                f.when(
                    f.col("effectivePeriod.start").isNotNull(), f.to_timestamp(f.col("effectivePeriod.start"))
                ).otherwise(f.to_timestamp(f.col("effectiveDateTime"))),
            )
            .withColumn("effective_end_date", f.to_timestamp(f.col("effectivePeriod.end")))
            .withColumn("performer_fhir_id", f.col("performer").getItem(0).getField("id"))
            .withColumn(
                "value",
                f.when(
                    f.col("valueQuantity").getField("value").isNotNull(),
                    f.col("valueQuantity").getField("value").cast(StringType()),
                )
                .when(
                    f.col("valueCodeableConcept").getField("coding").getItem(0).getField("code").isNotNull(),
                    f.col("valueCodeableConcept").getField("coding").getItem(0).getField("code"),
                )
                .when(f.col("valueString").isNotNull(), f.col("valueString")),
            )
            .withColumn(
                "value_code",
                f.when(f.col("valueQuantity").getField("code").isNotNull(), f.col("valueQuantity").getField("code")),
            )
            .withColumn(
                "value_unit",
                f.when(f.col("valueQuantity").getField("unit").isNotNull(), f.col("valueQuantity").getField("unit")),
            )
            .withColumn(
                "value_system", f.col("valueCodeableConcept").getField("coding").getItem(0).getField("system")
            )
            .withColumn(
                "value_desc",
                f.when(
                    f.col("valueCodeableConcept").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("valueCodeableConcept").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("valueCodeableConcept").getField("text")),
            )
            .withColumn(
                "interpretation_code",
                f.col("interpretation").getItem(0).getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "interpretation_system",
                f.col("interpretation").getItem(0).getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "interpretation_desc",
                f.when(
                    f.col("interpretation").getItem(0).getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("interpretation").getItem(0).getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("interpretation").getItem(0).getField("text")),
            )
            .withColumn(
                "body_site_code",
                f.col("bodySite").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "body_site_system",
                f.col("bodySite").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "body_site_desc",
                f.when(
                    f.col("bodySite").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("bodySite").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("bodySite").getField("text")),
            )
            .withColumn(
                "method_code",
                f.col("method").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "method_system",
                f.col("method").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "method_desc",
                f.when(
                    f.col("method").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("method").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("method").getField("text")),
            )
            .withColumn(
                "reference_range_low",
                f.when(
                    f.col("referenceRange").getItem(0).getField("low").getField("value").isNotNull(),
                    f.col("referenceRange").getItem(0).getField("low").getField("value").cast(StringType()),
                ),
            )
            .withColumn(
                "reference_range_high",
                f.when(
                    f.col("referenceRange").getItem(0).getField("high").getField("value").isNotNull(),
                    f.col("referenceRange").getItem(0).getField("high").getField("value").cast(StringType()),
                ),
            )
            .withColumn(
                "reference_range",
                f.when(
                    f.col("referenceRange").getItem(0).getField("text").isNotNull(),
                    f.col("referenceRange").getItem(0).getField("text").cast(StringType()),
                ),
            )
            .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
            .select(
                "observation_fhir_id",
                "external_id",
                "external_system",
                "status",
                "category_code",
                "category_system",
                "category_desc",
                "observation_code",
                "observation_code_system",
                "observation_code_desc",
                "patient_fhir_id",
                "encounter_fhir_id",
                "effective_date_time",
                "effective_start_date",
                "effective_end_date",
                "performer_fhir_id",
                "value",
                "value_code",
                "value_unit",
                "value_system",
                "value_desc",
                "interpretation_code",
                "interpretation_system",
                "interpretation_desc",
                "body_site_code",
                "body_site_system",
                "body_site_desc",
                "method_code",
                "method_system",
                "method_desc",
                "reference_range_low",
                "reference_range_high",
                "reference_range",
                "updated_user",
                "updated_ts",
                f.explode_outer("component").alias("component"),
            )
            .select("*", "component.*")
            .withColumn(
                "component_code",
                f.col("code").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "component_code_system",
                f.col("code").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "component_code_desc",
                f.when(
                    f.col("code").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("code").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("code").getField("text")),
            )
            .withColumn(
                "component_value",
                f.when(
                    f.col("valueQuantity").getField("value").isNotNull(),
                    f.col("valueQuantity").getField("value").cast(StringType()),
                )
                .when(
                    f.col("valueCodeableConcept").getField("coding").getItem(0).getField("code").isNotNull(),
                    f.col("valueCodeableConcept").getField("coding").getItem(0).getField("code"),
                )
                .when(f.col("valueString").isNotNull(), f.col("valueString")),
            )
            .withColumn(
                "component_value_code",
                f.when(f.col("valueQuantity").getField("code").isNotNull(), f.col("valueQuantity").getField("code")),
            )
            .withColumn(
                "component_value_unit",
                f.when(f.col("valueQuantity").getField("unit").isNotNull(), f.col("valueQuantity").getField("unit")),
            )
            .withColumn(
                "component_value_system",
                f.col("valueCodeableConcept").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "component_value_desc",
                f.when(
                    f.col("valueCodeableConcept").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("valueCodeableConcept").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("valueCodeableConcept").getField("text")),
            )
            .withColumn(
                "component_interpretation_code",
                f.col("interpretation").getItem(0).getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "component_interpretation_system",
                f.col("interpretation").getItem(0).getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "component_interpretation_desc",
                f.when(
                    f.col("interpretation").getItem(0).getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("interpretation").getItem(0).getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("interpretation").getItem(0).getField("text")),
            )
            .withColumn(
                "component_reference_range_low",
                f.when(
                    f.col("referenceRange").getItem(0).getField("low").getField("value").isNotNull(),
                    f.col("referenceRange").getItem(0).getField("low").getField("value").cast(StringType()),
                ),
            )
            .withColumn(
                "component_reference_range_high",
                f.when(
                    f.col("referenceRange").getItem(0).getField("high").getField("value").isNotNull(),
                    f.col("referenceRange").getItem(0).getField("high").getField("value").cast(StringType()),
                ),
            )
            .withColumn(
                "component_reference_range",
                f.when(
                    f.col("referenceRange").getItem(0).getField("text").isNotNull(),
                    f.col("referenceRange").getItem(0).getField("text").cast(StringType()),
                ),
            )
            .drop(
                "component",
                "code",
                "valueString",
                "valueBoolean",
                "valueQuantity",
                "valueCodeableConcept",
                "dataAbsentReason",
                "interpretation",
                "referenceRange",
            )
            .withColumnRenamed("observation_code", "code")
            .withColumnRenamed("observation_code_system", "code_system")
            .withColumnRenamed("observation_code_desc", "code_desc")
        )

        if not observation_df.isEmpty():
            # expose final dataframe into temp view
            temp_view = f"observation_updates_{generate_random_string()}"
            observation_df.createOrReplaceTempView(temp_view)

            delta_table_location = os.path.join(delta_schema_location, "observation")

            # build and run merge query
            merge_query = f"""MERGE INTO delta.`{delta_table_location}`
            USING {temp_view}
            ON delta.`{delta_table_location}`.observation_fhir_id = {temp_view}.observation_fhir_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *;
            """
            spark.sql(merge_query)

        return observation_df

    return inner
