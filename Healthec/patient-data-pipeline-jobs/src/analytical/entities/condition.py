import os

from fhirclient.resources.condition import Condition
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from analytical.fhirschema.condition import CONDITION_SCHEMA
from dependencies.spark import flatten
from utils.api_client import get_patient_sub_entities
from utils.constants import ANALYTICAL_PIPELINE_USER
from utils.utils import generate_random_string


def update_condition_table(spark: SparkSession, delta_schema_location: str):
    condition_service_udf = f.udf(
        lambda resource_id, patient_id: get_patient_sub_entities(resource_id, patient_id, entity=Condition)
    )

    def inner(df: DataFrame):
        # get condition resource using condition service.
        resource_df = df.withColumn("resource", condition_service_udf(df["resourceId"], df["ownerId"])).select(
            "resource"
        )

        # flatten resource json content
        resource_df = resource_df.withColumn("resource", f.from_json(f.col("resource"), CONDITION_SCHEMA))
        flatten_df = flatten(resource_df)

        # construct analytical base table fields from resource dataframe
        condition_df = (
            flatten_df.withColumn("condition_fhir_id", f.col("id"))
            .withColumn(
                "external_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'RI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn("clinical_status", f.col("clinicalStatus").getField("coding").getItem(0).getField("code"))
            .withColumn(
                "clinical_status_system", f.col("clinicalStatus").getField("coding").getItem(0).getField("system")
            )
            .withColumn(
                "clinical_status_desc",
                f.when(
                    f.col("clinicalStatus").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("clinicalStatus").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("clinicalStatus").getField("text")),
            )
            .withColumn(
                "verification_status", f.col("verificationStatus").getField("coding").getItem(0).getField("code")
            )
            .withColumn(
                "verification_status_system",
                f.col("verificationStatus").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "verification_status_desc",
                f.when(
                    f.col("verificationStatus").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("verificationStatus").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("verificationStatus").getField("text")),
            )
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
                "severity_code",
                f.col("severity").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "severity_code_system",
                f.col("severity").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "severity_code_desc",
                f.when(
                    f.col("severity").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("severity").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("severity").getField("text")),
            )
            .withColumn(
                "condition_code",
                f.col("code").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "condition_code_system",
                f.col("code").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "condition_code_desc",
                f.when(
                    f.col("code").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("code").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("code").getField("text")),
            )
            .withColumn(
                "bodysite_code",
                f.col("bodySite").getItem(0).getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "bodysite_system",
                f.col("bodySite").getItem(0).getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "bodysite_desc",
                f.when(
                    f.col("bodySite").getItem(0).getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("bodySite").getItem(0).getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("bodySite").getItem(0).getField("text")),
            )
            .withColumn("patient_fhir_id", f.col("subject.id"))
            .withColumn("encounter_fhir_id", f.col("encounter.id"))
            .withColumn(
                "onset_start_date",
                f.when(f.col("onsetPeriod.start").isNotNull(), f.to_timestamp(f.col("onsetPeriod.start"))).otherwise(
                    f.to_timestamp(f.col("onsetDateTime"))
                ),
            )
            .withColumn("onset_end_date", f.to_timestamp(f.col("onsetPeriod.end")))
            .withColumn(
                "abatement_start_date",
                f.when(
                    f.col("abatementPeriod.start").isNotNull(), f.to_timestamp(f.col("abatementPeriod.start"))
                ).otherwise(f.to_timestamp(f.col("abatementDateTime"))),
            )
            .withColumn("abatement_end_date", f.to_timestamp(f.col("abatementPeriod.end")))
            .withColumn("recorded_date", f.to_timestamp(f.col("recordedDate")))
            .withColumn(
                "provider_function_code",
                f.col("participant").getItem(0).getField("function").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "provider_function_system",
                f.col("participant").getItem(0).getField("function").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "provider_function_desc",
                f.when(
                    f.col("participant")
                    .getItem(0)
                    .getField("function")
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("participant")
                    .getItem(0)
                    .getField("function")
                    .getField("coding")
                    .getItem(0)
                    .getField("display"),
                ).otherwise(f.col("participant").getItem(0).getField("function").getField("text")),
            )
            .withColumn("provider_fhir_id", f.col("participant").getItem(0).getField("actor").getField("id"))
            .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )

        condition_final_df = (
            condition_df.select(
                "condition_fhir_id",
                "external_id",
                "clinical_status",
                "clinical_status_system",
                "clinical_status_desc",
                "verification_status",
                "verification_status_system",
                "verification_status_desc",
                "category_code",
                "category_system",
                "category_desc",
                "severity_code",
                "severity_code_system",
                "severity_code_desc",
                "condition_code",
                "condition_code_system",
                "condition_code_desc",
                "bodysite_code",
                "bodysite_system",
                "bodysite_desc",
                "patient_fhir_id",
                "encounter_fhir_id",
                "onset_start_date",
                "onset_end_date",
                "abatement_start_date",
                "abatement_end_date",
                "recorded_date",
                "provider_function_code",
                "provider_function_system",
                "provider_function_desc",
                "provider_fhir_id",
                "updated_user",
                "updated_ts",
            )
            .withColumnRenamed("condition_code", "code")
            .withColumnRenamed("condition_code_desc", "code_desc")
            .withColumnRenamed("condition_code_system", "code_system")
        )

        # expose final dataframe into temp view
        temp_view = f"condition_updates_{generate_random_string()}"
        condition_final_df.createOrReplaceTempView(temp_view)

        delta_table_location = os.path.join(delta_schema_location, "condition")

        # build and run merge query
        merge_query = f"""MERGE INTO delta.`{delta_table_location}`
        USING {temp_view}
        ON delta.`{delta_table_location}`.condition_fhir_id = {temp_view}.condition_fhir_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
        """
        spark.sql(merge_query)

        return condition_final_df

    return inner
