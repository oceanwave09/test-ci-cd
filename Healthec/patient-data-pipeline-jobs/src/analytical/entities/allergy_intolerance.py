import os

from fhirclient.resources.allergyintolerance import AllergyIntolerance
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from analytical.fhirschema.allergy_intolerance import ALLERGY_INTOLERANCE_SCHEMA
from dependencies.spark import flatten
from utils.api_client import get_patient_sub_entities
from utils.constants import ANALYTICAL_PIPELINE_USER
from utils.utils import generate_random_string


def update_allergy_intolerance_table(spark: SparkSession, delta_schema_location: str):
    allergy_intolerance_service_udf = f.udf(
        lambda resource_id, patient_id: get_patient_sub_entities(resource_id, patient_id, entity=AllergyIntolerance)
    )

    def inner(df: DataFrame):
        # get Allergy Intolerance sub resource using AllergyIntolerance service.
        resource_df = df.withColumn("resource", allergy_intolerance_service_udf(df["resourceId"], df["ownerId"])).select(
            "resource"
        )

        # flatten resource json content
        resource_df = resource_df.withColumn("resource", f.from_json(f.col("resource"), ALLERGY_INTOLERANCE_SCHEMA))
        flatten_df = flatten(resource_df)
        # construct analytical base table fields from resource dataframe
        allergy_intolerance_df = (
            flatten_df.withColumn("allergy_intolerance_fhir_id", f.col("id"))
            .withColumn(
                "external_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'RI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn("clinical_status", f.col("clinicalStatus").getField("coding").getItem(0).getField("code"))
            .withColumn("clinical_system", f.col("clinicalStatus").getField("coding").getItem(0).getField("system"))
            .withColumn(
                "clinical_desc",
                f.when(
                    f.col("clinicalStatus").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("clinicalStatus").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("clinicalStatus").getField("text")),
            )
            .withColumn(
                "verification_status", f.col("verificationStatus").getField("coding").getItem(0).getField("code")
            )
            .withColumn(
                "verification_system", f.col("verificationStatus").getField("coding").getItem(0).getField("system")
            )
            .withColumn(
                "verification_desc",
                f.when(
                    f.col("verificationStatus").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("verificationStatus").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("verificationStatus").getField("text")),
            )
            .withColumn("type", f.col("type"))
            .withColumn(
                "category",
                f.col("category").getItem(0),
            )
            .withColumn("criticality", f.col("criticality"))
            .withColumn(
                "allergy_intolerance_code",
                f.col("code").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "allergy_intolerance_code_system",
                f.col("code").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "allergy_intolerance_code_desc",
                f.when(
                    f.col("code").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("code").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("code").getField("text")),
            )
            .withColumn("patient_fhir_id", f.col("patient.id"))
            .withColumn("encounter_fhir_id", f.col("encounter.id"))
            .withColumn(
                "onset_start_date",
                f.when(f.col("onsetPeriod.start").isNotNull(), f.to_timestamp(f.col("onsetPeriod.start"))).otherwise(
                    f.to_timestamp(f.col("onsetDateTime"))
                ),
            )
            .withColumn("onset_end_date", f.to_timestamp(f.col("onsetPeriod.end")))
            .withColumn("recorded_date", f.to_timestamp(f.col("recordedDate")))
            .withColumn("provider_fhir_id", f.col("recorder.id"))
            .withColumn("reaction_desc", f.col("reaction").getItem(0).getField("description"))
            .withColumn(
                "manifestation_code",
                f.col("reaction")
                .getItem(0)
                .getField("manifestation")
                .getItem(0)
                .getField("coding")
                .getItem(0)
                .getField("code"),
            )
            .withColumn(
                "manifestation_code_system",
                f.col("reaction")
                .getItem(0)
                .getField("manifestation")
                .getItem(0)
                .getField("coding")
                .getItem(0)
                .getField("system"),
            )
            .withColumn(
                "manifestation_desc",
                f.when(
                    f.col("reaction")
                    .getItem(0)
                    .getField("manifestation")
                    .getItem(0)
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("reaction")
                    .getItem(0)
                    .getField("manifestation")
                    .getItem(0)
                    .getField("coding")
                    .getItem(0)
                    .getField("display"),
                ).otherwise(f.col("reaction").getItem(0).getField("manifestation").getItem(0).getField("text")),
            )
            .withColumn("last_occurance_date", f.to_timestamp(f.col("lastOccurrence")))
            .withColumn("onset", f.to_timestamp(f.col("reaction").getItem(0).getField("onset")))
            .withColumn("severity", f.col("reaction").getItem(0).getField("severity"))
            .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )

        allergy_intolerance_final_df = (
            allergy_intolerance_df.select(
                "allergy_intolerance_fhir_id",
                "external_id",
                "clinical_status",
                "clinical_system",
                "clinical_desc",
                "verification_status",
                "verification_system",
                "verification_desc",
                "type",
                "category",
                "criticality",
                "allergy_intolerance_code",
                "allergy_intolerance_code_desc",
                "allergy_intolerance_code_system",
                "patient_fhir_id",
                "encounter_fhir_id",
                "onset_start_date",
                "onset_end_date",
                "recorded_date",
                "provider_fhir_id",
                "reaction_desc",
                "manifestation_code",
                "manifestation_desc",
                "manifestation_code_system",
                "last_occurance_date",
                "onset",
                "severity",
                "updated_user",
                "updated_ts",
            )
            .withColumnRenamed("allergy_intolerance_code", "code")
            .withColumnRenamed("allergy_intolerance_code_desc", "code_desc")
            .withColumnRenamed("allergy_intolerance_code_system", "code_system")
        )

        # expose final dataframe into temp view
        temp_view = f"allergy_intolerance_updates_{generate_random_string()}"
        allergy_intolerance_final_df.createOrReplaceTempView(temp_view)

        delta_table_location = os.path.join(delta_schema_location, "allergy_intolerance")

        # build and run merge query
        merge_query = f"""MERGE INTO delta.`{delta_table_location}`
        USING {temp_view}
        ON delta.`{delta_table_location}`.allergy_intolerance_fhir_id = {temp_view}.allergy_intolerance_fhir_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
        """
        spark.sql(merge_query)

        return allergy_intolerance_final_df

    return inner
