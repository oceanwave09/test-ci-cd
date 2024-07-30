import os

from fhirclient.resources.immunization import Immunization
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from analytical.fhirschema.immunization import IMMUNIZATION_SCHEMA
from dependencies.spark import flatten
from utils.api_client import get_patient_sub_entities
from utils.constants import ANALYTICAL_PIPELINE_USER
from utils.utils import generate_random_string


def update_immunization_table(spark: SparkSession, delta_schema_location: str):
    immunization_service_udf = f.udf(
        lambda resource_id, patient_id: get_patient_sub_entities(resource_id, patient_id, entity=Immunization)
    )

    def inner(df: DataFrame):
        # get immunization resource using immunization service.
        resource_df = df.withColumn("resource", immunization_service_udf(df["resourceId"], df["ownerId"])).select(
            "resource"
        )

        # flatten resource json content
        resource_df = resource_df.withColumn("resource", f.from_json(f.col("resource"), IMMUNIZATION_SCHEMA))
        flatten_df = flatten(resource_df)

        # construct analytical base table fields from resource dataframe
        immunization_df = (
            flatten_df.withColumn("immunization_fhir_id", f.col("id"))
            .withColumn(
                "external_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'RI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn("status", f.col("status"))
            .withColumn(
                "vaccine_system",
                f.col("vaccineCode").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "vaccine_code",
                f.col("vaccineCode").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "vaccine_desc",
                f.when(
                    f.col("vaccineCode").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("vaccineCode").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("vaccineCode").getField("text")),
            )
            .withColumn("patient_fhir_id", f.col("patient.id"))
            .withColumn("encounter_fhir_id", f.col("encounter.id"))
            .withColumn("occurrence_date_time", f.to_timestamp(f.col("occurrenceDateTime")))
            .withColumn("recorded_date_time", f.to_timestamp(f.col("recorded")))
            .withColumn("facility_fhir_id", f.col("location.id"))
            .withColumn("manufacturer_fhir_id", f.col("manufacturer.id"))
            .withColumn("lot_number", f.col("lotNumber"))
            .withColumn("expiration_date", f.to_date(f.col("expirationDate")))
            .withColumn(
                "site_system",
                f.col("site").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "site_code",
                f.col("site").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "site_desc",
                f.when(
                    f.col("site").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("site").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("site").getField("text")),
            )
            .withColumn(
                "route_system",
                f.col("route").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "route_code",
                f.col("route").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "route_desc",
                f.when(
                    f.col("route").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("route").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("route").getField("text")),
            )
            .withColumn("dose", f.col("doseQuantity").getField("value").cast(StringType()))
            .withColumn("dose_unit", f.col("doseQuantity").getField("unit"))
            .withColumn("dosage_system", f.col("doseQuantity").getField("system"))
            .withColumn("dosage_code", f.col("doseQuantity").getField("code"))
            .withColumn(
                "performer_system",
                f.col("performer").getItem(0).getField("function").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "performer_code",
                f.col("performer").getItem(0).getField("function").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "performer_display",
                f.when(
                    f.col("performer")
                    .getItem(0)
                    .getField("function")
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("performer")
                    .getItem(0)
                    .getField("function")
                    .getField("coding")
                    .getItem(0)
                    .getField("display"),
                ).otherwise(f.col("performer").getItem(0).getField("function").getField("text")),
            )
            .withColumn(
                "practitioner_fhir_id",
                f.col("performer").getItem(0).getField("actor").getField("id"),
            )
            .withColumn(
                "condition_fhir_id",
                f.col("reasonReference").getItem(0).getField("id"),
            )
            .withColumn(
                "reason_system",
                f.col("reasonCode").getItem(0).getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "reason_code",
                f.col("reasonCode").getItem(0).getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "reason_desc",
                f.when(
                    f.col("reasonCode").getItem(0).getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("reasonCode").getItem(0).getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("reasonCode").getItem(0).getField("text")),
            )
            .withColumn(
                "program_eligibility_system",
                f.col("programEligibility").getItem(0).getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "program_eligibility_code",
                f.col("programEligibility").getItem(0).getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "program_eligibility_desc",
                f.when(
                    f.col("programEligibility")
                    .getItem(0)
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("programEligibility").getItem(0).getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("programEligibility").getItem(0).getField("text")),
            )
            .withColumn("provider_fhir_id", f.col("performer").getItem(0).getField("actor").getField("id"))
            .withColumn("reason_fhir_id", f.col("reasonReference").getItem(0).getField("id"))
            .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )
        immunization_final_df = immunization_df.select(
            "immunization_fhir_id",
            "external_id",
            "status",
            "vaccine_system",
            "vaccine_code",
            "vaccine_desc",
            "patient_fhir_id",
            "encounter_fhir_id",
            "occurrence_date_time",
            "recorded_date_time",
            "facility_fhir_id",
            "manufacturer_fhir_id",
            "lot_number",
            "expiration_date",
            "site_system",
            "site_code",
            "site_desc",
            "route_system",
            "route_code",
            "route_desc",
            "dose",
            "dose_unit",
            "dosage_system",
            "dosage_code",
            "performer_system",
            "performer_code",
            "performer_display",
            "practitioner_fhir_id",
            "condition_fhir_id",
            "reason_system",
            "reason_code",
            "reason_desc",
            "program_eligibility_system",
            "program_eligibility_code",
            "program_eligibility_desc",
            "provider_fhir_id",
            "reason_fhir_id",
            "updated_user",
            "updated_ts",
        )

        # expose final dataframe into temp view
        temp_view = f"immunization_updates_{generate_random_string()}"
        immunization_final_df.createOrReplaceTempView(temp_view)

        delta_table_location = os.path.join(delta_schema_location, "immunization")

        # build and run merge query
        merge_query = f"""MERGE INTO delta.`{delta_table_location}`
        USING {temp_view}
        ON delta.`{delta_table_location}`.immunization_fhir_id = {temp_view}.immunization_fhir_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
        """
        spark.sql(merge_query)

        return immunization_final_df

    return inner
