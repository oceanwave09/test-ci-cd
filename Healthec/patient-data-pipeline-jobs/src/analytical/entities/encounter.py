import os

from fhirclient.resources.encounter import Encounter
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from analytical.fhirschema.encounter import ENCOUNTER_SCHEMA
from dependencies.spark import flatten
from utils.api_client import get_patient_sub_entities
from utils.constants import ANALYTICAL_PIPELINE_USER
from utils.utils import generate_random_string


def update_encounter_table(spark: SparkSession, delta_schema_location: str):
    encounter_service_udf = f.udf(
        lambda resource_id, patient_id: get_patient_sub_entities(resource_id, patient_id, entity=Encounter)
    )

    def inner(df: DataFrame):
        # get encounter resource using encounter service.
        resource_df = df.withColumn("resource", encounter_service_udf(df["resourceId"], df["ownerId"])).select(
            "resource"
        )

        # flatten resource json content
        resource_df = resource_df.withColumn("resource", f.from_json(f.col("resource"), ENCOUNTER_SCHEMA))
        flatten_df = flatten(resource_df)

        # construct analytical base table fields from resource dataframe
        encounter_df = (
            flatten_df.withColumn("encounter_fhir_id", f.col("id"))
            .withColumn(
                "external_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'RI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn("status", f.col("status"))
            .withColumn("class_code", f.col("class.code"))
            .withColumn("class_system", f.col("class.system"))
            .withColumn("class_desc", f.col("class.display"))
            .withColumn(
                "type_system",
                f.col("type").getItem(0).getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "type_code",
                f.col("type").getItem(0).getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "type_desc",
                f.when(
                    f.col("type").getItem(0).getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("type").getItem(0).getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("type").getItem(0).getField("text")),
            )
            .withColumn("patient_fhir_id", f.col("subject.id"))
            .withColumn("provider_fhir_id", f.col("participant").getItem(0).getField("individual").getField("id"))
            .withColumn("start_date", f.to_timestamp(f.col("period.start")))
            .withColumn("end_date", f.to_timestamp(f.col("period.end")))
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
            .withColumn("origin_fhir_id", f.col("hospitalization.origin.id"))
            .withColumn("destination_fhir_id", f.col("hospitalization.destination.id"))
            .withColumn(
                "admit_source_system",
                f.col("hospitalization").getField("admitSource").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "admit_source_code",
                f.col("hospitalization").getField("admitSource").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "admit_source_desc",
                f.when(
                    f.col("hospitalization")
                    .getField("admitSource")
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("hospitalization")
                    .getField("admitSource")
                    .getField("coding")
                    .getItem(0)
                    .getField("display"),
                ).otherwise(f.col("hospitalization").getField("admitSource").getField("text")),
            )
            .withColumn(
                "discharge_disposition_system",
                f.col("hospitalization")
                .getField("dischargeDisposition")
                .getField("coding")
                .getItem(0)
                .getField("system"),
            )
            .withColumn(
                "discharge_disposition_code",
                f.col("hospitalization")
                .getField("dischargeDisposition")
                .getField("coding")
                .getItem(0)
                .getField("code"),
            )
            .withColumn(
                "discharge_disposition_desc",
                f.when(
                    f.col("hospitalization")
                    .getField("dischargeDisposition")
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("hospitalization")
                    .getField("dischargeDisposition")
                    .getField("coding")
                    .getItem(0)
                    .getField("display"),
                ).otherwise(f.col("hospitalization").getField("dischargeDisposition").getField("text")),
            )
            .withColumn("facility_fhir_id", f.col("location").getItem(0).getField("location").getField("id"))
            .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )

        encounter_final_df = encounter_df.select(
            "encounter_fhir_id",
            "external_id",
            "status",
            "class_system",
            "class_code",
            "class_desc",
            "type_system",
            "type_code",
            "type_desc",
            "patient_fhir_id",
            "provider_fhir_id",
            "start_date",
            "end_date",
            "reason_system",
            "reason_code",
            "reason_desc",
            "origin_fhir_id",
            "destination_fhir_id",
            "admit_source_system",
            "admit_source_code",
            "admit_source_desc",
            "discharge_disposition_system",
            "discharge_disposition_code",
            "discharge_disposition_desc",
            "facility_fhir_id",
            "updated_user",
            "updated_ts",
        )

        # expose final dataframe into temp view
        temp_view = f"encounter_updates_{generate_random_string()}"
        encounter_final_df.createOrReplaceTempView(temp_view)

        delta_table_location = os.path.join(delta_schema_location, "encounter")

        # build and run merge query
        merge_query = f"""MERGE INTO delta.`{delta_table_location}`
        USING {temp_view}
        ON delta.`{delta_table_location}`.encounter_fhir_id = {temp_view}.encounter_fhir_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
        """
        spark.sql(merge_query)

        return encounter_final_df

    return inner
