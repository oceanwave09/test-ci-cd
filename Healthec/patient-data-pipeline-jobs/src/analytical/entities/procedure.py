import os

from fhirclient.resources.procedure import Procedure
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from analytical.fhirschema.procedure import PROCEDURE_SCHEMA
from dependencies.spark import flatten
from utils.api_client import get_patient_sub_entities
from utils.constants import ANALYTICAL_PIPELINE_USER
from utils.utils import generate_random_string


def update_procedure_table(spark: SparkSession, delta_schema_location: str):
    procedure_service_udf = f.udf(
        lambda resource_id, patient_id: get_patient_sub_entities(resource_id, patient_id, entity=Procedure)
    )

    def inner(df: DataFrame):
        # get procedure resource using procedure service.
        resource_df = df.withColumn("resource", procedure_service_udf(df["resourceId"], df["ownerId"])).select(
            "resource"
        )

        # flatten resource json content
        resource_df = resource_df.withColumn("resource", f.from_json(f.col("resource"), PROCEDURE_SCHEMA))
        flatten_df = flatten(resource_df)

        # construct analytical base table fields from resource dataframe
        procedure_df = (
            flatten_df.withColumn("procedure_fhir_id", f.col("id"))
            .withColumn(
                "external_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'RI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn("status", f.col("status"))
            .withColumn(
                "category_code",
                f.col("category").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "category_system",
                f.col("category").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "category_desc",
                f.when(
                    f.col("category").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("category").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("category").getField("text")),
            )
            .withColumn(
                "procedure_code",
                f.col("code").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "procedure_code_system",
                f.col("code").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "procedure_code_desc",
                f.when(
                    f.col("code").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("code").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("code").getField("text")),
            )
            .withColumn("patient_fhir_id", f.col("subject.id"))
            .withColumn("encounter_fhir_id", f.col("encounter.id"))
            .withColumn(
                "performed_start_date",
                f.when(
                    f.col("performedPeriod.start").isNotNull(), f.to_timestamp(f.col("performedPeriod.start"))
                ).otherwise(f.to_timestamp(f.col("performedDateTime"))),
            )
            .withColumn("performed_end_date", f.to_timestamp(f.col("performedPeriod.end")))
            .withColumn("provider_fhir_id", f.col("performer").getItem(0).getField("actor").getField("id"))
            .withColumn("facility_fhir_id", f.col("location.id"))
            .withColumn("reason_fhir_id", f.col("reasonReference").getItem(0).getField("id"))
            .withColumn(
                "modifier_url",
                f.col("modifierExtension").getItem(0).getField("url"),
            )
            .withColumn(
                "modifier_code",
                f.col("modifierExtension")
                .getItem(0)
                .getField("valueCodeableConcept")
                .getField("coding")
                .getItem(0)
                .getField("code"),
            )
            .withColumn(
                "modifier_system",
                f.col("modifierExtension")
                .getItem(0)
                .getField("valueCodeableConcept")
                .getField("coding")
                .getItem(0)
                .getField("system"),
            )
            .withColumn(
                "modifier_desc",
                f.when(
                    f.col("modifierExtension")
                    .getItem(0)
                    .getField("valueCodeableConcept")
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("modifierExtension")
                    .getItem(0)
                    .getField("valueCodeableConcept")
                    .getField("coding")
                    .getItem(0)
                    .getField("display"),
                ).otherwise(f.col("modifierExtension").getItem(0).getField("valueCodeableConcept").getField("text")),
            )
            .withColumn(
                "reason_code",
                f.col("reasonCode").getItem(0).getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "reason_system",
                f.col("reasonCode").getItem(0).getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "reason_desc",
                f.when(
                    f.col("reasonCode").getItem(0).getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("reasonCode").getItem(0).getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("reasonCode").getItem(0).getField("text")),
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
            .withColumn(
                "outcome_code",
                f.col("outcome").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "outcome_system",
                f.col("outcome").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "outcome_desc",
                f.when(
                    f.col("outcome").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("outcome").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("outcome").getField("text")),
            )
            .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )

        procedure_final_df = (
            procedure_df.select(
                "procedure_fhir_id",
                "external_id",
                "status",
                "category_code",
                "category_system",
                "category_desc",
                "procedure_code",
                "procedure_code_desc",
                "procedure_code_system",
                "patient_fhir_id",
                "encounter_fhir_id",
                "performed_start_date",
                "performed_end_date",
                "provider_fhir_id",
                "facility_fhir_id",
                "reason_fhir_id",
                "reason_fhir_id",
                "modifier_url",
                "modifier_code",
                "modifier_desc",
                "modifier_system",
                "reason_code",
                "reason_system",
                "reason_desc",
                "bodysite_code",
                "bodysite_system",
                "bodysite_desc",
                "outcome_code",
                "outcome_system",
                "outcome_desc",
                "updated_user",
                "updated_ts",
            )
            .withColumnRenamed("procedure_code", "code")
            .withColumnRenamed("procedure_code_desc", "code_desc")
            .withColumnRenamed("procedure_code_system", "code_system")
        )

        # expose final dataframe into temp view
        temp_view = f"procedure_updates_{generate_random_string()}"
        procedure_final_df.createOrReplaceTempView(temp_view)

        delta_table_location = os.path.join(delta_schema_location, "procedure")

        # build and run merge query
        merge_query = f"""MERGE INTO delta.`{delta_table_location}`
        USING {temp_view}
        ON delta.`{delta_table_location}`.procedure_fhir_id = {temp_view}.procedure_fhir_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
        """
        spark.sql(merge_query)

        return procedure_final_df

    return inner
