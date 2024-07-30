import os

from fhirclient.resources.coverage import Coverage
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from analytical.fhirschema.coverage import COVERAGE_SCHEMA
from dependencies.spark import flatten
from utils.api_client import get_patient_sub_entities
from utils.constants import ANALYTICAL_PIPELINE_USER
from utils.utils import generate_random_string


def update_coverage_table(spark: SparkSession, delta_schema_location: str):
    coverage_service_udf = f.udf(
        lambda resource_id, patient_id: get_patient_sub_entities(resource_id, patient_id, entity=Coverage)
    )

    def inner(df: DataFrame):
        # get coverage resource using coverage service.
        resource_df = df.withColumn("resource", coverage_service_udf(df["resourceId"], df["ownerId"])).select("resource")

        # flatten resource json content
        resource_df = resource_df.withColumn("resource", f.from_json(f.col("resource"), COVERAGE_SCHEMA))
        flatten_df = flatten(resource_df)

        # construct analytical base table fields from resource dataframe
        coverage_df = (
            flatten_df.withColumn("coverage_fhir_id", f.col("id"))
            .withColumn(
                "member_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'MB')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn(
                "medicaid_id",
                f.expr(
                    "filter(identifier, x -> size(x.type.coding) > 0 \
                        AND x.type.coding[0].code = 'MA')"
                )
                .getItem(0)
                .getField("value"),
            )
            .withColumn(
                "external_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'RI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn("status", f.col("status"))
            .withColumn("kind", f.col("kind"))
            .withColumn(
                "type_system",
                f.col("type").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "type_code",
                f.col("type").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "type_desc",
                f.when(
                    f.col("type").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("type").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("type").getField("text")),
            )
            .withColumn("subscriber_id", f.col("subscriber.id"))
            .withColumn("patient_fhir_id", f.col("beneficiary.id"))
            .withColumn("policy_holder_id", f.col("policyHolder.id"))
            .withColumn("subscriber_patient_id", f.col("subscriber.id"))
            .withColumn("dependent", f.col("dependent"))
            .withColumn(
                "relationship_system",
                f.col("relationship").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "relationship_code",
                f.col("relationship").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "relationship_desc",
                f.when(
                    f.col("relationship").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("relationship").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("relationship").getField("text")),
            )
            .withColumn("period_start_date", f.to_timestamp(f.col("period.start")))
            .withColumn("period_end_date", f.to_timestamp(f.col("period.end")))
            .withColumn("payer_fhir_id", f.col("insurer.id"))
            .withColumn(
                "coverage_class_system",
                f.col("class").getItem(0).getField("type").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "coverage_class_code",
                f.col("class").getItem(0).getField("type").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "coverage_class_desc",
                f.when(
                    f.col("class")
                    .getItem(0)
                    .getField("type")
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("class").getItem(0).getField("type").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("class").getItem(0).getField("type").getField("text")),
            )
            .withColumn(
                "copay_money_value",
                f.col("costToBeneficiary").getItem(0).getField("valueMoney").getField("value").cast(StringType()),
            )
            .withColumn(
                "copay_money_currency",
                f.col("costToBeneficiary").getItem(0).getField("valueMoney").getField("currency"),
            )
            .withColumn(
                "copay_quantity_value",
                f.col("costToBeneficiary").getItem(0).getField("valueQuantity").getField("value").cast(StringType()),
            )
            .withColumn(
                "copay_quantity_unit",
                f.col("costToBeneficiary").getItem(0).getField("valueQuantity").getField("unit"),
            )
            .withColumn(
                "copay_quantity_system",
                f.col("costToBeneficiary").getItem(0).getField("valueQuantity").getField("system"),
            )
            .withColumn(
                "copay_quantity_code",
                f.col("costToBeneficiary").getItem(0).getField("valueQuantity").getField("code"),
            )
            .withColumn(
                "copay_type_system",
                f.col("costToBeneficiary").getItem(0).getField("type").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "copay_type_code",
                f.col("costToBeneficiary").getItem(0).getField("type").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "copay_type_display",
                f.when(
                    f.col("costToBeneficiary")
                    .getItem(0)
                    .getField("type")
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("costToBeneficiary")
                    .getItem(0)
                    .getField("type")
                    .getField("coding")
                    .getItem(0)
                    .getField("display"),
                ).otherwise(f.col("costToBeneficiary").getItem(0).getField("type").getField("text")),
            )
            .withColumn("coverage_order", f.col("order"))
            .withColumn("class_value", f.col("class").getItem(0).getField("value"))
            .withColumn("class_name", f.col("class").getItem(0).getField("name"))
            .withColumn("network", f.col("network"))
            .withColumn("plan_fhir_id", f.col("insurancePlan.id"))
            .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )

        coverage_final_df = coverage_df.select(
            "coverage_fhir_id",
            "member_id",
            "medicaid_id",
            "external_id",
            "status",
            "kind",
            "type_system",
            "type_code",
            "type_desc",
            "subscriber_id",
            "patient_fhir_id",
            "policy_holder_id",
            "subscriber_patient_id",
            "dependent",
            "relationship_system",
            "relationship_code",
            "relationship_desc",
            "period_start_date",
            "period_end_date",
            "payer_fhir_id",
            "coverage_class_system",
            "coverage_class_code",
            "coverage_class_desc",
            "copay_money_value",
            "copay_money_currency",
            "copay_quantity_value",
            "copay_quantity_unit",
            "copay_quantity_system",
            "copay_quantity_code",
            "copay_type_system",
            "copay_type_code",
            "copay_type_display",
            "coverage_order",
            "class_value",
            "class_name",
            "network",
            "plan_fhir_id",
            "updated_user",
            "updated_ts",
        ).withColumnRenamed("coverage_class", "class")

        # expose final dataframe into temp view
        temp_view = f"coverage_updates_{generate_random_string()}"
        coverage_final_df.createOrReplaceTempView(temp_view)

        delta_table_location = os.path.join(delta_schema_location, "coverage")

        # build and run merge query
        merge_query = f"""MERGE INTO delta.`{delta_table_location}`
        USING {temp_view}
        ON delta.`{delta_table_location}`.coverage_fhir_id = {temp_view}.coverage_fhir_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
        """
        spark.sql(merge_query)

        return coverage_final_df

    return inner
