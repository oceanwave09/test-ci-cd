import os

from fhirclient.resources.insuranceplan import InsurancePlan
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from analytical.fhirschema.insurance_plan import INSURANCE_PLAN_SCHEMA
from dependencies.spark import flatten
from utils.api_client import get_core_entity
from utils.constants import ANALYTICAL_PIPELINE_USER
from utils.utils import generate_random_string


def update_insurance_plan_table(spark: SparkSession, delta_schema_location: str):
    insurance_plan_service_udf = f.udf(lambda resource_id: get_core_entity(resource_id, entity=InsurancePlan))

    def inner(df: DataFrame):
        # get encounter resource using encounter service.
        resource_df = df.withColumn("resource", insurance_plan_service_udf(df["resourceId"])).select("resource")
        resource_df.show(truncate=False)
        # flatten resource json content
        resource_df = resource_df.withColumn("resource", f.from_json(f.col("resource"), INSURANCE_PLAN_SCHEMA))
        flatten_df = flatten(resource_df)

        # construct analytical base table fields from resource dataframe
        insurance_plan_df = (
            flatten_df.withColumn("plan_fhir_id", f.col("id"))
            .withColumn(
                "external_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'RI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn("status", f.col("status"))
            .withColumn("alias", f.col("alias"))
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
            .withColumn("name", f.col("name"))
            .withColumn("period_start_date", f.to_timestamp(f.col("period.start")))
            .withColumn("period_end_date", f.to_timestamp(f.col("period.end")))
            .withColumn("payer_fhir_id", f.col("ownedBy.id"))
            .withColumn(
                "coverage_type_system",
                f.col("coverage").getItem(0).getField("type").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "coverage_type_code",
                f.col("coverage").getItem(0).getField("type").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "coverage_type_desc",
                f.when(
                    f.col("coverage")
                    .getItem(0)
                    .getField("type")
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("coverage").getItem(0).getField("type").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("coverage").getItem(0).getField("type").getField("text")),
            )
            .withColumn("network_fhir_id", f.col("coverage").getItem(0).getField("network").getItem(0).getField("id"))
            .withColumn(
                "coverage_benefit_system",
                f.col("coverage")
                .getItem(0)
                .getField("benefit")
                .getItem(0)
                .getField("type")
                .getField("coding")
                .getItem(0)
                .getField("system"),
            )
            .withColumn(
                "coverage_benefit_code",
                f.col("coverage")
                .getItem(0)
                .getField("benefit")
                .getItem(0)
                .getField("type")
                .getField("coding")
                .getItem(0)
                .getField("code"),
            )
            .withColumn(
                "coverage_benefit_desc",
                f.when(
                    f.col("coverage")
                    .getItem(0)
                    .getField("benefit")
                    .getItem(0)
                    .getField("type")
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("coverage")
                    .getItem(0)
                    .getField("benefit")
                    .getItem(0)
                    .getField("type")
                    .getField("coding")
                    .getItem(0)
                    .getField("display"),
                ).otherwise(
                    f.col("coverage").getItem(0).getField("benefit").getItem(0).getField("type").getField("text")
                ),
            )
            .withColumn(
                "coverage_requirement",
                f.col("coverage").getItem(0).getField("benefit").getItem(0).getField("requirement"),
            )
            .withColumn(
                "coverage_limit_value",
                f.col("coverage")
                .getItem(0)
                .getField("benefit")
                .getItem(0)
                .getField("limit")
                .getItem(0)
                .getField("value")
                .getField("value")
                .cast(StringType()),
            )
            .withColumn(
                "coverage_limit_unit",
                f.col("coverage")
                .getItem(0)
                .getField("benefit")
                .getItem(0)
                .getField("limit")
                .getItem(0)
                .getField("value")
                .getField("unit"),
            )
            .withColumn("coverage_administered_by", f.col("administeredBy").getField("id"))
            .withColumn("coverage_area", f.col("coverageArea").getItem(0).getField("id"))
            .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )
        insurance_plan_final_df = insurance_plan_df.select(
            "plan_fhir_id",
            "external_id",
            "status",
            "alias",
            "type_system",
            "type_code",
            "type_desc",
            "name",
            "period_start_date",
            "period_end_date",
            "payer_fhir_id",
            "coverage_type_system",
            "coverage_type_code",
            "coverage_type_desc",
            "network_fhir_id",
            "coverage_benefit_system",
            "coverage_benefit_code",
            "coverage_benefit_desc",
            "coverage_requirement",
            "coverage_limit_value",
            "coverage_limit_unit",
            "coverage_administered_by",
            "coverage_area",
            "updated_user",
            "updated_ts",
        )

        insurance_plan_final_df.show(5, truncate=False)

        # expose final dataframe into temp view
        temp_view = f"insurance_plan_updates_{generate_random_string()}"
        insurance_plan_final_df.createOrReplaceTempView(temp_view)

        delta_table_location = os.path.join(delta_schema_location, "insurance_plan")

        # build and run merge query
        merge_query = f"""MERGE INTO delta.`{delta_table_location}`
        USING {temp_view}
        ON delta.`{delta_table_location}`.plan_fhir_id = {temp_view}.plan_fhir_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
        """
        spark.sql(merge_query)

        return insurance_plan_final_df

    return inner
