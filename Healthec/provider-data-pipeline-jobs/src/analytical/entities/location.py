import os

from fhirclient.resources.location import Location
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from analytical.fhirschema.location import LOCATION_SCHEMA
from dependencies.spark import flatten
from utils.api_client import get_sub_entity
from utils.constants import ANALYTICAL_PIPELINE_USER
from utils.utils import generate_random_string


def update_facility_table(spark: SparkSession, delta_schema_location: str):
    location_service_udf = f.udf(
        lambda resource_id, scope_id: get_sub_entity(resource_id, Location, scope_id, "Organization")
    )

    def inner(df: DataFrame):
        # get encounter resource using encounter service.
        resource_df = df.withColumn("resource", location_service_udf(df["resourceId"], df["ownerId"])).select("resource")

        # flatten resource json content
        resource_df = resource_df.withColumn("resource", f.from_json(f.col("resource"), LOCATION_SCHEMA))
        flatten_df = flatten(resource_df)

        # construct analytical base table fields from resource dataframe
        facility_df = (
            flatten_df.withColumn("facility_fhir_id", f.col("id"))
            .withColumn(
                "external_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'RI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn(
                "group_npi",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'NPI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn("status", f.col("status"))
            .withColumn("name", f.col("name"))
            .withColumn("alias", f.col("alias").getItem(0))
            .withColumn("description", f.col("description"))
            .withColumn("mode", f.col("mode"))
            .withColumn(
                "type_code",
                f.col("type").getItem(0).getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "type_system",
                f.col("type").getItem(0).getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "type_desc",
                f.when(
                    f.col("type").getItem(0).getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("type").getItem(0).getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("type").getItem(0).getField("text")),
            )
            .withColumn(
                "phone_work",
                f.expr("filter(telecom, x -> x.use = 'work')[0].value")
            )
            .withColumn(
                "email",
                f.expr("filter(telecom, x -> x.system = 'email')[0].value")
            )
            .withColumn(
                "fax",
                f.expr("filter(telecom, x -> x.system = 'fax')[0].value")
            )
            .withColumn("operational_code", f.col("operationalStatus").getField("code"))
            .withColumn("operational_system", f.col("operationalStatus").getField("system"))
            .withColumn("operational_desc", f.col("operationalStatus").getField("display"))
            .withColumn("address_line_1", f.col("address").getField("line").getItem(0))
            .withColumn("address_line_2", f.col("address").getField("line").getItem(1))
            .withColumn("county", f.col("address").getField("district"))
            .withColumn("state", f.col("address").getField("state"))
            .withColumn("city", f.col("address").getField("city"))
            .withColumn("zip", f.col("address").getField("postalCode"))
            .withColumn("country", f.col("address").getField("country"))
            .withColumn("practice_fhir_id", f.col("managingOrganization").getField("id"))
            .withColumn(
                "physical_type_code",
                f.col("physicalType").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "physical_type_system",
                f.col("physicalType").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "physical_type_desc",
                f.when(
                    f.col("physicalType").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("physicalType").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("physicalType").getField("text")),
            )
            .withColumn("availability_exceptions", f.col("availabilityExceptions"))
            .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )

        facility_final_df = facility_df.select(
            "facility_fhir_id",
            "external_id",
            "group_npi",
            "status",
            "name",
            "alias",
            "description",
            "mode",
            "type_code",
            "type_system",
            "type_desc",
            "phone_work",
            "email",
            "fax",
            "operational_code",
            "operational_system",
            "operational_desc",
            "address_line_1",
            "address_line_2",
            "county",
            "state",
            "city",
            "zip",
            "country",
            "practice_fhir_id",
            "physical_type_code",
            "physical_type_system",
            "physical_type_desc",
            "availability_exceptions",
            "updated_user",
            "updated_ts",
        )

        # expose final dataframe into temp view
        temp_view = f"facility_updates_{generate_random_string()}"
        facility_final_df.createOrReplaceTempView(temp_view)

        delta_table_location = os.path.join(delta_schema_location, "facility")

        # build and run merge query
        merge_query = f"""MERGE INTO delta.`{delta_table_location}`
        USING {temp_view}
        ON delta.`{delta_table_location}`.facility_fhir_id = {temp_view}.facility_fhir_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
        """
        spark.sql(merge_query)

        return facility_final_df

    return inner
