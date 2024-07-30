import os

from fhirclient.resources.organization import Organization
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import BooleanType, StringType

from analytical.fhirschema.organization import ORGANIZATION_SCHEMA
from dependencies.spark import flatten
from utils.api_client import get_core_entity
from utils.constants import ANALYTICAL_PIPELINE_USER
from utils.utils import generate_random_string


def update_practice_payer_table(spark: SparkSession, delta_schema_location: str):
    organization_service_udf = f.udf(lambda resource_id: get_core_entity(resource_id, entity=Organization))

    def inner(df: DataFrame):
        # get organization resource using organization service with resource id
        resource_df = df.withColumn("resource", organization_service_udf(df["resourceId"])).select("resource")

        # flatten resource json content
        resource_df = resource_df.withColumn("resource", f.from_json(f.col("resource"), ORGANIZATION_SCHEMA))
        flatten_df = flatten(resource_df)

        # construct analytical base table fields from resource dataframe
        organization_df = (
            flatten_df.withColumn("organization_fhir_id", f.col("id"))
            .withColumn(
                "external_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'RI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn(
                "ssn",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'SS')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn(
                "tin",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'TAX')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn(
                "group_npi",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'NPI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn(
                "nabp_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'NABP')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn("parent_fhir_id", f.col("partOf").getField("id"))
            .withColumn("name", f.col("name"))
            .withColumn("alias", f.col("alias").getItem(0))
            .withColumn("active", f.col("active").cast(BooleanType()))
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
            .withColumn("address_line_1", f.col("address").getItem(0).getField("line").getItem(0))
            .withColumn("address_line_2", f.col("address").getItem(0).getField("line").getItem(1))
            .withColumn("county", f.col("address").getItem(0).getField("district"))
            .withColumn("state", f.col("address").getItem(0).getField("state"))
            .withColumn("city", f.col("address").getItem(0).getField("city"))
            .withColumn("zip", f.col("address").getItem(0).getField("postalCode"))
            .withColumn("country", f.col("address").getItem(0).getField("country"))
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
            .withColumn("contact_last_name", f.col("contact").getItem(0).getField("name").getField("family"))
            .withColumn("contact_first_name", f.col("contact").getItem(0).getField("name").getField("given").getItem(0))
            .withColumn("contact_middle_name", f.col("contact").getItem(0).getField("name").getField("given").getItem(1))
            .withColumn(
                "contact_phone_work",
                f.expr("filter(contact[0].telecom, x -> x.system = 'phone')[0].value")
            )
            .withColumn(
                "contact_email",
                f.expr("filter(contact[0].telecom, x -> x.system = 'email')[0].value")
            )
            .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )
        organization_df.persist()

        # filter practice by type `prov`
        practice_df = (
            organization_df.filter(f.lower(f.col("type_code")) != "pay")
            .withColumnRenamed("organization_fhir_id", "fhir_id")
            .withColumnRenamed("alias", "dba_name")
            .select(
                "fhir_id",
                "external_id",
                "ssn",
                "tin",
                "group_npi",
                "nabp_id",
                "type_code",
                "type_system",
                "type_desc",
                "parent_fhir_id",
                "name",
                "dba_name",
                "active",
                "address_line_1",
                "address_line_2",
                "county",
                "state",
                "city",
                "zip",
                "country",
                "phone_work",
                "email",
                "fax",
                "contact_last_name",
                "contact_first_name",
                "contact_middle_name",
                "contact_phone_work",
                "contact_email",
                "updated_user",
                "updated_ts",
            )
        )
        practice_df.persist()

        if practice_df.count() > 0:
            # expose practice dataframe into temp view
            temp_view = f"practice_updates_{generate_random_string()}"
            practice_df.createOrReplaceTempView(temp_view)

            pract_delta_table_location = os.path.join(delta_schema_location, "practice")
            print(f"pract_delta_table_location: {pract_delta_table_location}")

            # build and run merge query
            practice_merge_query = f"""MERGE INTO delta.`{pract_delta_table_location}`
            USING {temp_view}
            ON delta.`{pract_delta_table_location}`.fhir_id = {temp_view}.fhir_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *;
            """
            spark.sql(practice_merge_query)
            print(f"practice inserted/updated : {practice_df.count()}")

        # filter practice by type `pay`
        payer_df = (
            organization_df.filter(f.lower(f.col("type_code")) == "pay")
            .withColumnRenamed("organization_fhir_id", "fhir_id")
            .select(
                "fhir_id",
                "external_id",
                "parent_fhir_id",
                "name",
                "active",
                "type_code",
                "type_system",
                "type_desc",
                "address_line_1",
                "address_line_2",
                "county",
                "state",
                "city",
                "zip",
                "country",
                "phone_work",
                "email",
                "fax",
                "contact_last_name",
                "contact_first_name",
                "contact_middle_name",
                "contact_phone_work",
                "contact_email",
                "updated_user",
                "updated_ts",
            )
        )
        payer_df.persist()

        if payer_df.count() > 0:
            # expose payer dataframe into temp view
            temp_view = f"payer_updates_{generate_random_string()}"
            payer_df.createOrReplaceTempView(temp_view)

            payer_delta_table_location = os.path.join(delta_schema_location, "payer")
            print(f"payer_delta_table_location: {payer_delta_table_location}")

            # build and run merge query
            payer_merge_query = f"""MERGE INTO delta.`{payer_delta_table_location}`
            USING {temp_view}
            ON delta.`{payer_delta_table_location}`.fhir_id = {temp_view}.fhir_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *;
            """
            spark.sql(payer_merge_query)
            print(f"payer inserted/updated : {payer_df.count()}")

        return organization_df

    return inner
