import os

from fhirclient.resources.patient import Patient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from analytical.fhirschema.patient import PATIENT_SCHEMA
from dependencies.spark import flatten
from utils.api_client import get_core_entity
from utils.constants import ANALYTICAL_PIPELINE_USER
from utils.utils import generate_random_string


def update_patient_table(spark: SparkSession, delta_schema_location: str):
    patient_service_udf = f.udf(lambda resource_id: get_core_entity(resource_id, Patient))

    def inner(df: DataFrame):
        # get patient resource using patient service with resource id
        resource_df = df.withColumn("resource", patient_service_udf(df["resourceId"])).select("resource")

        # flatten resource json content
        # resource_schema = spark.read.json(resource_df.rdd.map(lambda row: row.resource)).schema
        resource_df = resource_df.withColumn("resource", f.from_json(f.col("resource"), PATIENT_SCHEMA))
        flatten_df = flatten(resource_df)

        # construct analytical base table fields from resource dataframe
        patient_df = (
            flatten_df.withColumn("patient_fhir_id", f.col("id"))
            .withColumn(
                "external_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'RI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn(
                "subscriber_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'SN')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn(
                "member_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'MB')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn(
                "member_mbi",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'SB')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn(
                "medicaid_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'MA')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn("active", f.col("active"))
            .withColumn("gender", f.col("gender"))
            .withColumn("birth_date", f.trunc(f.col("birthDate"), "month"))
            .withColumn("deceased_flag", f.col("deceasedBoolean"))
            .withColumn(
                "deceased_date",
                f.when(
                    f.col("deceasedDateTime").isNotNull(),
                    f.expr("date_trunc('month', deceasedDateTime)"),
                ).otherwise(f.col("deceasedDateTime")),
            )
            .withColumn("county", f.col("address").getItem(0).getField("district"))
            .withColumn("state", f.col("address").getItem(0).getField("state"))
            .withColumn("city", f.col("address").getItem(0).getField("city"))
            .withColumn("zip", f.col("address").getItem(0).getField("postalCode"))
            .withColumn("country", f.col("address").getItem(0).getField("country"))
            .withColumn("marital_status_code", f.col("maritalStatus").getField("coding").getItem(0).getField("code"))
            .withColumn(
                "marital_status_system", f.col("maritalStatus").getField("coding").getItem(0).getField("system")
            )
            .withColumn(
                "marital_status_desc",
                f.when(
                    f.col("maritalStatus").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("maritalStatus").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("maritalStatus").getField("text")),
            )
            .withColumn(
                "primary_language_code",
                f.col("communication").getItem(0).getField("language").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "primary_language_syatem",
                f.col("communication")
                .getItem(0)
                .getField("language")
                .getField("coding")
                .getItem(0)
                .getField("system"),
            )
            .withColumn(
                "primary_language_desc",
                f.when(
                    f.col("communication")
                    .getItem(0)
                    .getField("language")
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("communication")
                    .getItem(0)
                    .getField("language")
                    .getField("coding")
                    .getItem(0)
                    .getField("display"),
                ).otherwise(f.col("communication").getItem(0).getField("language").getField("text")),
            )
            .withColumn("practice_fhir_id", f.col("managingOrganization.id"))
            .withColumn("provider_fhir_id", f.expr("transform(generalPractitioner, g -> g.id)"))
            .withColumn(
                "race_code",
                f.expr(
                    "filter(extension, x -> x.url = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race')"
                )
                .getItem(0)
                .getField("extension")
                .getItem(0)
                .getField("valueCoding")
                .getField("code"),
            )
            .withColumn(
                "race",
                f.when(
                    f.expr(
                        "filter(extension, x -> x.url = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race')"
                    )
                    .getItem(0)
                    .getField("extension")
                    .getItem(0)
                    .getField("url")
                    == "ombCategory",
                    f.expr(
                        "filter(extension, x -> x.url = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race')"
                    )
                    .getItem(0)
                    .getField("extension")
                    .getItem(0)
                    .getField("valueCoding")
                    .getField("display"),
                ).when(
                    f.expr(
                        "filter(extension, x -> x.url = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race')"
                    )
                    .getItem(0)
                    .getField("extension")
                    .getItem(0)
                    .getField("url")
                    == "text",
                    f.expr(
                        "filter(extension, x -> x.url = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race')"
                    )
                    .getItem(0)
                    .getField("extension")
                    .getItem(0)
                    .getField("valueString"),
                ),
            )
            .withColumn(
                "ethnicity_code",
                f.expr(
                    "filter(extension, x -> x.url = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity')"
                )
                .getItem(0)
                .getField("extension")
                .getItem(0)
                .getField("valueCoding")
                .getField("code"),
            )
            .withColumn(
                "ethnicity",
                f.when(
                    f.expr(
                        (
                            "filter(extension, x -> "
                            "x.url = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity')"
                        )
                    )
                    .getItem(0)
                    .getField("extension")
                    .getItem(0)
                    .getField("url")
                    == "ombCategory",
                    f.expr(
                        (
                            "filter(extension, x -> "
                            "x.url = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity')"
                        )
                    )
                    .getItem(0)
                    .getField("extension")
                    .getItem(0)
                    .getField("valueCoding")
                    .getField("display"),
                ).when(
                    f.expr(
                        (
                            "filter(extension, x -> "
                            "x.url = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity')"
                        )
                    )
                    .getItem(0)
                    .getField("extension")
                    .getItem(0)
                    .getField("url")
                    == "text",
                    f.expr(
                        (
                            "filter(extension, x -> "
                            "x.url = 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity')"
                        )
                    )
                    .getItem(0)
                    .getField("extension")
                    .getItem(0)
                    .getField("valueString"),
                ),
            )
            .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )
        patient_df = patient_df.select(
            "patient_fhir_id",
            "external_id",
            "subscriber_id",
            "member_id",
            "member_mbi",
            "medicaid_id",
            "active",
            "gender",
            "birth_date",
            "deceased_flag",
            "deceased_date",
            "county",
            "state",
            "city",
            "zip",
            "country",
            "marital_status_code",
            "marital_status_system",
            "marital_status_desc",
            "primary_language_code",
            "primary_language_desc",
            "primary_language_syatem",
            "practice_fhir_id",
            "provider_fhir_id",
            "race_code",
            "race",
            "ethnicity_code",
            "ethnicity",
            "updated_user",
            "updated_ts",
            f.explode_outer("contact").alias("contact"),
        )

        patient_final_df = (
            patient_df.withColumn("contact_gender", f.col("contact.gender"))
            .withColumn(
                "contact_relationship_code",
                f.col("contact.relationship").getItem(0).getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "contact_relationship_system",
                f.col("contact.relationship").getItem(0).getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "contact_relationship_desc",
                f.when(
                    f.col("contact.relationship")
                    .getItem(0)
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("contact.relationship").getItem(0).getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("contact.relationship").getItem(0).getField("text")),
            )
            .withColumn("contact_period_start", f.to_timestamp(f.col("contact.period").getField("start")))
            .withColumn("contact_period_end", f.to_timestamp(f.col("contact.period").getField("end")))
            .withColumn("contact_county", f.col("contact.address").getField("district"))
            .withColumn("contact_state", f.col("contact.address").getField("state"))
            .withColumn("contact_city", f.col("contact.address").getField("city"))
            .withColumn("contact_zip", f.col("contact.address").getField("postalCode"))
            .withColumn("contact_country", f.col("contact.address").getField("country"))
            .drop("contact")
        )

        # expose final dataframe into temp view
        temp_view = f"patient_updates_{generate_random_string()}"
        patient_final_df.createOrReplaceTempView(temp_view)

        delta_table_location = os.path.join(delta_schema_location, "patient")

        # build and run merge query
        merge_query = f"""MERGE INTO delta.`{delta_table_location}`
        USING {temp_view}
        ON delta.`{delta_table_location}`.patient_fhir_id = {temp_view}.patient_fhir_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
        """
        spark.sql(merge_query)

        return patient_final_df

    return inner
