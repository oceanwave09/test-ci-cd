import os

from fhirclient.resources.claim import Claim
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType, BooleanType

from analytical.fhirschema.claim import CLAIM_SCHEMA
from dependencies.spark import flatten
from utils.api_client import get_patient_sub_entities
from utils.constants import ANALYTICAL_PIPELINE_USER
from utils.utils import generate_random_string


def update_medical_rx_claim_table(spark: SparkSession, delta_schema_location: str):
    claim_service_udf = f.udf(
        lambda resource_id, patient_id: get_patient_sub_entities(resource_id, patient_id, entity=Claim)
    )

    def inner(df: DataFrame):
        # get medical claim resource using patient service with resource id
        resource_df = df.withColumn("resource", claim_service_udf(df["resourceId"], df["ownerId"])).select("resource")

        # flatten resource json content
        resource_df = resource_df.withColumn("resource", f.from_json(f.col("resource"), CLAIM_SCHEMA))
        flatten_df = flatten(resource_df)

        # construct analytical base table fields from resource dataframe
        claim_df = (
            flatten_df.withColumn("claim_fhir_id", f.col("id"))
            .withColumn(
                "claim_number",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'RI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn("status", f.col("status"))
            .withColumn(
                "type_code",
                f.col("type").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "type_system",
                f.col("type").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "type_desc",
                f.when(
                    f.col("type").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("type").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("type").getField("text")),
            )
            .withColumn("use", f.col("use"))
            .withColumn(
                "related_ref_value",
                f.col("related").getItem(0).getField("reference").getField("value").cast(StringType()),
            )
            .withColumn(
                "related_ref_system",
                f.col("related").getItem(0).getField("reference").getField("system"),
            )
            .withColumn("patient_fhir_id", f.col("patient.id"))
            .withColumn("billable_start_date", f.to_timestamp(f.col("billablePeriod.start")))
            .withColumn("billable_end_date", f.to_timestamp(f.col("billablePeriod.end")))
            .withColumn("created", f.to_timestamp(f.col("created")))
            .withColumn("enterer_fhir_id", f.col("enterer.id"))
            .withColumn("payer_fhir_id", f.col("insurer.id"))
            .withColumn("provider_fhir_id", f.col("provider.id"))
            .withColumn(
                "priority_code",
                f.col("priority").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "priority_system",
                f.col("priority").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "priority_desc",
                f.when(
                    f.col("priority").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("priority").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("priority").getField("text")),
            )
            .withColumn(
                "funds_reserve_code",
                f.col("fundsReserve").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "funds_reserve_system",
                f.col("fundsReserve").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "funds_reserve_desc",
                f.when(
                    f.col("fundsReserve").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("fundsReserve").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("fundsReserve").getField("text")),
            )
            .withColumn("prescription_fhir_id", f.col("prescription.id"))
            .withColumn("orig_prescription_fhir_id", f.col("originalPrescription.id"))
            .withColumn(
                "payee_type_code",
                f.col("payee").getField("type").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "payee_type_system",
                f.col("payee").getField("type").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "payee_type_desc",
                f.when(
                    f.col("payee").getField("type").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("payee").getField("type").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("payee").getField("type").getField("text")),
            )
            .withColumn("payee_fhir_id", f.col("payee.party.id"))
            .withColumn("facility_fhir_id", f.col("facility.id"))
            .withColumn(
                "diagnosis_fhir_id", f.col("diagnosis").getItem(0).getField("diagnosisReference").getField("id")
            )
            .withColumn("diagnosis_sequence", f.col("diagnosis").getItem(0).getField("sequence").cast(StringType()))
            .withColumn(
                "procedure_fhir_id", f.col("procedure").getItem(0).getField("procedureReference").getField("id")
            )
            .withColumn("procedure_sequence", f.col("procedure").getItem(0).getField("sequence").cast(StringType()))
            .withColumn(
                "insurance_sequence",
                f.col("insurance").getItem(0).getField("sequence").cast(StringType()),
            )
            .withColumn(
                "insurance_focal",
                f.col("insurance").getItem(0).getField("focal").cast(BooleanType()),
            )
            .withColumn("coverage_fhir_id", f.col("insurance").getItem(0).getField("coverage").getField("id"))
            .withColumn("accident_date", f.to_timestamp(f.col("accident.date")))
            .withColumn("accident_type_code", f.col("accident.type.coding").getItem(0).getField("code"))
            .withColumn("accident_type_system", f.col("accident.type.coding").getItem(0).getField("system"))
            .withColumn(
                "accident_type_desc",
                f.when(
                    f.col("accident.type.coding").getItem(0).getField("display").isNotNull(),
                    f.col("accident.type.coding").getItem(0).getField("display"),
                ).otherwise(f.col("accident.type").getField("text")),
            )
            .withColumn("accident_address_line_1", f.col("accident.locationAddress").getField("line").getItem(0))
            .withColumn("accident_address_line_2", f.col("accident.locationAddress").getField("line").getItem(1))
            .withColumn("accident_city", f.col("accident.locationAddress.city"))
            .withColumn("accident_state", f.col("accident.locationAddress.state"))
            .withColumn("accident_postal_code", f.col("accident.locationAddress.postalCode"))
            .withColumn("location_fhir_id", f.col("accident.locationReference.id"))
            .withColumn("total_amount", f.col("total.value").cast(StringType()))
            .withColumn("currency", f.col("total.currency"))
            .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )
        claim_df.persist()

        # filter medical claim by type `institutional, professional`
        medical_claim_df = claim_df.filter((f.col("type_code").isin(["institutional", "professional"]))).select(
            "claim_fhir_id",
            "claim_number",
            "type_code",
            "type_system",
            "type_desc",
            "use",
            "related_ref_value",
            "related_ref_system",
            "patient_fhir_id",
            "billable_start_date",
            "billable_end_date",
            "created",
            "enterer_fhir_id",
            "payer_fhir_id",
            "provider_fhir_id",
            "priority_code",
            "priority_system",
            "priority_desc",
            "funds_reserve_code",
            "funds_reserve_system",
            "funds_reserve_desc",
            "prescription_fhir_id",
            "orig_prescription_fhir_id",
            "payee_type_code",
            "payee_type_desc",
            "payee_type_system",
            "payee_fhir_id",
            "facility_fhir_id",
            "diagnosis_sequence",
            "diagnosis_fhir_id",
            "procedure_sequence",
            "procedure_fhir_id",
            "insurance_sequence",
            "insurance_focal",
            "coverage_fhir_id",
            "accident_date",
            "accident_type_code",
            "accident_type_system",
            "accident_type_desc",
            "accident_address_line_1",
            "accident_address_line_2",
            "accident_city",
            "accident_state",
            "accident_postal_code",
            "location_fhir_id",
            "total_amount",
            "currency",
            "updated_user",
            "updated_ts",
        )
        medical_claim_df.persist()

        if medical_claim_df.count() > 0:
            # expose medical_claim_info dataframe into temp view
            temp_view = f"medical_claim_updates_{generate_random_string()}"
            medical_claim_df.createOrReplaceTempView(temp_view)

            medic_delta_table_location = os.path.join(delta_schema_location, "medical_claim")
            print(f"medical_claim_delta_table_location: {medic_delta_table_location}")

            # build and run merge query
            medical_merge_query = f"""MERGE INTO delta.`{medic_delta_table_location}`
            USING {temp_view}
            ON delta.`{medic_delta_table_location}`.claim_fhir_id = {temp_view}.claim_fhir_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *;
            """
            spark.sql(medical_merge_query)
            print(f"medical claim inserted/updated : {medical_claim_df.count()}")

            # select claim_lines related to claims
            med_claim_line_df = claim_df.select(
                "claim_fhir_id", "diagnosis", "procedure", f.explode("item").alias("item")
            ).select("claim_fhir_id", "diagnosis", "procedure", "item.*")
            med_claim_line_df.printSchema()

            med_claim_line_df = (
                med_claim_line_df.withColumn("line_number", f.col("sequence").cast(StringType()))
                .withColumn(
                    "service_code",
                    f.col("productOrService").getField("coding").getItem(0).getField("code"),
                )
                .withColumn(
                    "service_system",
                    f.col("productOrService").getField("coding").getItem(0).getField("system"),
                )
                .withColumn(
                    "service_desc",
                    f.when(
                        f.col("productOrService").getField("coding").getItem(0).getField("display").isNotNull(),
                        f.col("productOrService").getField("coding").getItem(0).getField("display"),
                    ).otherwise(f.col("productOrService").getField("text")),
                )
                .withColumn(
                    "revenue_code",
                    f.col("revenue").getField("coding").getItem(0).getField("code"),
                )
                .withColumn(
                    "revenue_system",
                    f.col("revenue").getField("coding").getItem(0).getField("system"),
                )
                .withColumn(
                    "revenue_desc",
                    f.when(
                        f.col("revenue").getField("coding").getItem(0).getField("display").isNotNull(),
                        f.col("revenue").getField("coding").getItem(0).getField("display"),
                    ).otherwise(f.col("revenue").getField("text")),
                )
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
                    "serviced_start_date",
                    f.when(
                        f.col("servicedPeriod.start").isNotNull(), f.to_timestamp(f.col("servicedPeriod.start"))
                    ).otherwise(f.to_timestamp(f.col("servicedDate"))),
                )
                .withColumn("serviced_end_date", f.to_timestamp(f.col("servicedPeriod.end")))
                .withColumn("facility_fhir_id", f.col("locationReference.id"))
                .withColumn("encounter_fhir_id", f.col("encounter.id"))
                .withColumn(
                    "diagnosis_fhir_id", f.col("diagnosis").getItem(0).getField("diagnosisReference").getField("id")
                )
                .withColumn(
                    "diagnosis_sequence", f.col("diagnosis").getItem(0).getField("sequence").cast(StringType())
                )
                .withColumn(
                    "procedure_fhir_id", f.col("procedure").getItem(0).getField("procedureReference").getField("id")
                )
                .withColumn(
                    "procedure_sequence", f.col("procedure").getItem(0).getField("sequence").cast(StringType())
                )
                .withColumn("quantity", f.col("quantity.value").cast(StringType()))
                .withColumn("unit_price", f.col("unitPrice.value").cast(StringType()))
                .withColumn("unit_price_currency", f.col("unitPrice.currency"))
                .withColumn("factor", f.col("factor").cast(StringType()))
                .withColumn("net_price", f.col("net.value").cast(StringType()))
                .withColumn("net_price_currency", f.col("net.currency"))
                .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
                .withColumn("updated_ts", f.current_timestamp())
            )

            med_claim_line_df = med_claim_line_df.select(
                "claim_fhir_id",
                "line_number",
                "service_code",
                "service_system",
                "service_desc",
                "revenue_code",
                "revenue_system",
                "revenue_desc",
                "category_code",
                "category_system",
                "category_desc",
                "serviced_start_date",
                "serviced_end_date",
                "encounter_fhir_id",
                "facility_fhir_id",
                "diagnosis_fhir_id",
                "diagnosis_sequence",
                "procedure_fhir_id",
                "procedure_sequence",
                "quantity",
                "unit_price",
                "unit_price_currency",
                "factor",
                "net_price",
                "net_price_currency",
                "updated_user",
                "updated_ts",
            )
            med_claim_line_df.persist()

            # expose final dataframe into temp view
            if med_claim_line_df.count() > 0:
                # expose medical_claim_line_info dataframe into temp view
                temp_view = f"medical_claim_line_updates_{generate_random_string()}"
                med_claim_line_df.createOrReplaceTempView(temp_view)

                med_line_delta_table_location = os.path.join(delta_schema_location, "medical_claim_line")
                print(f"medical_claim_line_delta_table_location: {medic_delta_table_location}")

                # build and run merge query
                merge_query = f"""MERGE INTO delta.`{med_line_delta_table_location}`
                USING {temp_view}
                ON (delta.`{med_line_delta_table_location}`.claim_fhir_id = {temp_view}.claim_fhir_id
                AND `{med_line_delta_table_location}`.line_number = {temp_view}.line_number)
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *;
                """
                spark.sql(merge_query)
                print(f"medical claim line inserted/updated : {med_claim_line_df.count()}")

        # filter rx claim by type `pharmacy`
        rx_claim_df = claim_df.filter(
            (f.col("type_code") == "pharmacy") & (f.col("prescription_fhir_id").isNotNull())
        ).select(
            "claim_fhir_id",
            "claim_number",
            "type_code",
            "type_system",
            "type_desc",
            "use",
            "patient_fhir_id",
            "billable_start_date",
            "billable_end_date",
            "created",
            "enterer_fhir_id",
            "payer_fhir_id",
            "provider_fhir_id",
            "prescription_fhir_id",
            "payee_type_code",
            "payee_type_system",
            "payee_type_desc",
            "payee_fhir_id",
            "facility_fhir_id",
            "coverage_fhir_id",
            "total_amount",
            "updated_user",
            "updated_ts",
        )
        rx_claim_df.persist()

        if rx_claim_df.count() > 0:
            # expose final dataframe into temp view
            temp_view = f"rx_claim_updates_{generate_random_string()}"
            rx_claim_df.createOrReplaceTempView(temp_view)

            rx_delta_table_location = os.path.join(delta_schema_location, "rx_claim")
            print(f"rx_claim_delta_table_location: {rx_delta_table_location}")

            # build and run merge query
            merge_query = f"""MERGE INTO delta.`{rx_delta_table_location}`
                USING {temp_view}
                ON delta.`{rx_delta_table_location}`.claim_fhir_id = {temp_view}.claim_fhir_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *;
                """
            spark.sql(merge_query)
            print(f"rx claim inserted/updated : {rx_claim_df.count()}")

        return claim_df

    return inner
