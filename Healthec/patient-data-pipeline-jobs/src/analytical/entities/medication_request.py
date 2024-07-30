import os

from fhirclient.resources.medicationrequest import MedicationRequest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType

from analytical.fhirschema.medication_request import MEDICATION_REQUEST_SCHEMA
from dependencies.spark import flatten
from utils.api_client import get_patient_sub_entities
from utils.constants import ANALYTICAL_PIPELINE_USER
from utils.utils import generate_random_string


def update_medication_request_table(spark: SparkSession, delta_schema_location: str):
    medication_request_service_udf = f.udf(
        lambda resource_id, patient_id: get_patient_sub_entities(resource_id, patient_id, entity=MedicationRequest)
    )

    def inner(df: DataFrame):
        # get medication request resource using medication request service.
        resource_df = df.withColumn(
            "resource", medication_request_service_udf(df["resourceId"], df["ownerId"])
        ).select("resource")

        # flatten resource json content
        resource_df = resource_df.withColumn("resource", f.from_json(f.col("resource"), MEDICATION_REQUEST_SCHEMA))
        flatten_df = flatten(resource_df)

        # construct analytical base table fields from resource dataframe
        medication_request_df = (
            flatten_df.withColumn("medication_request_fhir_id", f.col("id"))
            .withColumn(
                "external_id",
                f.expr("filter(identifier, x -> size(x.type.coding) > 0 AND x.type.coding[0].code = 'RI')")
                .getItem(0)
                .getField("value"),
            )
            .withColumn("status", f.col("status"))
            .withColumn("intent", f.col("intent"))
            .withColumn(
                "status_reason_code",
                f.col("statusReason").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "status_reason_system",
                f.col("statusReason").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "status_reason_desc",
                f.when(
                    f.col("statusReason").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("statusReason").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("statusReason").getField("text")),
            )
            .withColumn(
                "category_code",
                f.col("category").getItem(0).getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "category_system",
                f.col("category").getItem(0).getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "category_desc",
                f.when(
                    f.col("category").getItem(0).getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("category").getItem(0).getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("category").getItem(0).getField("text")),
            )
            .withColumn("priority", f.col("priority"))
            .withColumn(
                "medication_code",
                f.col("medicationCodeableConcept").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "medication_system",
                f.col("medicationCodeableConcept").getField("coding").getItem(0).getField("system"),
            )
            .withColumn(
                "medication_desc",
                f.when(
                    f.col("medicationCodeableConcept").getField("coding").getItem(0).getField("display").isNotNull(),
                    f.col("medicationCodeableConcept").getField("coding").getItem(0).getField("display"),
                ).otherwise(f.col("medicationCodeableConcept").getField("text")),
            )
            .withColumn("medication_fhir_id", f.col("medicationReference.id"))
            .withColumn("patient_fhir_id", f.col("subject.id"))
            .withColumn("encounter_fhir_id", f.col("encounter.id"))
            .withColumn("authored_on", f.to_timestamp(f.col("authoredOn")))
            .withColumn("provider_fhir_id", f.col("requester.id"))
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
            .withColumn("validity_period_start_date", f.to_timestamp(f.col("dispenseRequest.validityPeriod.start")))
            .withColumn("validity_period_end_date", f.to_timestamp(f.col("dispenseRequest.validityPeriod.end")))
            .withColumn("number_of_repeats_allow", f.col("dispenseRequest.numberOfRepeatsAllowed"))
            .withColumn("quantity_value", f.col("dispenseRequest.quantity.value").cast(StringType()))
            .withColumn("quantity_unit", f.col("dispenseRequest.quantity.unit"))
            .withColumn("quantity_system", f.col("dispenseRequest.quantity.system"))
            .withColumn("quantity_code", f.col("dispenseRequest.quantity.code"))
            .withColumn("expected_supply_value", f.col("dispenseRequest.expectedSupplyDuration.value"))
            .withColumn("expected_supply_unit", f.col("dispenseRequest.expectedSupplyDuration.unit"))
            .withColumn("expected_supply_system", f.col("dispenseRequest.expectedSupplyDuration.system"))
            .withColumn("expected_supply_code", f.col("dispenseRequest.expectedSupplyDuration.code"))
            .withColumn("organization_fhir_id", f.col("dispenseRequest.dispenser.id"))
            .withColumn(
                "dosage_sequence", f.col("dosageInstruction").getItem(0).getField("sequence").cast(StringType())
            )
            .withColumn("dosage_text", f.col("dosageInstruction").getItem(0).getField("text"))
            .withColumn(
                "additional_instruction_code",
                f.col("dosageInstruction")
                .getItem(0)
                .getField("additionalInstruction")
                .getField("coding")
                .getItem(0)
                .getField("code"),
            )
            .withColumn(
                "additional_instruction_system",
                f.col("dosageInstruction")
                .getItem(0)
                .getField("additionalInstruction")
                .getField("coding")
                .getItem(0)
                .getField("system"),
            )
            .withColumn(
                "additional_instruction_desc",
                f.when(
                    f.col("dosageInstruction")
                    .getItem(0)
                    .getField("additionalInstruction")
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("dosageInstruction")
                    .getItem(0)
                    .getField("additionalInstruction")
                    .getField("coding")
                    .getItem(0)
                    .getField("display"),
                ).otherwise(f.col("dosageInstruction").getItem(0).getField("additionalInstruction").getField("text")),
            )
            .withColumn("patient_instruction", f.col("dosageInstruction").getItem(0).getField("patientInstruction"))
            .withColumn(
                "dosage_frequency",
                f.col("dosageInstruction")
                .getItem(0)
                .getField("timing")
                .getField("repeat")
                .getField("frequency")
                .cast(StringType()),
            )
            .withColumn(
                "dosage_duration",
                f.col("dosageInstruction")
                .getItem(0)
                .getField("timing")
                .getField("repeat")
                .getField("duration")
                .cast(StringType()),
            )
            .withColumn(
                "dosage_period_unit",
                f.col("dosageInstruction").getItem(0).getField("timing").getField("repeat").getField("periodUnit"),
            )
            .withColumn(
                "route_code",
                f.col("dosageInstruction")
                .getItem(0)
                .getField("route")
                .getField("coding")
                .getItem(0)
                .getField("code"),
            )
            .withColumn(
                "route_system",
                f.col("dosageInstruction")
                .getItem(0)
                .getField("route")
                .getField("coding")
                .getItem(0)
                .getField("system"),
            )
            .withColumn(
                "route_desc",
                f.when(
                    f.col("dosageInstruction")
                    .getItem(0)
                    .getField("route")
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("dosageInstruction")
                    .getItem(0)
                    .getField("route")
                    .getField("coding")
                    .getItem(0)
                    .getField("display"),
                ).otherwise(f.col("dosageInstruction").getItem(0).getField("route").getField("text")),
            )
            .withColumn(
                "site_code",
                f.col("dosageInstruction").getItem(0).getField("site").getField("coding").getItem(0).getField("code"),
            )
            .withColumn(
                "site_system",
                f.col("dosageInstruction")
                .getItem(0)
                .getField("site")
                .getField("coding")
                .getItem(0)
                .getField("system"),
            )
            .withColumn(
                "site_desc",
                f.when(
                    f.col("dosageInstruction")
                    .getItem(0)
                    .getField("site")
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("dosageInstruction")
                    .getItem(0)
                    .getField("site")
                    .getField("coding")
                    .getItem(0)
                    .getField("display"),
                ).otherwise(f.col("dosageInstruction").getItem(0).getField("site").getField("text")),
            )
            .withColumn(
                "dosage_type_code",
                f.col("dosageInstruction")
                .getItem(0)
                .getField("doseAndRate")
                .getItem(0)
                .getField("type")
                .getField("coding")
                .getItem(0)
                .getField("code"),
            )
            .withColumn(
                "dosage_type_system",
                f.col("dosageInstruction")
                .getItem(0)
                .getField("doseAndRate")
                .getItem(0)
                .getField("type")
                .getField("coding")
                .getItem(0)
                .getField("system"),
            )
            .withColumn(
                "dosage_type_desc",
                f.when(
                    f.col("dosageInstruction")
                    .getItem(0)
                    .getField("doseAndRate")
                    .getItem(0)
                    .getField("type")
                    .getField("coding")
                    .getItem(0)
                    .getField("display")
                    .isNotNull(),
                    f.col("dosageInstruction")
                    .getItem(0)
                    .getField("doseAndRate")
                    .getItem(0)
                    .getField("type")
                    .getField("coding")
                    .getItem(0)
                    .getField("display"),
                ).otherwise(
                    f.col("dosageInstruction")
                    .getItem(0)
                    .getField("doseAndRate")
                    .getItem(0)
                    .getField("type")
                    .getField("text")
                ),
            )
            .withColumn(
                "dose_quantity_value",
                f.col("dosageInstruction")
                .getItem(0)
                .getField("doseAndRate")
                .getItem(0)
                .getField("doseQuantity")
                .getField("value")
                .cast(StringType()),
            )
            .withColumn(
                "dose_quantity_unit",
                f.col("dosageInstruction")
                .getItem(0)
                .getField("doseAndRate")
                .getItem(0)
                .getField("doseQuantity")
                .getField("unit"),
            )
            .withColumn(
                "dose_rate_quantity_value",
                f.col("dosageInstruction")
                .getItem(0)
                .getField("doseAndRate")
                .getItem(0)
                .getField("rateQuantity")
                .getField("value")
                .cast(StringType()),
            )
            .withColumn(
                "dose_rate_quantity_unit",
                f.col("dosageInstruction")
                .getItem(0)
                .getField("doseAndRate")
                .getItem(0)
                .getField("rateQuantity")
                .getField("unit"),
            )
            .withColumn(
                "dose_range_low",
                f.col("dosageInstruction")
                .getItem(0)
                .getField("doseAndRate")
                .getItem(0)
                .getField("rateRange")
                .getField("low")
                .cast(StringType()),
            )
            .withColumn(
                "dose_range_high",
                f.col("dosageInstruction")
                .getItem(0)
                .getField("doseAndRate")
                .getItem(0)
                .getField("rateRange")
                .getField("high")
                .cast(StringType()),
            )
            .withColumn("updated_user", f.lit(ANALYTICAL_PIPELINE_USER).cast(StringType()))
            .withColumn("updated_ts", f.current_timestamp())
        )

        medication_request_final_df = medication_request_df.select(
            "medication_request_fhir_id",
            "external_id",
            "status",
            "intent",
            "status_reason_code",
            "status_reason_system",
            "status_reason_desc",
            "category_system",
            "category_code",
            "category_desc",
            "priority",
            "medication_code",
            "medication_system",
            "medication_desc",
            "medication_fhir_id",
            "patient_fhir_id",
            "encounter_fhir_id",
            "authored_on",
            "provider_fhir_id",
            "reason_code",
            "reason_system",
            "reason_desc",
            "validity_period_start_date",
            "validity_period_end_date",
            "number_of_repeats_allow",
            "quantity_value",
            "quantity_unit",
            "quantity_system",
            "quantity_code",
            "expected_supply_value",
            "expected_supply_unit",
            "expected_supply_system",
            "expected_supply_code",
            "organization_fhir_id",
            "dosage_sequence",
            "dosage_text",
            "additional_instruction_code",
            "additional_instruction_system",
            "additional_instruction_desc",
            "patient_instruction",
            "dosage_frequency",
            "dosage_duration",
            "dosage_period_unit",
            "route_code",
            "route_system",
            "route_desc",
            "site_code",
            "site_system",
            "site_desc",
            "dosage_type_code",
            "dosage_type_system",
            "dosage_type_desc",
            "dose_quantity_value",
            "dose_quantity_unit",
            "dose_rate_quantity_value",
            "dose_rate_quantity_unit",
            "dose_range_low",
            "dose_range_high",
            "updated_user",
            "updated_ts",
        )

        # expose final dataframe into temp view
        temp_view = f"medication_request_updates_{generate_random_string()}"
        medication_request_final_df.createOrReplaceTempView(temp_view)

        delta_table_location = os.path.join(delta_schema_location, "medication_request")

        # build and run merge query
        merge_query = f"""MERGE INTO delta.`{delta_table_location}`
        USING {temp_view}
        ON delta.`{delta_table_location}`.medication_request_fhir_id = {temp_view}.medication_request_fhir_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
        """
        spark.sql(merge_query)

        return medication_request_final_df

    return inner
