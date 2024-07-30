import json
import os

import click
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import Row
from pyspark.sql import functions as f

from dependencies.spark import add_storage_context, start_spark
from utils.constants import ADJUDICATION_CATEGORY
from utils.enums import ResourceType
from utils.transformation import (
    transform_date,
    transform_date_time,
    transform_gender,
    transform_to_float,
)
from utils.utils import exit_with_error, load_config, upload_bundle_files


ORG_JINJA_TEMPLATE = "organization.j2"
PAT_JINJA_TEMPLATE = "patient.j2"
PRC_JINJA_TEMPLATE = "practitioner.j2"
MED_JINJA_TEMPLATE = "medication.j2"
MED_REQ_JINJA_TEMPLATE = "medication_request.j2"
COV_JINJA_TEMPLATE = "coverage.j2"
CLM_JINJA_TEMPLATE = "claim.j2"
CLM_RES_JINJA_TEMPLATE = "claim_response.j2"


def _transform_resource(data_dict: dict, template: str, resource_type, transformer: FHIRTransformer) -> dict:
    transformer.load_template(template)
    return json.loads(transformer.render_resource(resource_type, data_dict))


# Patient Resource
def _transform_patient(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("patient_internal_id", "")
    subscrier_id = row_dict.get("subscriber_id", "")
    member_suffix = row_dict.get("member_id_suffix", "")
    member_id = row_dict.get("member_id", "")
    if not member_id and (subscrier_id and member_suffix):
        member_id = subscrier_id + member_suffix
    data_dict["member_id"] = member_id
    data_dict["mbi"] = row_dict.get("member_mbi", "")
    data_dict["medicaid_id"] = row_dict.get("member_medicaid_id", "")
    data_dict["subscriber_id"] = subscrier_id
    data_dict["assigner_organization_id"] = row_dict.get("provider_org_internal_id", "")
    data_dict["ssn"] = row_dict.get("member_ssn", "")
    data_dict["mrn"] = row_dict.get("member_mrn", "")
    data_dict["source_id"] = row_dict.get("member_internal_id", "")
    data_dict["lastname"] = row_dict.get("member_last_name", "")
    data_dict["firstname"] = row_dict.get("member_first_name", "")
    data_dict["middleinitials"] = row_dict.get("member_middle_initial", "")
    data_dict["dob"] = row_dict.get("member_dob", "")
    data_dict["gender"] = row_dict.get("member_gender", "")
    data_dict["street_address_1"] = row_dict.get("member_address_line_1", "")
    data_dict["street_address_2"] = row_dict.get("member_address_line_2", "")
    data_dict["city"] = row_dict.get("member_city", "")
    data_dict["district_code"] = row_dict.get("member_county", "")
    data_dict["state"] = row_dict.get("member_state", "")
    data_dict["zip"] = row_dict.get("member_zip", "")
    data_dict["organization_id"] = row_dict.get("provider_org_internal_id", "")

    return _transform_resource(
        data_dict=data_dict,
        template=PAT_JINJA_TEMPLATE,
        resource_type=ResourceType.Patient.value,
        transformer=transformer,
    )


# Organization Resource (Insurer)
def _transform_insurer_organization(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("insurer_internal_id", "")
    data_dict["source_id"] = row_dict.get("insurance_company_id", "")
    data_dict["name"] = row_dict.get("insurance_company_name", "")
    data_dict["type_code"] = "pay"
    data_dict["type_display"] = "Payer"
    data_dict["type_system"] = "http://hl7.org/fhir/ValueSet/organization-type"
    data_dict["type_text"] = "Payer"

    return _transform_resource(
        data_dict=data_dict,
        template=ORG_JINJA_TEMPLATE,
        resource_type=ResourceType.Organization.value,
        transformer=transformer,
    )


# Coverage Resource
def _transform_coverage(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("coverage_internal_id", "")
    data_dict["source_id"] = row_dict.get("alternate_member_id", "")
    data_dict["beneficiary_patient_id"] = row_dict.get("patient_internal_id", "")
    data_dict["insurer_organization_id"] = row_dict.get("insurer_internal_id", "")
    member_id = row_dict.get("member_id", "")
    subscrier_id = row_dict.get("subscriber_id", "")
    member_suffix = row_dict.get("member_id_suffix", "")
    if not member_id and (subscrier_id and member_suffix):
        member_id = subscrier_id + member_suffix
    data_dict["member_id"] = member_id
    # data_dict["subscriber_id"] = subscrier_id
    data_dict["relationship_code"] = (
        row_dict.get("member_relationship_code", "") if row_dict.get("member_relationship_code", "") else member_suffix
    )
    data_dict["status"] = "active"
    data_dict["kind"] = "insurance"
    return _transform_resource(
        data_dict=data_dict,
        template=COV_JINJA_TEMPLATE,
        resource_type=ResourceType.Coverage.value,
        transformer=transformer,
    )


# Practitioner Resource
def _transform_provider(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("prescriber_internal_id", "")
    data_dict["source_id"] = row_dict.get("render_prescriber_internal_id", "")
    data_dict["npi"] = row_dict.get("render_prescriber_npi", "")
    data_dict["firstname"] = row_dict.get("render_prescriber_first_name", "")
    data_dict["lastname"] = row_dict.get("render_prescriber_last_name", "")
    data_dict["street_address_1"] = row_dict.get("render_prescriber_street_line_1", "")
    data_dict["street_address_2"] = row_dict.get("render_prescriber_street_line_2", "")
    data_dict["city"] = row_dict.get("render_prescriber_city", "")
    data_dict["state"] = row_dict.get("render_prescriber_state", "")
    data_dict["zip"] = row_dict.get("render_prescriber_zip", "")
    return _transform_resource(
        data_dict=data_dict,
        template=PRC_JINJA_TEMPLATE,
        resource_type=ResourceType.Practitioner.value,
        transformer=transformer,
    )


# Organization Resource
def _transform_provider_organization(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("provider_org_internal_id", "")
    data_dict["source_id"] = row_dict.get("provider_org_napb_id", "")
    data_dict["type_code"] = "prov"
    data_dict["type_display"] = "Healthcare Provider"
    data_dict["type_system"] = "http://hl7.org/fhir/ValueSet/organization-type"
    # data_dict["type_text"] = "prov"
    return _transform_resource(
        data_dict=data_dict,
        template=ORG_JINJA_TEMPLATE,
        resource_type=ResourceType.Organization.value,
        transformer=transformer,
    )


# Medication Resource
def _transform_medication(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("medication_internal_id", "")
    data_dict["code"] = row_dict.get("drug_ndc_code", "")
    data_dict["code_display"] = row_dict.get("drug_ndc_desc", "")
    data_dict["code_text"] = row_dict.get("generic_branded_ind", "")
    return _transform_resource(
        data_dict=data_dict,
        template=MED_JINJA_TEMPLATE,
        resource_type=ResourceType.Medication.value,
        transformer=transformer,
    )


# MedicationRequest Resource
def _transform_prescription(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("prescription_internal_id", "")
    data_dict["source_id"] = row_dict.get("prescription_number", "")
    data_dict["patient_id"] = row_dict.get("patient_internal_id", "")
    data_dict["medication_id"] = row_dict.get("medication_internal_id", "")
    data_dict["authored_date_time"] = row_dict.get("prescription_date_time", "")
    # data_dict["dispenser_organization_id"] = row_dict.get("dispenser_internal_id", "")
    if row_dict.get("days_supply", ""):
        data_dict["days_supply"] = row_dict.get("days_supply", "")
    if row_dict.get("drug_quantity", ""):
        data_dict["dosages"] = [{"quantity": row_dict.get("drug_quantity", "")}]
    return _transform_resource(
        data_dict=data_dict,
        template=MED_REQ_JINJA_TEMPLATE,
        resource_type=ResourceType.MedicationRequest.value,
        transformer=transformer,
    )


# Claim Resource
def _transform_claim(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("claim_internal_id", "")
    data_dict["patient_id"] = row_dict.get("patient_internal_id", "")
    data_dict["insurer_organization_id"] = row_dict.get("insurer_internal_id", "")
    data_dict["provider_organization_id"] = row_dict.get("insurer_internal_id", "")
    if row_dict.get("previous_claim_id", ""):
        data_dict["related_claim_id"] = row_dict.get("previous_claim_id")
    if row_dict.get("coverage_internal_id", ""):
        data_dict["insurance_sequence"] = 1
        data_dict["insurance_focal"] = "true"
        data_dict["insurance_coverage_id"] = row_dict.get("coverage_internal_id", "")
    # if row_dict.get("claim_patient_paid_amount", ""):
    #     data_dict["patient_paid_amount"] = row_dict.get("claim_patient_paid_amount", "")
    if row_dict.get("claim_total_charges", ""):
        data_dict["total_value"] = row_dict.get("claim_total_charges", "")

    data_dict["type_code"] = row_dict.get("claim_type", "")
    data_dict["source_id"] = row_dict.get("claim_id", "")
    data_dict["related_claims"] = [{"related_claim_id": row_dict.get("previous_claim_id", "")}]
    data_dict["created_date_time"] = row_dict.get("claim_created_date_time", "")
    data_dict["billable_period_start"] = row_dict.get("claim_billable_period_start", "")
    data_dict["billable_period_end"] = row_dict.get("claim_billable_period_end", "")

    data_dict["status"] = "active"
    data_dict["use"] = "claim"

    service_lines = []
    for i, line_entry in enumerate(row_dict.get("service_lines", "")):
        service_line = {"sequence": i + 1}
        service_line["service_period_start"] = line_entry.get("service_start_date", "")
        service_line["service_period_end"] = line_entry.get("service_end_date", "")
        product_or_service = []
        if line_entry.get("drug_ndc_code", ""):
            product_or_service.append(
                {
                    "code": line_entry.get("drug_ndc_code", ""),
                    "display": line_entry.get("drug_ndc_desc", ""),
                }
            )
        service_line["product_or_service"] = product_or_service
        if line_entry.get("service_line_charges"):
            service_line["net_value"] = line_entry.get("service_line_charges")
        if line_entry.get("line_patient_paid_amount"):
            service_line["patient_paid_amount"] = line_entry.get("line_patient_paid_amount")
        service_lines.append(service_line)

    data_dict["service_lines"] = service_lines
    return _transform_resource(
        data_dict=data_dict,
        template=CLM_JINJA_TEMPLATE,
        resource_type=ResourceType.Claim.value,
        transformer=transformer,
    )


def _get_adjudication(amount: float, category: str) -> dict:
    return {
        "amount_value": amount,
        "category_code": ADJUDICATION_CATEGORY.get(category, {}).get("category_code", ""),
        "category_system": ADJUDICATION_CATEGORY.get(category, {}).get("category_system", ""),
        "category_display": ADJUDICATION_CATEGORY.get(category, {}).get("category_display", ""),
    }


# Claim Response Resource
def _transform_claim_response(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("claim_response_internal_id", "")
    data_dict["source_id"] = row_dict.get("claim_id", "")
    data_dict["patient_id"] = row_dict.get("patient_internal_id", "")
    data_dict["organization_id"] = row_dict.get("insurer_internal_id", "")
    data_dict["requestor_practitioner_id"] = row_dict.get("prescriber_internal_id", "")
    data_dict["type_code"] = row_dict.get("claim_type", "")
    data_dict["claim_id"] = row_dict.get("claim_internal_id", "")
    data_dict["disposition"] = row_dict.get("claim_disposition", "")
    data_dict["outcome"] = "complete"
    if row_dict.get("claim_total_charges", ""):
        data_dict["adjudications"] = [_get_adjudication(row_dict.get("claim_total_charges", ""), "submitted")]
    if row_dict.get("claim_total_paid", ""):
        data_dict["payment_value"] = row_dict.get("claim_total_paid", "")
        # Need to review and change this
        # data_dict["payment_type_text"] = row_dict.get("claim_paid_status", "")
        data_dict["payment_type_code"] = row_dict.get("claim_paid_status", "")
        if row_dict.get("claim_paid_date", ""):
            data_dict["payment_date"] = row_dict.get("claim_paid_date", "")
    total_amounts = []
    if row_dict.get("claim_copay_amount", ""):
        total_amounts.append(_get_adjudication(row_dict.get("claim_copay_amount", ""), "copay"))
    data_dict["total_amounts"] = total_amounts
    data_dict["created_date_time"] = row_dict.get("claim_created_date_time", "")

    data_dict["status"] = "active"
    data_dict["use"] = "claim"

    service_lines = []
    for i, line_entry in enumerate(row_dict.get("service_lines", "")):
        service_line = {"sequence": i + 1}
        adjudications = []
        if line_entry.get("service_line_charges", ""):
            adjudications.append(_get_adjudication(row_dict.get("service_line_charges", ""), "submitted"))
        if line_entry.get("line_amount_paid", ""):
            adjudications.append(_get_adjudication(row_dict.get("line_amount_paid", ""), "benefit"))
        if line_entry.get("line_copay_amount", ""):
            adjudications.append(_get_adjudication(row_dict.get("line_copay_amount", ""), "copay"))
        # if line_entry.get("line_patient_paid_amount", ""):
        #     adjudications.append(
        #         _get_adjudication(row_dict.get("line_patient_paid_amount", ""), "patient-paid")
        #     )
        # if line_entry.get("medicare_paid_amount", ""):
        #     adjudications.append(
        #         _get_adjudication(row_dict.get("medicare_paid_amount", ""), "medicare-paid")
        #     )
        service_line["adjudications"] = adjudications
        service_lines.append(service_line)

    data_dict["claim_items"] = service_lines
    return _transform_resource(
        data_dict=data_dict,
        template=CLM_RES_JINJA_TEMPLATE,
        resource_type=ResourceType.ClaimResponse.value,
        transformer=transformer,
    )


def render_resources(row: Row, transformer: FHIRTransformer) -> Row:
    row_dict = row.asDict(recursive=True)

    resources = []
    resources.append(_transform_patient(row_dict, transformer))
    if row_dict.get("insurance_company_id"):
        resources.append(_transform_insurer_organization(row_dict, transformer))
    resources.append(_transform_coverage(row_dict, transformer))
    if row_dict.get("provider_org_napb_id"):
        resources.append(_transform_provider_organization(row_dict, transformer))
    if row_dict.get("drug_ndc_code"):
        resources.append(_transform_medication(row_dict, transformer))
    if row_dict.get("render_prescriber_npi"):
        resources.append(_transform_provider(row_dict, transformer))
    if row_dict.get("prescription_number"):
        resources.append(_transform_prescription(row_dict, transformer))
    resources.append(_transform_claim(row_dict, transformer))
    resources.append(_transform_claim_response(row_dict, transformer))
    return Row(
        **{
            "resource_bundle": json.dumps(
                {
                    "resourceType": "Bundle",
                    "id": row_dict.get("bundle_id"),
                    "type": "batch",
                    "entry": resources,
                }
            )
        }
    )


@click.command()
@click.option(
    "--app-name",
    "-a",
    help="spark app name",
    default="rx_claim_stage_to_fhirbundle_job",
)
@click.option(
    "--config-file-path",
    "-c",
    help="application config file path",
    default=None,
)
def main(app_name, config_file_path=None):
    app_name = os.environ.get("SPARK_APP_NAME", app_name)

    spark_config = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    }

    # start spark session and logger
    spark, log = start_spark(app_name, spark_config)
    spark.sparkContext.addPyFile("/app/python-deps.zip")
    log.warn(f"spark app {app_name} started")

    # if config file is provided, extract details into a dict
    # config file should be a yaml file
    config = {}
    if config_file_path:
        config = load_config(config_file_path)
    log.info(f"config: {config}")

    try:
        # validate environment variables and config parameters
        delta_schema_location = os.environ.get("DELTA_SCHEMA_LOCATION", config.get("delta_schema_location", ""))
        if not delta_schema_location:
            exit_with_error(log, "delta schema location should be provided!")

        delta_table_name = os.environ.get("DELTA_TABLE_NAME", config.get("delta_table_name", "rx_claim"))
        if not delta_table_name:
            exit_with_error(log, "delta table location should be provided!")

        # construct delta table location
        delta_table_location = os.path.join(delta_schema_location, delta_table_name)
        log.warn(f"delta_table_location: {delta_table_location}")

        file_batch_id = os.environ.get("FILE_BATCH_ID", config.get("file_batch_id", ""))
        if not file_batch_id:
            exit_with_error(log, "file batch id should be provided!")
        log.warn(f"file_batch_id: {file_batch_id}")

        file_source = os.environ.get("FILE_SOURCE", config.get("file_source", ""))
        if not file_source:
            exit_with_error(log, "file source should be provided!")
        log.warn(f"file_source: {file_source}")

        landing_path = os.environ.get("LANDING_PATH", config.get("landing_path", ""))
        if not landing_path:
            exit_with_error(log, "landing path should be provided!")
        log.warn(f"landing_path: {landing_path}")

        fhirbundle_landing_path = os.path.join(landing_path, file_source)
        log.warn(f"fhirbundle_landing_path: {fhirbundle_landing_path}")

        pipeline_data_key = os.environ.get("PIPELINE_DATA_KEY", config.get("pipeline_data_key", ""))
        if not pipeline_data_key:
            exit_with_error(log, "pipeline data key should be provided!")

        file_tenant = os.environ.get("FILE_TENANT", config.get("file_tenant", ""))
        log.warn(f"file_tenant: {file_tenant}")

        resource_type = os.environ.get("RESOURCE_TYPE", config.get("resource_type", ""))
        log.warn(f"resource_type: {resource_type}")

        src_file_name = os.environ.get("SRC_FILE_NAME", config.get("src_file_name", ""))
        log.warn(f"src_file_name: {src_file_name}")

        src_organization_id = os.environ.get("SRC_ORGANIZATION_ID", config.get("src_organization_id", ""))
        log.warn(f"src_organization_id: {src_organization_id}")

        # Change FHIRBUNDLE to FHIRBUNDLE_BULK
        fhirbundle_landing_path = fhirbundle_landing_path.replace("FHIRBUNDLE", "FHIRBUNDLE_BULK")

        # Construct fhir bundle temp path
        fhir_temp_path = fhirbundle_landing_path.replace("landing", "temporary")
        fhir_bundle_temp_path = os.path.join(fhir_temp_path, file_batch_id)

        # construct metadata
        metadata = {
            "file_tenant": file_tenant,
            "file_source": file_source,
            "resource_type": resource_type,
            "file_batch_id": file_batch_id,
            "src_file_name": src_file_name,
            "src_organization_id": src_organization_id,
        }

        # add storage context in spark session
        spark = add_storage_context(spark, [delta_table_location])

        # load the records from delta table location
        log.warn("load records from delta table location")
        data_df = (
            spark.read.format("delta")
            .load(delta_table_location)
            .filter(f.col("file_batch_id") == file_batch_id)
            .drop(
                "batch_id",
                "source_system",
                "file_name",
                "status",
                "created_user",
                "created_ts",
                "updated_user",
                "updated_ts",
            )
            .fillna("")
        )

        pay_df = data_df.filter(f.col("insurance_company_id") == "")
        if not pay_df.isEmpty():
            exit_with_error(log, "insurance company id must be a not-null value.")
        pat_df = data_df.filter(f.col("member_id") == "")
        if not pat_df.isEmpty():
            exit_with_error(log, "member id must be not-null values.")

        data_df = (
            data_df.withColumn(
                "member_id",
                f.expr(f"aes_decrypt(unhex(member_id) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "member_mbi",
                f.expr(f"aes_decrypt(unhex(member_mbi) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "member_medicaid_id",
                f.expr(f"aes_decrypt(unhex(member_medicaid_id) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "alternate_member_id",
                f.expr(f"aes_decrypt(unhex(alternate_member_id) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "subscriber_id",
                f.expr(f"aes_decrypt(unhex(subscriber_id) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "member_id_suffix",
                f.expr(f"aes_decrypt(unhex(member_id_suffix) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "member_ssn",
                f.expr(f"aes_decrypt(unhex(member_ssn) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "member_mrn",
                f.expr(f"aes_decrypt(unhex(member_mrn) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "member_last_name",
                f.expr(f"aes_decrypt(unhex(member_last_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "member_first_name",
                f.expr(f"aes_decrypt(unhex(member_first_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "member_middle_initial",
                f.expr(f"aes_decrypt(unhex(member_middle_initial) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "member_dob",
                f.expr(f"aes_decrypt(unhex(member_dob) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "member_address_line_1",
                f.expr(f"aes_decrypt(unhex(member_address_line_1) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "member_address_line_2",
                f.expr(f"aes_decrypt(unhex(member_address_line_2) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "member_city",
                f.expr(f"aes_decrypt(unhex(member_city) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "member_state",
                f.expr(f"aes_decrypt(unhex(member_state) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "member_zip",
                f.expr(f"aes_decrypt(unhex(member_zip) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "provider_org_napb_id",
                f.expr(f"aes_decrypt(unhex(provider_org_napb_id),'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "render_prescriber_npi",
                f.expr(f"aes_decrypt(unhex(render_prescriber_npi) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "render_prescriber_first_name",
                f.expr(f"aes_decrypt(unhex(render_prescriber_first_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "render_prescriber_last_name",
                f.expr(f"aes_decrypt(unhex(render_prescriber_last_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
        )

        data_df = (
            data_df.withColumn("patient_internal_id", f.expr("uuid()"))
            .withColumn(
                "insurer_internal_id",
                f.when(f.col("insurance_company_id") != "", f.expr("uuid()")).otherwise(f.lit("")),
            )
            .withColumn("coverage_internal_id", f.expr("uuid()"))
            .withColumn(
                "prescriber_internal_id",
                f.when(f.col("render_prescriber_npi") != "", f.expr("uuid()")).otherwise(f.lit("")),
            )
            .withColumn(
                "provider_org_internal_id",
                f.when(f.col("provider_org_napb_id") != "", f.expr("uuid()")).otherwise(f.lit("")),
            )
            .withColumn(
                "medication_internal_id",
                f.when(f.col("drug_ndc_code") != "", f.expr("uuid()")).otherwise(f.lit("")),
            )
            .withColumn(
                "prescription_internal_id",
                f.when(f.col("prescription_number") != "", f.expr("uuid()")).otherwise(f.lit("")),
            )
            .withColumn("claim_internal_id", f.expr("uuid()"))
            .withColumn("bundle_id", f.expr("uuid()"))
            .withColumn("claim_response_internal_id", f.expr("uuid()"))
            .withColumn("member_dob", transform_date(f.col("member_dob")))
            .withColumn("claim_paid_date", transform_date(f.col("claim_paid_date")))
            .withColumn("prescription_date_time", transform_date_time(f.col("prescription_date")))
            .withColumn("claim_created_date_time", transform_date_time(f.col("claim_billed_date")))
            .withColumn("claim_billable_period_start", transform_date_time(f.col("claim_start_date")))
            .withColumn("claim_billable_period_end", transform_date_time(f.col("claim_end_date")))
            .withColumn("service_start_date", transform_date_time(f.col("service_start_date")))
            .withColumn("service_end_date", transform_date_time(f.col("service_end_date")))
            .withColumn("member_gender", transform_gender(f.col("member_gender")))
            .withColumn("days_supply", transform_to_float(f.col("days_supply")))
            .withColumn("drug_quantity", transform_to_float(f.col("drug_quantity")))
            .withColumn("claim_patient_paid_amount", transform_to_float(f.col("claim_patient_paid_amount")))
            .withColumn("claim_total_charges", transform_to_float(f.col("claim_total_charges")))
            .withColumn("service_line_charges", transform_to_float(f.col("service_line_charges")))
            .withColumn("line_patient_paid_amount", transform_to_float(f.col("line_patient_paid_amount")))
            .withColumn("claim_total_paid", transform_to_float(f.col("claim_total_paid")))
            .withColumn("claim_copay_amount", transform_to_float(f.col("claim_copay_amount")))
            .withColumn("line_amount_paid", transform_to_float(f.col("line_amount_paid")))
            .withColumn("line_copay_amount", transform_to_float(f.col("line_copay_amount")))
            .withColumn("file_tenant", f.lit(file_tenant))
            .withColumn("file_source", f.lit(file_source))
            .withColumn("resource_type", f.lit(resource_type))
            .withColumn("file_batch_id", f.lit(file_batch_id))
            .withColumn("src_file_name", f.lit(src_file_name))
            .withColumn("src_organization_id", f.lit(src_organization_id))
        )

        group_by_cols = (
            "claim_id",
            "claim_type",
            "claim_disposition",
            "patient_internal_id",
            "coverage_internal_id",
            "insurer_internal_id",
            "prescriber_internal_id",
            "primary_payer_code",
            "claim_billed_date",
            "claim_start_date",
            "claim_end_date",
            "claim_total_charges",
            "claim_adjudication_status",
            "claim_total_paid",
            "claim_paid_date",
            "claim_paid_status",
            "claim_copay_amount",
            "claim_patient_paid_amount",
            "claim_other_payer_paid",
        )
        claim_df = data_df.groupBy(*group_by_cols).agg(
            f.collect_list(
                f.struct(
                    f.col("line_number"),
                    f.col("drug_ndc_code"),
                    f.col("service_start_date"),
                    f.col("service_end_date"),
                    f.col("service_line_charges"),
                    f.col("line_adjudication_status"),
                    f.col("line_amount_paid"),
                    f.col("service_line_paid_date"),
                    f.col("line_payment_level"),
                    f.col("line_copay_amount"),
                    f.col("line_patient_paid_amount"),
                    # f.col("medicare_paid_amount"),
                    # f.col("line_allowed_units"),
                )
            ).alias("service_lines")
        )
        data_df = (
            data_df.join(claim_df, on=["claim_id"], how="left")
            .select(data_df["*"], claim_df["service_lines"])
            .fillna("")
        )

        transformer = FHIRTransformer()
        data_rdd = data_df.rdd.map(lambda row: render_resources(row, transformer))
        resources_df = spark.createDataFrame(data_rdd)

        resources_df.write.mode("overwrite").text(fhir_bundle_temp_path)

        upload_bundle_files(
            fhir_bundle_temp_path=fhir_bundle_temp_path,
            landing_path=fhirbundle_landing_path,
            metadata=metadata,
            enc_data_key=pipeline_data_key,
        )

        log.warn(
            f"spark job {app_name} completed successfully, "
            f"patient from file with batch id {file_batch_id} transformed "
            f"into fhir bundle and copied to {fhirbundle_landing_path}."
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
