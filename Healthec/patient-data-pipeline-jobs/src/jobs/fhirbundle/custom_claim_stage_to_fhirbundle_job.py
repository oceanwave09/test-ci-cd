import json
import os

import click
from fhirtransformer.transformer import FHIRTransformer
from pyspark.sql import Row
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w

from dependencies.spark import add_storage_context, start_spark
from utils.constants import (
    ADJUDICATION_CATEGORY,
    CPT_CODE_SYSTEM,
    HCPCS_CODE_SYSTEM,
    ICD10_CODE_SYSTEM,
    PROCEDURE_CPT_SYSTEM,
)
from utils.enums import ResourceType
from utils.transformation import transform_date, transform_date_time, transform_gender, transform_to_float
from utils.utils import exit_with_error, load_config, upload_bundle_files

ORG_JINJA_TEMPLATE = "organization.j2"
PAT_JINJA_TEMPLATE = "patient.j2"
PRC_JINJA_TEMPLATE = "practitioner.j2"
COV_JINJA_TEMPLATE = "coverage.j2"
ENC_JINJA_TEMPLATE = "encounter.j2"
COND_JINJA_TEMPLATE = "condition.j2"
PROC_JINJA_TEMPLATE = "procedure.j2"
CLM_JINJA_TEMPLATE = "claim.j2"
CLM_RES_JINJA_TEMPLATE = "claim_response.j2"
LOC_JINJA_TEMPLATE = "location.j2"


def _transform_resource(data_dict: dict, template: str, resource_type, transformer: FHIRTransformer) -> dict:
    transformer.load_template(template)
    return json.loads(transformer.render_resource(resource_type, data_dict))


# Patient Resource
def _transform_patient(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("patient_internal_id", "")
    data_dict["member_id"] = row_dict.get("member_id", "")
    data_dict["mbi"] = row_dict.get("member_mbi", "")
    data_dict["medicaid_id"] = row_dict.get("member_medicaid_id", "")
    data_dict["assigner_organization_id"] = row_dict.get("prov_org_internal_id", "")
    data_dict["ssn"] = row_dict.get("member_ssn", "")
    data_dict["mrn"] = row_dict.get("member_mrn", "")
    data_dict["source_id"] = row_dict.get("member_internal_id", "")
    data_dict["lastname"] = row_dict.get("member_last_name", "")
    data_dict["firstname"] = row_dict.get("member_first_name", "")
    data_dict["middleinitials"] = row_dict.get("member_middle_initial", "")
    data_dict["dob"] = row_dict.get("member_dob", "")
    data_dict["gender"] = row_dict.get("member_gender", "")
    data_dict["contact_relationship_code"] = row_dict.get("member_relationship_code", "")
    data_dict["street_address_1"] = row_dict.get("member_address_line_1", "")
    data_dict["street_address_2"] = row_dict.get("member_address_line_2", "")
    data_dict["city"] = row_dict.get("member_city", "")
    data_dict["state"] = row_dict.get("member_state", "")
    data_dict["zip"] = row_dict.get("member_zip", "")
    data_dict["organization_id"] = row_dict.get("prov_org_internal_id", "")
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
    data_dict["beneficiary_patient_id"] = row_dict.get("patient_internal_id", "")
    data_dict["insurer_organization_id"] = row_dict.get("insurer_internal_id", "")
    data_dict["assigner_organization_id"] = row_dict.get("prov_org_internal_id", "")
    data_dict["member_id"] = row_dict.get("member_id", "")
    data_dict["relationship_code"] = row_dict.get("member_relationship_code", "")
    data_dict["status"] = "active"
    data_dict["kind"] = "insurance"

    return _transform_resource(
        data_dict=data_dict,
        template=COV_JINJA_TEMPLATE,
        resource_type=ResourceType.Coverage.value,
        transformer=transformer,
    )


# Practitioner Resource Attending Provider
def _transform_attending_provider(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    # data_dict["internal_id"] = row_dict.get("attending_provider_internal_id", "")
    # data_dict["source_id"] = row_dict.get("attending_internal_id", "")
    data_dict["npi"] = row_dict.get("attending_npi", "")
    data_dict["firstname"] = row_dict.get("attending_first_name", "")
    data_dict["lastname"] = row_dict.get("attending_last_name", "")

    return _transform_resource(
        data_dict=data_dict,
        template=PRC_JINJA_TEMPLATE,
        resource_type=ResourceType.Practitioner.value,
        transformer=transformer,
    )


# Practitioner Resource Referring Provider
def _transform_referring_provider(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    # data_dict["internal_id"] = row_dict.get("referring_provider_internal_id", "")
    # data_dict["source_id"] = row_dict.get("referring_internal_id", "")
    data_dict["npi"] = row_dict.get("referring_npi", "")
    data_dict["firstname"] = row_dict.get("referring_first_name", "")
    data_dict["lastname"] = row_dict.get("referring_last_name", "")

    return _transform_resource(
        data_dict=data_dict,
        template=PRC_JINJA_TEMPLATE,
        resource_type=ResourceType.Practitioner.value,
        transformer=transformer,
    )


# Practitioner Resource Prescriber Provider
def _transform_prescriber(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    # data_dict["internal_id"] = row_dict.get("prescriber_internal_id", "")
    # data_dict["source_id"] = row_dict.get("render_prescriber_internal_id", "")
    data_dict["npi"] = row_dict.get("prescribing_npi", "")
    # data_dict["firstname"] = row_dict.get("render_prescriber_first_name", "")
    # data_dict["lastname"] = row_dict.get("render_prescriber_last_name", "")

    return _transform_resource(
        data_dict=data_dict,
        template=PRC_JINJA_TEMPLATE,
        resource_type=ResourceType.Practitioner.value,
        transformer=transformer,
    )


# Location Resource (Facility)
def _transform_facility(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("facility_internal_id", "")
    data_dict["group_npi"] = row_dict.get("facility_group_npi", "")
    data_dict["name"] = row_dict.get("facility_name", "")
    data_dict["organization_id"] = row_dict.get("insurer_internal_id", "")
    return _transform_resource(
        data_dict=data_dict,
        template=LOC_JINJA_TEMPLATE,
        resource_type=ResourceType.Location.value,
        transformer=transformer,
    )


def _transform_prov_organization(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("prov_org_internal_id", "")
    data_dict["source_id"] = row_dict.get("facility_internal_id", "")
    data_dict["name"] = row_dict.get("facility_name", "")
    return _transform_resource(
        data_dict=data_dict,
        template=ORG_JINJA_TEMPLATE,
        resource_type=ResourceType.Organization.value,
        transformer=transformer,
    )


# Encounter Resource
def _transform_encounter(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    if row_dict.get("encounter_internal_id", ""):
        data_dict["internal_id"] = row_dict.get("encounter_internal_id", "")
    elif len(row_dict.get("service_lines", [])) > 0:
        data_dict["internal_id"] = row_dict.get("service_lines", [])[0].get("encounter_internal_id", "")
    data_dict["patient_id"] = row_dict.get("patient_internal_id", "")
    data_dict["location_id"] = row_dict.get("facility_internal_id", "")
    data_dict["period_start_date"] = (
        row_dict.get("admission_date", "")
        if row_dict.get("admission_date", "")
        else row_dict.get("svc_line_start_date", "")
    )
    data_dict["period_end_date"] = (
        row_dict.get("discharge_date", "")
        if row_dict.get("discharge_date", "")
        else row_dict.get("svc_line_end_date", "")
    )
    data_dict["type_text"] = row_dict.get("admission_type", "")
    if row_dict.get("place_of_service"):
        data_dict["physical_type_code"] = row_dict.get("place_of_service", "")
        data_dict[
            "physical_type_system"
        ] = "https://www.cms.gov/Medicare/Coding/place-of-service-codes/Place_of_Service_Code_Set"
    data_dict["admit_source_text"] = row_dict.get("admission_source", "")
    data_dict["discharge_disposition_text"] = row_dict.get("discharge_status", "")
    data_dict["class_code"] = "AMB"
    data_dict["class_display"] = "ambulatory"

    return _transform_resource(
        data_dict=data_dict,
        template=ENC_JINJA_TEMPLATE,
        resource_type=ResourceType.Encounter.value,
        transformer=transformer,
    )


# Condition Resource (Diagnoses)
def _transform_condition(row_dict: dict, transformer: FHIRTransformer) -> dict:
    conditions = []
    for condition in row_dict.get("diagnosis_structs", ""):
        if condition.get("code", ""):
            data_dict = {}
            data_dict["internal_id"] = condition.get("condition_id", "")
            data_dict["patient_id"] = condition.get("patient_internal_id", "")
            data_dict["code"] = condition.get("code", "")
            data_dict["code_system"] = ICD10_CODE_SYSTEM
            data_dict["clinical_status"] = "active"
            conditions.append(
                _transform_resource(
                    data_dict=data_dict,
                    template=COND_JINJA_TEMPLATE,
                    resource_type=ResourceType.Condition.value,
                    transformer=transformer,
                )
            )

    return conditions


# Procedure Resource
def _transform_procedure(row_dict: dict, transformer: FHIRTransformer) -> dict:
    procedures = []
    for procedure in row_dict.get("procedure_structs", ""):
        if procedure.get("code", ""):
            data_dict = {}
            data_dict["internal_id"] = procedure.get("procedure_id", "")
            data_dict["patient_id"] = procedure.get("patient_internal_id", "")
            data_dict["code"] = procedure.get("code", "")
            data_dict["code_system"] = PROCEDURE_CPT_SYSTEM
            procedures.append(
                _transform_resource(
                    data_dict=data_dict,
                    template=PROC_JINJA_TEMPLATE,
                    resource_type=ResourceType.Procedure.value,
                    transformer=transformer,
                )
            )

    return procedures


def _transform_claim(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("claim_internal_id", "")
    data_dict["patient_id"] = row_dict.get("patient_internal_id", "")
    data_dict["insurer_organization_id"] = row_dict.get("insurer_internal_id", "")
    data_dict["provider_organization_id"] = row_dict.get("prov_org_internal_id", "")
    if row_dict.get("previous_claim_id", ""):
        data_dict["related_claim_id"] = row_dict.get("previous_claim_id")
    if row_dict.get("coverage_internal_id", ""):
        data_dict["insurance_sequence"] = 1
        data_dict["insurance_focal"] = "true"
        data_dict["insurance_coverage_id"] = row_dict.get("coverage_internal_id", "")

    diagnoses = []
    if row_dict.get("diagnosis_structs", ""):
        for diag_entry in row_dict.get("diagnosis_structs", []):
            diagnoses.append(
                {
                    "sequence": diag_entry.get("sequence"),
                    "diagnosis_condition_id": diag_entry.get("condition_id", ""),
                }
            )
    data_dict["diagnoses"] = diagnoses

    procedures = []
    if row_dict.get("procedure_structs", ""):
        for proc_entry in row_dict.get("procedure_structs", []):
            procedures.append(
                {
                    "sequence": proc_entry.get("sequence"),
                    "procedure_id": proc_entry.get("procedure_id", ""),
                }
            )
    data_dict["procedures"] = procedures
    care_team = []
    care_team_seq = 0
    # attending provider
    if row_dict.get("attending_npi", ""):
        care_team_seq = care_team_seq + 1
        care_team.append(
            {
                "sequence": care_team_seq,
                # "provider_practitioner_id": row_dict.get("attending_provider_internal_id", ""),
                "provider_role": "attending",
                "provider_role_system": "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimCareTeamRole",
            }
        )

    # referring provider
    if row_dict.get("referring_npi", ""):
        care_team_seq = care_team_seq + 1
        care_team.append(
            {
                "sequence": care_team_seq,
                # "provider_practitioner_id": row_dict.get("referring_provider_internal_id", ""),
                "provider_role": "referring",
                "provider_role_system": "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimCareTeamRole",
            }
        )

    # render prescriber npi provider
    if row_dict.get("render_prescriber_npi", ""):
        care_team_seq = care_team_seq + 1
        care_team.append(
            {
                "sequence": care_team_seq,
                # "provider_practitioner_id": row_dict.get("prescriber_internal_id", ""),
                "provider_role": "prescribing",
                "provider_role_system": "http://hl7.org/fhir/us/carin-bb/CodeSystem/C4BBClaimCareTeamRole",
            }
        )
    data_dict["care_team"] = care_team
    # if row_dict.get("claim_patient_paid_amount", ""):
    # data_dict["patient_paid_amount"] = row_dict.get("claim_patient_paid_amount", "")
    if row_dict.get("claim_total_charges", ""):
        data_dict["total_value"] = row_dict.get("claim_total_charges", "")

    data_dict["type_code"] = row_dict.get("claim_type", "")
    data_dict["source_id"] = row_dict.get("claim_id", "")
    data_dict["related_claims"] = [{"related_claim_id": row_dict.get("previous_claim_id", "")}]
    data_dict["created_date_time"] = row_dict.get("claim_billed_date", "")
    data_dict["billable_period_start"] = row_dict.get("claim_start_date", "")
    data_dict["billable_period_end"] = row_dict.get("claim_end_date", "")

    data_dict["status"] = "active"
    data_dict["use"] = "claim"

    service_lines = []
    for i, line_entry in enumerate(row_dict.get("service_lines", [])):
        service_line = {"sequence": i + 1}
        if line_entry.get("diagnosis_sequence", ""):
            service_line["diagnosis_sequence"] = line_entry.get("diagnosis_sequence", "")
        if line_entry.get("procedure_sequence", ""):
            service_line["procedure_sequence"] = line_entry.get("procedure_sequence", "")
        # if care_team_seq > 0:
        #     service_line["careteam_sequence"] = ",".join([str(n) for n in range(1, care_team_seq + 1)])
        service_line["revenue_code"] = line_entry.get("revenue_code", "")
        if line_entry.get("svc_line_start_date", ""):
            service_line["service_period_start"] = line_entry.get("svc_line_start_date", "")
        if line_entry.get("svc_line_end_date", ""):
            service_line["service_period_end"] = line_entry.get("svc_line_end_date", "")
        product_or_service = []
        if line_entry.get("cpt_code", ""):
            product_or_service.append(
                {
                    "code": line_entry.get("cpt_code", ""),
                    "display": line_entry.get("cpt_code_description", ""),
                    "system": CPT_CODE_SYSTEM,
                }
            )
        if line_entry.get("hcpcs_code", ""):
            product_or_service.append(
                {
                    "code": line_entry.get("hcpcs_code", ""),
                    "display": line_entry.get("hcpsc_code_description", ""),
                    "system": HCPCS_CODE_SYSTEM,
                }
            )
        service_line["product_or_service"] = product_or_service
        svc_modifiers = []
        if line_entry.get("svc_modifier_1", ""):
            svc_modifiers.append({"modifier_code": line_entry.get("svc_modifier_1", "")})
        if line_entry.get("svc_modifier_2", ""):
            svc_modifiers.append({"modifier_code": line_entry.get("svc_modifier_2", "")})
        if line_entry.get("svc_modifier_3", ""):
            svc_modifiers.append({"modifier_code": line_entry.get("svc_modifier_3", "")})
        if line_entry.get("svc_modifier_4", ""):
            svc_modifiers.append({"modifier_code": line_entry.get("svc_modifier_4", "")})
        service_line["svc_modifiers"] = svc_modifiers
        if line_entry.get("svc_line_units"):
            service_line["quantity_value"] = line_entry.get("svc_line_units")
        if line_entry.get("svc_line_charges"):
            service_line["net_value"] = line_entry.get("svc_line_charges")
        if line_entry.get("encounter_internal_id", ""):
            service_line["encounter_id"] = line_entry.get("encounter_internal_id", "")
        if line_entry.get("facility_internal_id", ""):
            service_line["location_id"] = line_entry.get("facility_internal_id", "")
        service_lines.append(service_line)

    data_dict["service_lines"] = service_lines
    return _transform_resource(
        data_dict=data_dict,
        template=CLM_JINJA_TEMPLATE,
        resource_type=ResourceType.Claim.value,
        transformer=transformer,
    )


# Claim Response Resource
def _get_adjudication(amount: float, category: str) -> dict:
    return {
        "amount_value": amount,
        "category_code": ADJUDICATION_CATEGORY.get(category, {}).get("category_code", ""),
        "category_system": ADJUDICATION_CATEGORY.get(category, {}).get("category_system", ""),
        "category_display": ADJUDICATION_CATEGORY.get(category, {}).get("category_display", ""),
    }


def _transform_claim_response(row_dict: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["internal_id"] = row_dict.get("claim_response_internal_id", "")
    data_dict["source_id"] = row_dict.get("claim_id", "")
    data_dict["patient_id"] = row_dict.get("patient_internal_id", "")
    data_dict["organization_id"] = row_dict.get("insurer_internal_id", "")
    data_dict["requestor_organization_id"] = row_dict.get("prov_org_internal_id", "")
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
        # data_dict["payment_type_code"] = row_dict.get("claim_paid_status", "")
        if row_dict.get("claim_paid_date", ""):
            data_dict["payment_date"] = row_dict.get("claim_paid_date", "")
    total_amounts = []
    if row_dict.get("claim_deduct_amount", ""):
        total_amounts.append(_get_adjudication(row_dict.get("claim_deduct_amount", ""), "deductible"))
    if row_dict.get("claim_copay_amount", ""):
        total_amounts.append(_get_adjudication(row_dict.get("claim_copay_amount", ""), "copay"))
    data_dict["total_amounts"] = total_amounts
    if row_dict.get("claim_paid_date", ""):
        data_dict["created_date_time"] = row_dict.get("claim_paid_date", "")

    data_dict["status"] = "active"
    data_dict["use"] = "claim"

    service_lines = []
    for i, line_entry in enumerate(row_dict.get("service_lines", "")):
        service_line = {"sequence": i + 1}
        adjudications = []
        if line_entry.get("svc_line_charges", ""):
            adjudications.append(_get_adjudication(line_entry.get("svc_line_charges", ""), "submitted"))
        if line_entry.get("svc_line_amount_paid", ""):
            adjudications.append(_get_adjudication(line_entry.get("svc_line_amount_paid", ""), "benefit"))
        if line_entry.get("svc_line_deduct_amount", ""):
            adjudications.append(_get_adjudication(line_entry.get("svc_line_deduct_amount", ""), "deductible"))
        if line_entry.get("svc_line_copay_amount", ""):
            adjudications.append(_get_adjudication(line_entry.get("svc_line_copay_amount", ""), "copay"))
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
    # render resources
    resources.append(_transform_prov_organization(row_dict, transformer))
    resources.append(_transform_insurer_organization(row_dict, transformer))
    resources.append(_transform_patient(row_dict, transformer))
    resources.append(_transform_coverage(row_dict, transformer))
    if row_dict.get("attending_npi"):
        resources.append(_transform_attending_provider(row_dict, transformer))
    if row_dict.get("referring_npi"):
        resources.append(_transform_referring_provider(row_dict, transformer))
    if row_dict.get("prescribing_npi"):
        resources.append(_transform_prescriber(row_dict, transformer))
    if row_dict.get("facility_group_npi"):
        resources.append(_transform_facility(row_dict, transformer))
    resources.append(_transform_encounter(row_dict, transformer))
    if row_dict.get("diagnosis_structs", ""):
        resources.extend(_transform_condition(row_dict, transformer))
    if row_dict.get("procedure_structs", ""):
        resources.extend(_transform_procedure(row_dict, transformer))
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
    default="custom_claim_stage_to_fhirbundle_job",
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
        "spark.sql.legacy.timeParserPolicy": "LEGACY",
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

        delta_table_name = os.environ.get("DELTA_TABLE_NAME", config.get("delta_table_name", "custom_claim"))
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

        pipeline_data_key = os.environ.get("PIPELINE_DATA_KEY", config.get("pipeline_data_key", ""))
        if not pipeline_data_key:
            exit_with_error(log, "pipeline data key should be provided!")

        fhirbundle_landing_path = os.path.join(landing_path, file_source)
        log.warn(f"fhirbundle_landing_path: {fhirbundle_landing_path}")

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

        # decryting personal identity
        data_df = (
            data_df.withColumn(
                "member_mrn",
                f.expr(f"aes_decrypt(unhex(member_mrn) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "member_ssn",
                f.expr(f"aes_decrypt(unhex(member_ssn) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
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
                "member_middle_name",
                f.expr(f"aes_decrypt(unhex(member_middle_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "member_gender",
                f.expr(f"aes_decrypt(unhex(member_gender) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
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
                "member_phone_home",
                f.expr(f"aes_decrypt(unhex(member_phone_home) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "member_email",
                f.expr(f"aes_decrypt(unhex(member_email) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "attributed_last_name",
                f.expr(f"aes_decrypt(unhex(attributed_last_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "attributed_first_name",
                f.expr(f"aes_decrypt(unhex(attributed_first_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "attributed_npi",
                f.expr(f"aes_decrypt(unhex(attributed_npi) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "provider_org_group_npi",
                f.expr(f"aes_decrypt(unhex(provider_org_group_npi) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "attending_last_name",
                f.expr(f"aes_decrypt(unhex(attending_last_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "attending_first_name",
                f.expr(f"aes_decrypt(unhex(attending_first_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "attending_middle_name",
                f.expr(f"aes_decrypt(unhex(attending_middle_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "attending_npi",
                f.expr(f"aes_decrypt(unhex(attending_npi) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "rendering_last_name",
                f.expr(f"aes_decrypt(unhex(rendering_last_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "rendering_first_name",
                f.expr(f"aes_decrypt(unhex(rendering_first_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "rendering_middle_name",
                f.expr(f"aes_decrypt(unhex(rendering_middle_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "rendering_npi",
                f.expr(f"aes_decrypt(unhex(rendering_npi) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
            .withColumn(
                "svc_rendering_last_name",
                f.expr(f"aes_decrypt(unhex(svc_rendering_last_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "svc_rendering_first_name",
                f.expr(f"aes_decrypt(unhex(svc_rendering_first_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "svc_rendering_middle_name",
                f.expr(f"aes_decrypt(unhex(svc_rendering_middle_name) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast(
                    "string"
                ),
            )
            .withColumn(
                "svc_rendering_npi",
                f.expr(f"aes_decrypt(unhex(svc_rendering_npi) ,'{pipeline_data_key}', 'ECB', 'PKCS')").cast("string"),
            )
        )

        pay_df = data_df.filter(f.col("insurance_company_name") == "")
        if not pay_df.isEmpty():
            exit_with_error(log, "insurance company id must be a not-null value.")
        pat_df = data_df.filter(f.col("member_id") == "")
        if not pat_df.isEmpty():
            exit_with_error(log, "member id must be not-null values.")
        # pro_df = data_df.filter(f.col("attending_npi") == "")
        # if not pro_df.isEmpty():
        #     exit_with_error(log, "attending npi must be not-null values.")
        prov_df = data_df.filter(f.col("facility_name") == "")
        if not prov_df.isEmpty():
            exit_with_error(log, "facility name must be not-null values.")

        # applying required transformation
        data_df = (
            data_df.withColumn("encounter_internal_id", f.expr("uuid()"))
            .withColumn("member_gender", transform_gender(f.col("member_gender")))
            .withColumn("member_dob", transform_date(f.col("member_dob")))
            .withColumn("admission_date", transform_date_time(f.col("admission_date")))
            .withColumn("discharge_date", transform_date_time(f.col("discharge_date")))
            .withColumn("claim_billed_date", transform_date_time(f.col("claim_billed_date")))
            .withColumn("claim_start_date", transform_date_time(f.col("claim_start_date")))
            .withColumn("claim_end_date", transform_date_time(f.col("claim_end_date")))
            .withColumn("claim_paid_date", transform_date_time(f.col("claim_paid_date")))
            .withColumn("principal_icd_proc_date", transform_date_time(f.col("principal_icd_proc_date")))
            .withColumn("icd_proc_code_1_date", transform_date_time(f.col("icd_proc_code_1_date")))
            .withColumn("icd_proc_code_2_date", transform_date_time(f.col("icd_proc_code_2_date")))
            .withColumn("icd_proc_code_3_date", transform_date_time(f.col("icd_proc_code_3_date")))
            .withColumn("icd_proc_code_4_date", transform_date_time(f.col("icd_proc_code_4_date")))
            .withColumn("icd_proc_code_5_date", transform_date_time(f.col("icd_proc_code_5_date")))
            .withColumn("icd_proc_code_6_date", transform_date_time(f.col("icd_proc_code_6_date")))
            .withColumn("icd_proc_code_7_date", transform_date_time(f.col("icd_proc_code_7_date")))
            .withColumn("icd_proc_code_8_date", transform_date_time(f.col("icd_proc_code_8_date")))
            .withColumn("icd_proc_code_9_date", transform_date_time(f.col("icd_proc_code_9_date")))
            .withColumn("icd_proc_code_10_date", transform_date_time(f.col("icd_proc_code_10_date")))
            .withColumn("icd_proc_code_11_date", transform_date_time(f.col("icd_proc_code_11_date")))
            .withColumn("icd_proc_code_12_date", transform_date_time(f.col("icd_proc_code_12_date")))
            .withColumn("svc_line_billed_date", transform_date_time(f.col("svc_line_billed_date")))
            .withColumn("svc_line_start_date", transform_date_time(f.col("svc_line_start_date")))
            .withColumn("svc_line_end_date", transform_date_time(f.col("svc_line_end_date")))
            .withColumn("svc_line_paid_date", transform_date_time(f.col("svc_line_paid_date")))
            .withColumn("dispense_date", transform_date_time(f.col("dispense_date")))
            .withColumn("claim_allowed_amount", transform_to_float(f.col("claim_allowed_amount")))
            .withColumn("claim_total_paid", transform_to_float(f.col("claim_total_paid")))
            .withColumn("claim_total_charges", transform_to_float(f.col("claim_total_charges")))
            .withColumn("claim_deduct_amount", transform_to_float(f.col("claim_deduct_amount")))
            .withColumn("claim_coinsurance_amount", transform_to_float(f.col("claim_coinsurance_amount")))
            .withColumn("claim_copay_amount", transform_to_float(f.col("claim_copay_amount")))
            .withColumn("svc_line_copay_amount", transform_to_float(f.col("svc_line_copay_amount")))
            .withColumn("svc_line_coinsurance_amount", transform_to_float(f.col("svc_line_coinsurance_amount")))
            .withColumn("svc_line_deduct_amount", transform_to_float(f.col("svc_line_deduct_amount")))
            .withColumn("svc_line_charges", transform_to_float(f.col("svc_line_charges")))
            .withColumn("svc_line_units", transform_to_float(f.col("svc_line_units")))
            .withColumn("service_paid_units", transform_to_float(f.col("service_paid_units")))
            .withColumn("svc_line_allowed_amount", transform_to_float(f.col("svc_line_allowed_amount")))
            .withColumn("file_tenant", f.lit(file_tenant))
            .withColumn("file_source", f.lit(file_source))
            .withColumn("resource_type", f.lit(resource_type))
            .withColumn("file_batch_id", f.lit(file_batch_id))
            .withColumn("src_file_name", f.lit(src_file_name))
            .withColumn("src_organization_id", f.lit(src_organization_id))
        )

        # consolidate diagnosis codes at line level and claim level
        line_diag_df = (
            data_df.withColumn(
                "diagnosis_code_list",
                f.array(
                    f.col("primary_diagnosis"),
                    f.col("admitting_diagnosis"),
                    f.col("diagnosis_code_1"),
                    f.col("diagnosis_code_2"),
                    f.col("diagnosis_code_3"),
                    f.col("diagnosis_code_4"),
                    f.col("diagnosis_code_5"),
                    f.col("diagnosis_code_6"),
                    f.col("diagnosis_code_7"),
                    f.col("diagnosis_code_8"),
                    f.col("diagnosis_code_9"),
                    f.col("diagnosis_code_10"),
                    f.col("diagnosis_code_11"),
                    f.col("diagnosis_code_12"),
                ),
            )
            .withColumn("diagnosis_code_list", f.array_distinct(f.expr("filter(diagnosis_code_list, x -> x != '')")))
            .withColumn("diagnosis_code", f.explode(f.col("diagnosis_code_list")))
            .select(["claim_id", "line_number", "diagnosis_code"])
        )
        claim_diag_df = (
            line_diag_df.groupBy("claim_id")
            .agg(f.collect_list("diagnosis_code").alias("diagnosis_code_list"))
            .withColumn("diagnosis_code", f.explode(f.array_distinct(f.col("diagnosis_code_list"))))
            .select(["claim_id", "diagnosis_code"])
        )
        diag_window_spec = w.partitionBy("claim_id").orderBy("claim_id")
        claim_diag_seq_df = claim_diag_df.withColumn("sequence", f.row_number().over(diag_window_spec))
        claim_diag_seq_df.persist()

        line_diag_seq_df = line_diag_df.join(
            claim_diag_seq_df,
            (line_diag_df.claim_id == claim_diag_seq_df.claim_id)
            & (line_diag_df.diagnosis_code == claim_diag_seq_df.diagnosis_code),
        ).select(line_diag_df["*"], claim_diag_seq_df["sequence"])
        line_diag_seq_df = (
            line_diag_seq_df.groupBy(["claim_id", "line_number"])
            .agg(f.collect_list("sequence").alias("diag_sequences"))
            .select(["claim_id", "line_number", "diag_sequences"])
        )

        # consolidate procedure codes at line level and claim level
        line_proc_df = (
            data_df.withColumn(
                "procedure_code_list",
                f.array(
                    f.col("principal_icd_proc_code"),
                    f.col("icd_proc_code_1"),
                    f.col("icd_proc_code_2"),
                    f.col("icd_proc_code_3"),
                    f.col("icd_proc_code_4"),
                    f.col("icd_proc_code_5"),
                    f.col("icd_proc_code_6"),
                    f.col("icd_proc_code_7"),
                    f.col("icd_proc_code_8"),
                    f.col("icd_proc_code_9"),
                    f.col("icd_proc_code_10"),
                    f.col("icd_proc_code_11"),
                    f.col("icd_proc_code_12"),
                ),
            )
            .withColumn("procedure_code_list", f.array_distinct(f.expr("filter(procedure_code_list, x -> x != '')")))
            .withColumn("procedure_code", f.explode(f.col("procedure_code_list")))
            .select(["claim_id", "line_number", "procedure_code"])
        )

        claim_proc_df = (
            line_proc_df.groupBy("claim_id")
            .agg(f.collect_list("procedure_code").alias("procedure_code_list"))
            .withColumn(
                "procedure_code", f.explode(f.array_distinct(f.expr("filter(procedure_code_list, x -> x != '')")))
            )
            .select(["claim_id", "procedure_code"])
        )
        proc_window_spec = w.partitionBy("claim_id").orderBy("claim_id")
        claim_proc_seq_df = claim_proc_df.withColumn("sequence", f.row_number().over(proc_window_spec))
        claim_proc_seq_df.persist()

        line_proc_seq_df = line_proc_df.join(
            claim_proc_seq_df,
            (line_proc_df.claim_id == claim_proc_seq_df.claim_id)
            & (line_proc_df.procedure_code == claim_proc_seq_df.procedure_code),
        ).select(line_proc_df["*"], claim_proc_seq_df["sequence"])
        line_proc_seq_df.persist()

        line_proc_seq_df = (
            line_proc_seq_df.groupBy(["claim_id", "line_number"])
            .agg(f.collect_list("sequence").alias("proc_sequences"))
            .select(["claim_id", "line_number", "proc_sequences"])
        )
        line_proc_seq_df.persist()

        # add diagmosis, procedure sequences into source dataframe
        line_data_df = data_df.join(
            line_diag_seq_df,
            (data_df.claim_id == line_diag_seq_df.claim_id) & (data_df.line_number == line_diag_seq_df.line_number),
            how="left",
        ).select(data_df["*"], line_diag_seq_df["diag_sequences"])
        line_data_df = line_data_df.join(
            line_proc_seq_df,
            (line_data_df.claim_id == line_proc_seq_df.claim_id)
            & (line_data_df.line_number == line_proc_seq_df.line_number),
            how="left",
        ).select(line_data_df["*"], line_proc_seq_df["proc_sequences"])
        line_data_df.persist()
        line_diag_seq_df.unpersist()
        line_proc_seq_df.unpersist()

        # aggregate service lines group by claim id
        group_by_cols = (
            "member_id",
            "member_mbi",
            "member_medicaid_id",
            "member_ssn",
            "member_mrn",
            "member_last_name",
            "member_first_name",
            "member_middle_initial",
            "member_dob",
            "member_gender",
            "member_relationship_code",
            "member_address_line_1",
            "member_address_line_2",
            "member_city",
            "member_state",
            "member_zip",
            "admission_date",
            "discharge_date",
            "admission_type",
            "admission_source",
            "discharge_status",
            "place_of_service",
            "facility_internal_id",
            "facility_name",
            "insurance_company_id",
            "insurance_company_name",
            "claim_id",
            "previous_claim_id",
            "claim_type",
            "claim_disposition",
            # "primary_payer_code",
            "claim_tob",
            "claim_billed_date",
            "claim_start_date",
            "claim_end_date",
            "ms_drg_code",
            "ap_drg_code",
            "apr_drg_code",
            "claim_total_charges",
            "claim_adjudication_status",
            "claim_total_paid",
            "claim_payment_indicator",
            "claim_paid_date",
            "claim_deduct_amount",
            "claim_copay_amount",
            "claim_coinsurance_amount",
            "claim_allowed_amount",
            # "claim_discount_amount",
            "claim_patient_paid_amount",
            # "claim_other_payer_paid",
        )
        claim_data_df = line_data_df.groupBy(*group_by_cols).agg(
            f.collect_list(
                f.struct(
                    f.col("line_number"),
                    f.col("svc_line_start_date"),
                    f.col("svc_line_end_date"),
                    f.col("revenue_code"),
                    f.col("cpt_code"),
                    f.col("cpt_code_description"),
                    f.col("hcpcs_code"),
                    f.col("hcpsc_code_description"),
                    f.col("svc_modifier_1"),
                    f.col("svc_modifier_2"),
                    f.col("svc_modifier_3"),
                    f.col("svc_modifier_4"),
                    f.col("svc_line_units"),
                    # f.col("type_of_service"),
                    f.col("svc_line_charges"),
                    f.col("svc_line_adjudication_status"),
                    f.col("svc_line_amount_paid"),
                    f.col("svc_line_paid_date"),
                    # f.col("line_payment_level"),
                    f.col("svc_line_deduct_amount"),
                    f.col("svc_line_copay_amount"),
                    f.col("svc_line_coinsurance_amount"),
                    f.col("svc_line_allowed_amount"),
                    # f.col("line_discount_amount"),
                    # f.col("line_patient_paid_amount"),
                    # f.col("line_other_payer_paid"),
                    # f.col("medicare_paid_amount"),
                    f.col("svc_line_units"),
                    f.concat_ws(",", f.col("diag_sequences")).alias("diagnosis_sequence"),
                    f.concat_ws(",", f.col("proc_sequences")).alias("procedure_sequence"),
                    f.col("encounter_internal_id"),
                )
            ).alias("service_lines")
        )

        claim_data_df = (
            claim_data_df.withColumn("patient_internal_id", f.expr("uuid()"))
            .withColumn(
                "prov_org_internal_id",
                f.when((f.col("facility_name") != ""), f.expr("uuid()")).otherwise(f.lit("")),
            )
            .withColumn("coverage_internal_id", f.expr("uuid()"))
            .withColumn(
                "insurer_internal_id",
                f.when(f.col("insurance_company_name") != "", f.expr("uuid()")).otherwise(f.lit("")),
            )
            .withColumn("claim_internal_id", f.expr("uuid()"))
            .withColumn("claim_response_internal_id", f.expr("uuid()"))
            .withColumn("bundle_id", f.expr("uuid()"))
        )

        # prepare claim diagnosis dataframe
        claim_diag_interm_df = claim_diag_seq_df.join(
            claim_data_df, claim_diag_seq_df.claim_id == claim_data_df.claim_id
        ).select(
            claim_diag_seq_df["*"],
            claim_data_df["patient_internal_id"],
        )

        claim_diag_final_df = (
            claim_diag_interm_df.withColumn("condition_id", f.expr("uuid()"))
            .withColumn(
                "diagnosis_struct",
                f.struct(
                    f.col("sequence"),
                    f.col("condition_id"),
                    f.col("diagnosis_code").alias("code"),
                    f.col("patient_internal_id"),
                ),
            )
            .select(["claim_id", "diagnosis_struct"])
        )
        claim_diag_final_df = claim_diag_final_df.groupBy("claim_id").agg(
            f.collect_list("diagnosis_struct").alias("diagnosis_structs")
        )

        # prepare claim procedure dataframe
        claim_proc_interm_df = claim_proc_seq_df.join(
            claim_data_df, claim_proc_seq_df.claim_id == claim_data_df.claim_id
        ).select(
            claim_proc_seq_df["*"],
            claim_data_df["patient_internal_id"],
        )

        claim_proc_final_df = (
            claim_proc_interm_df.withColumn("procedure_id", f.expr("uuid()"))
            .withColumn(
                "procedure_struct",
                f.struct(
                    f.col("sequence"),
                    f.col("procedure_id"),
                    f.col("procedure_code").alias("code"),
                    f.col("patient_internal_id"),
                ),
            )
            .select(["claim_id", "procedure_struct"])
        )
        claim_proc_final_df = claim_proc_final_df.groupBy("claim_id").agg(
            f.collect_list("procedure_struct").alias("procedure_structs")
        )

        # prepare final claim dataframe
        claim_data_final_df = claim_data_df.join(
            claim_diag_final_df, claim_data_df.claim_id == claim_diag_final_df.claim_id, how="left"
        ).select(claim_data_df["*"], claim_diag_final_df["diagnosis_structs"])
        claim_data_final_df = claim_data_final_df.join(
            claim_proc_final_df, claim_data_final_df.claim_id == claim_proc_final_df.claim_id, how="left"
        ).select(claim_data_final_df["*"], claim_proc_final_df["procedure_structs"])

        # claim_data_final_df = data_df.join(claim_data_final_df, on="claim_id", how="left").fillna("")

        # processing row wise operation
        transformer = FHIRTransformer()
        data_rdd = claim_data_final_df.rdd.map(lambda row: render_resources(row, transformer))
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
