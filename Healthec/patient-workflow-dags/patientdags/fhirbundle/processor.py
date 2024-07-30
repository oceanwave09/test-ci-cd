import json
import logging

import ijson
import smart_open
from airflow.exceptions import AirflowFailException, AirflowSkipException
from benedict import benedict
from fhirclient.resources.allergyintolerance import (
    AllergyIntolerance as AllergyIntoleranceEntity,
)
from fhirclient.resources.base import Base as BaseEntity
from fhirclient.resources.claim import Claim as ClaimEntity
from fhirclient.resources.claimresponse import ClaimResponse as ClaimResponseEntity
from fhirclient.resources.condition import Condition as ConditionEntity
from fhirclient.resources.coverage import Coverage as CoverageEntity
from fhirclient.resources.encounter import Encounter as EncounterEntity
from fhirclient.resources.immunization import Immunization as ImmunizationEntity
from fhirclient.resources.insuranceplan import InsurancePlan as InsurancePlanEntity
from fhirclient.resources.location import Location as LocationEntity
from fhirclient.resources.medication import Medication as MedicationEntity
from fhirclient.resources.medicationrequest import (
    MedicationRequest as MedicationRequestEntity,
)
from fhirclient.resources.medicationstatement import (
    MedicationStatement as MedicationStatementEntity,
)
from fhirclient.resources.observation import Observation as ObservationEntity
from fhirclient.resources.organization import Organization as OrganizationEntity
from fhirclient.resources.patient import Patient as PatientEntity
from fhirclient.resources.practitioner import Practitioner as PractitionerEntity
from fhirclient.resources.practitionerrole import (
    PractitionerRole as PractitionerRoleEntity,
)
from fhirclient.resources.procedure import Procedure as ProcedureEntity
from fhirclient.resources.servicerequest import ServiceRequest as ServiceRequestEntity

from patientdags.fhirvalidator.allergy_intolerance import AllergyIntolerance
from patientdags.fhirvalidator.base import Base
from patientdags.fhirvalidator.claim import Claim
from patientdags.fhirvalidator.claim_response import ClaimResponse
from patientdags.fhirvalidator.condition import Condition
from patientdags.fhirvalidator.coverage import Coverage
from patientdags.fhirvalidator.encounter import Encounter
from patientdags.fhirvalidator.immunization import Immunization
from patientdags.fhirvalidator.insurance_plan import InsurancePlan
from patientdags.fhirvalidator.location import Location
from patientdags.fhirvalidator.medication import Medication
from patientdags.fhirvalidator.medication_request import MedicationRequest
from patientdags.fhirvalidator.medication_statement import MedicationStatement
from patientdags.fhirvalidator.observation import Observation
from patientdags.fhirvalidator.organization import Organization
from patientdags.fhirvalidator.patient import Patient
from patientdags.fhirvalidator.practitioner import Prcatitioner
from patientdags.fhirvalidator.practitioner_role import PractitionerRole
from patientdags.fhirvalidator.procedure import Procedure
from patientdags.fhirvalidator.service_request import ServiceRequest
from patientdags.utils.api_client import (  # get_resource_entity,; put_resource_entity,
    match_resource_entity,
    post_resource_entity,
    upsert_resource_entity,
)
from patientdags.utils.enum import MatchingResourceType, ResourceType, UpsertFileType
from patientdags.utils.error_codes import PatientDagsErrorCodes, publish_error_code
from patientdags.utils.utils import is_empty, remove_duplicates


def _has_resource(bundle_stats: str, resource_type: str):
    bundle_stats = json.loads(bundle_stats) if not is_empty(bundle_stats) else {}
    if not bundle_stats.get(resource_type):
        raise AirflowSkipException(f"No entries for {resource_type} resource, Skipping the task.")


def _merge_resource(resource: dict, new_resource: dict) -> dict:
    """
    Merges new, existing resources and remove duplicate enries from list fields.
    """
    new_resource_dict = benedict(new_resource)
    resource_dict = benedict(resource)
    resource_dict.merge(new_resource_dict)
    return remove_duplicates(resource_dict)


def _compare_resource(resource: dict, updated_resource: dict) -> bool:
    return json.dumps(resource) == json.dumps(updated_resource)


def _process_resource(
    file_tenant: str,
    resource_type: str,
    validator: Base,
    client_entity: BaseEntity,
    scope: str = None,
    scope_field: str = None,
    file_resource_type: str = None,
):
    # update resource identifiers, references and other fields
    if resource_type == ResourceType.Patient.value:
        validator.update_resource(file_resource_type=file_resource_type)
    else:
        validator.update_resource()
    # logging.debug(f"Updated {resource_type} resource: {validator.resource}")
    logging.info("Applied transformation and resolved references on resource")

    # get parent(scope) resource id if scope is provided using field name
    scope_id = None
    if scope and scope_field:
        reference_field = validator.resource.get(scope_field, {})
        reference = reference_field.get("reference", "")
        scope_id = reference.replace(f"{scope}/", "")
        if not scope_id:
            raise ValueError(f"Failed to parse {scope} reference id from {resource_type}")

    if file_resource_type in [entry.value for entry in UpsertFileType]:
        # validate resource fields
        validator.validate()
        return upsert_resource_entity(file_tenant, validator.resource, client_entity, scope, scope_id)
    else:
        resource_id = ""
        if resource_type in [entry.value for entry in MatchingResourceType]:
            # check if resource already exists using match endpoint
            resource_id = match_resource_entity(file_tenant, validator.resource, client_entity, scope, scope_id)

            if resource_id:
                # # update not allowed on Organization resource
                # if resource_type in [ResourceType.Organization.value]:
                #     return resource_id

                # # get resource by match resource id and update with new resource information
                # resource = get_resource_entity(file_tenant, resource_id, client_entity, scope, scope_id)
                # updated_resource = -_merge_resource(resource, validator.resource)

                # # update resource if there is difference in resource information
                # if _compare_resource(resource, updated_resource):
                #     return resource_id
                # validator.resource = updated_resource

                # # validate updated resource fields
                # validator.validate()

                # put_resource_entity(file_tenant, resource_id, validator.resource, client_entity, scope, scope_id)
                return resource_id

        # validate resource fields
        validator.validate()

        resource_id = post_resource_entity(file_tenant, validator.resource, client_entity, scope, scope_id)
        return resource_id


def process_organization(bundle_stats: str, file_tenant: str, file_path: str):
    resouce_type = ResourceType.Organization.value
    # Skips if no organization entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing organization resource with id {temp_resource_id}")
                    validator = Organization(resource)
                    resource_id = _process_resource(file_tenant, resouce_type, validator, OrganizationEntity)
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def process_location(
    bundle_stats: str, file_tenant: str, file_path: str, organization_ids: str, src_organization_id: str = None
):
    resouce_type = ResourceType.Location.value
    # Skips if no location entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing location resource with id {temp_resource_id}")
                    validator = Location(resource, organization_ids, src_organization_id)
                    resource_id = _process_resource(
                        file_tenant, resouce_type, validator, LocationEntity, "Organization", "managingOrganization"
                    )
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def process_practitioner(bundle_stats: str, file_tenant: str, file_path: str):
    resouce_type = ResourceType.Practitioner.value
    # Skips if no practitioner entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing practitioner resource with id {temp_resource_id}")
                    validator = Prcatitioner(resource)
                    resource_id = _process_resource(file_tenant, resouce_type, validator, PractitionerEntity)
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def process_insurance_plan(bundle_stats: str, file_tenant: str, file_path: str, organization_ids: str):
    resouce_type = ResourceType.InsurancePlan.value
    # Skips if no insurance plan entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing insurance plan resource with id {temp_resource_id}")
                    validator = InsurancePlan(resource, organization_ids)
                    resource_id = _process_resource(file_tenant, resouce_type, validator, InsurancePlanEntity)
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def process_medication(bundle_stats: str, file_tenant: str, file_path: str, organization_ids: str):
    resouce_type = ResourceType.Medication.value
    # Skips if no medication entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing medication resource with id {temp_resource_id}")
                    validator = Medication(resource, organization_ids)
                    resource_id = _process_resource(file_tenant, resouce_type, validator, MedicationEntity)
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def process_patient(
    bundle_stats: str,
    file_tenant: str,
    file_path: str,
    organization_ids: str,
    practitioner_ids: str,
    src_organization_id: str = None,
    file_resource_type: str = None,
):
    resouce_type = ResourceType.Patient.value
    # Skips if no patient entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing patient resource with id {temp_resource_id}")
                    validator = Patient(resource, organization_ids, practitioner_ids, src_organization_id)
                    resource_id = _process_resource(
                        file_tenant=file_tenant,
                        resource_type=resouce_type,
                        validator=validator,
                        client_entity=PatientEntity,
                        file_resource_type=file_resource_type,
                    )
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def process_practitioner_role(
    bundle_stats: str,
    file_tenant: str,
    file_path: str,
    organization_ids: str,
    practitioner_ids: str,
    location_ids: str,
    src_organization_id: str = None,
):
    resouce_type = ResourceType.PractitionerRole.value
    # Skips if no practitioner role entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing practitioner role resource with id {temp_resource_id}")
                    validator = PractitionerRole(
                        resource, organization_ids, practitioner_ids, location_ids, src_organization_id
                    )
                    resource_id = _process_resource(
                        file_tenant, resouce_type, validator, PractitionerRoleEntity, "Practitioner", "practitioner"
                    )
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def process_coverage(
    bundle_stats: str,
    file_tenant: str,
    file_path: str,
    organization_ids: str,
    patient_ids: str,
    insurance_plan_ids: str,
):
    resouce_type = ResourceType.Coverage.value
    # Skips if no coverage entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing coverage resource with id {temp_resource_id}")
                    validator = Coverage(resource, organization_ids, patient_ids, insurance_plan_ids)
                    resource_id = _process_resource(
                        file_tenant, resouce_type, validator, CoverageEntity, "Patient", "beneficiary"
                    )
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def process_encounter(
    bundle_stats: str,
    file_tenant: str,
    file_path: str,
    patient_ids: str,
    practitioner_ids: str,
    organization_ids: str,
    location_ids: str,
    src_organization_id: str = None,
):
    resouce_type = ResourceType.Encounter.value
    # Skips if no encounter entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing encounter resource with id {temp_resource_id}")
                    validator = Encounter(
                        resource, patient_ids, practitioner_ids, organization_ids, location_ids, src_organization_id
                    )
                    resource_id = _process_resource(
                        file_tenant, resouce_type, validator, EncounterEntity, "Patient", "subject"
                    )
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def process_condition(
    bundle_stats: str, file_tenant: str, file_path: str, patient_ids: str, encounter_ids: str, practitioner_ids: str
):
    resouce_type = ResourceType.Condition.value
    # Skips if no condition entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing condition resource with id {temp_resource_id}")
                    validator = Condition(resource, patient_ids, encounter_ids, practitioner_ids)
                    resource_id = _process_resource(
                        file_tenant, resouce_type, validator, ConditionEntity, "Patient", "subject"
                    )
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def process_procedure(
    bundle_stats: str,
    file_tenant: str,
    file_path: str,
    patient_ids: str,
    encounter_ids: str,
    practitioner_ids: str,
    location_ids: str,
    condition_ids: str,
):
    resouce_type = ResourceType.Procedure.value
    # Skips if no procedure entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing procedure resource with id {temp_resource_id}")
                    validator = Procedure(
                        resource, patient_ids, encounter_ids, practitioner_ids, location_ids, condition_ids
                    )
                    resource_id = _process_resource(
                        file_tenant, resouce_type, validator, ProcedureEntity, "Patient", "subject"
                    )
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def process_service_request(
    bundle_stats: str, file_tenant: str, file_path: str, patient_ids: str, encounter_ids: str, practitioner_ids: str
):
    resouce_type = ResourceType.ServiceRequest.value
    # Skips if no condition entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing ServiceRequest resource with id {temp_resource_id}")
                    validator = ServiceRequest(resource, patient_ids, encounter_ids, practitioner_ids)
                    resource_id = _process_resource(
                        file_tenant, resouce_type, validator, ServiceRequestEntity, "Patient", "subject"
                    )
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def process_observation(
    bundle_stats: str, file_tenant: str, file_path: str, patient_ids: str, encounter_ids: str, practitioner_ids: str
):
    resouce_type = ResourceType.Observation.value
    # Skips if no observation entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing observation resource with id {temp_resource_id}")
                    validator = Observation(resource, patient_ids, encounter_ids, practitioner_ids)
                    resource_id = _process_resource(
                        file_tenant, resouce_type, validator, ObservationEntity, "Patient", "subject"
                    )
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def process_allergy_intolerance(
    bundle_stats: str, file_tenant: str, file_path: str, patient_ids: str, encounter_ids: str, practitioner_ids: str
):
    resouce_type = ResourceType.AllergyIntolerance.value
    # Skips if no allergy intolerance entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing allergy intolerance resource with id {temp_resource_id}")
                    validator = AllergyIntolerance(resource, patient_ids, encounter_ids, practitioner_ids)
                    resource_id = _process_resource(
                        file_tenant, resouce_type, validator, AllergyIntoleranceEntity, "Patient", "patient"
                    )
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def process_medication_request(
    bundle_stats: str,
    file_tenant: str,
    file_path: str,
    patient_ids: str,
    encounter_ids: str,
    practitioner_ids: str,
    medication_ids: str,
):
    resouce_type = ResourceType.MedicationRequest.value
    # Skips if no medication request entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing medication request resource with id {temp_resource_id}")
                    validator = MedicationRequest(resource, patient_ids, encounter_ids, practitioner_ids, medication_ids)
                    resource_id = _process_resource(
                        file_tenant, resouce_type, validator, MedicationRequestEntity, "Patient", "subject"
                    )
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def process_medication_statement(
    bundle_stats: str,
    file_tenant: str,
    file_path: str,
    patient_ids: str,
    encounter_ids: str,
    practitioner_ids: str,
    medication_ids: str,
    condition_ids: str,
):
    resouce_type = ResourceType.MedicationStatement.value
    # Skips if no medication statement entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing medication statement resource with id {temp_resource_id}")
                    validator = MedicationStatement(
                        resource, patient_ids, encounter_ids, practitioner_ids, medication_ids, condition_ids
                    )
                    resource_id = _process_resource(
                        file_tenant, resouce_type, validator, MedicationStatementEntity, "Patient", "subject"
                    )
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def process_immunization(
    bundle_stats: str,
    file_tenant: str,
    file_path: str,
    organization_ids: str,
    patient_ids: str,
    encounter_ids: str,
    practitioner_ids: str,
    location_ids: str,
    condition_ids: str,
):
    resouce_type = ResourceType.Immunization.value
    # Skips if no immunization entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing immunization resource with id {temp_resource_id}")
                    validator = Immunization(
                        resource,
                        organization_ids,
                        patient_ids,
                        encounter_ids,
                        practitioner_ids,
                        location_ids,
                        condition_ids,
                    )
                    resource_id = _process_resource(
                        file_tenant, resouce_type, validator, ImmunizationEntity, "Patient", "patient"
                    )
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def process_claim(
    bundle_stats: str,
    file_tenant: str,
    file_path: str,
    organization_ids: str,
    patient_ids: str,
    encounter_ids: str,
    practitioner_ids: str,
    location_ids: str,
    condition_ids: str,
    procedure_ids: str,
    medication_request_ids: str,
    coverage_ids: str,
):
    resouce_type = ResourceType.Claim.value
    # Skips if no claim entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing claim resource with id {temp_resource_id}")
                    validator = Claim(
                        resource,
                        organization_ids,
                        patient_ids,
                        encounter_ids,
                        practitioner_ids,
                        location_ids,
                        condition_ids,
                        procedure_ids,
                        medication_request_ids,
                        coverage_ids,
                    )
                    resource_id = _process_resource(
                        file_tenant, resouce_type, validator, ClaimEntity, "Patient", "patient"
                    )
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def process_claim_response(
    bundle_stats: str,
    file_tenant: str,
    file_path: str,
    organization_ids: str,
    patient_ids: str,
    practitioner_ids: str,
    claim_ids: str,
):
    resouce_type = ResourceType.ClaimResponse.value
    # Skips if no claim response entries
    _has_resource(bundle_stats, resouce_type)
    try:
        entity_ids = {}
        with smart_open.open(file_path, "rb") as f:
            for entry in ijson.items(f, "entry.item"):
                resource = entry.get("resource")
                if resource and resource.get("resourceType") == resouce_type:
                    temp_resource_id = resource.pop("id")
                    logging.info(f"Processing claim response resource with id {temp_resource_id}")
                    validator = ClaimResponse(resource, organization_ids, patient_ids, practitioner_ids, claim_ids)
                    resource_id = _process_resource(
                        file_tenant, resouce_type, validator, ClaimResponseEntity, "Patient", "patient"
                    )
                    entity_ids.update({temp_resource_id: resource_id})
        return json.dumps(entity_ids)
    except ValueError as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)
