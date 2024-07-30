import json
import logging
from collections import OrderedDict

from airflow.exceptions import AirflowFailException
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
from patientdags.utils.api_client import (
    match_resource_entity,
    post_resource_entity,
    upsert_resource_entity,
)
from patientdags.utils.enum import MatchingResourceType, ResourceType, UpsertFileType
from patientdags.utils.error_codes import PatientDagsErrorCodes, publish_error_code
from patientdags.utils.utils import remove_duplicates


class BundleProcessor:
    def __init__(
        self,
        data: str,
        file_tenant: str,
        file_source: str = None,
        file_resource_type: str = None,
        file_batch_id: str = None,
        src_file_name: str = None,
        src_organization_id: dict = None,
    ) -> None:
        """
        @param data : bundle data
        @param file_tenant : tenant name
        @param file_source : practice or source name
        @param file_resource_type : file resource type
        @param file_batch_id : unique id for each file processing
        @param src_file_name : source file name
        @param src_organization_id : practice or source organization id
        """
        self._data = data
        self._file_tenant = file_tenant
        self._file_source = file_source
        self._file_resource_type = file_resource_type
        self._file_batch_id = file_batch_id
        self._src_file_name = src_file_name
        self._src_organization_id = src_organization_id
        self._resources = {}
        self._ref_id_by_type = {}
        self._resource_ids = {}
        self._resource_type = ""
        self._resource_process_list = OrderedDict()
        self._construct_methods_dict()

    def _construct_methods_dict(self):
        """
        Create Ordered dict to store the resource type as key
        and value as corresponding function.
        """
        self._resource_process_list = {
            ResourceType.Organization.value: self._process_organization,
            ResourceType.Location.value: self._process_location,
            ResourceType.Practitioner.value: self._process_practitioner,
            ResourceType.InsurancePlan.value: self._process_insurance_plan,
            ResourceType.Medication.value: self._process_medication,
            ResourceType.Patient.value: self._process_patient,
            ResourceType.PractitionerRole.value: self._process_practitioner_role,
            ResourceType.Coverage.value: self._process_coverage,
            ResourceType.Encounter.value: self._process_encounter,
            ResourceType.Condition.value: self._process_condition,
            ResourceType.Procedure.value: self._process_procedure,
            ResourceType.Observation.value: self._process_observation,
            ResourceType.AllergyIntolerance.value: self._process_allergy_intolerance,
            ResourceType.MedicationRequest.value: self._process_medication_request,
            ResourceType.MedicationStatement.value: self._process_medication_statement,
            ResourceType.Immunization.value: self._process_immunization,
            ResourceType.Claim.value: self._process_claim,
            ResourceType.ClaimResponse.value: self._process_claim_response,
        }

    def _group_by_resource_type(self) -> dict:
        """
        Group fhir_resource by resource_type
        """
        bundle = json.loads(self._data)
        for entry in bundle.get("entry", []):
            resource_dict = entry.get("resource") if entry.get("resource") else entry
            if resource_dict.get("resourceType") not in self._resources:
                self._resources[resource_dict.get("resourceType")] = []
            self._resources[resource_dict.get("resourceType")].append(resource_dict)
        return bundle.get("id")

    def _merge_resource(self, resource: dict, new_resource: dict) -> dict:
        """
        Merges new, existing resources and remove duplicate enries from list fields.
        """
        new_resource_dict = benedict(new_resource)
        resource_dict = benedict(resource)
        resource_dict.merge(new_resource_dict)
        return remove_duplicates(resource_dict)

    def _compare_resource(self, resource: dict, updated_resource: dict) -> bool:
        return json.dumps(resource) == json.dumps(updated_resource)

    def _process_resource(self, validator: Base, client_entity: BaseEntity, scope: str = None, scope_field: str = None):
        """
        Insert/Update the fhir resource to DB using API service.
        @param validator: fhir resource
        @param client_entity: fhir resource type
        @param scope: path/query parameter(main resource) type for sub resources
        @param scope_field: key name to get path/query parameter(main resource) id
        """
        # update resource identifiers, references and other fields
        if self._resource_type == ResourceType.Patient.value:
            validator.update_resource(file_resource_type=self._file_resource_type)
        else:
            validator.update_resource()
        # logging.debug(f"Updated {self._resource_type} resource: {validator.resource}")
        logging.info("Applied transformation and resolved references on resource")

        # get parent(scope) resource id if scope is provided using field name
        scope_id = None
        if scope and scope_field:
            reference_field = validator.resource.get(scope_field, {})
            reference = reference_field.get("reference", "")
            scope_id = reference.replace(f"{scope}/", "")
            if not scope_id:
                raise ValueError(f"Failed to parse {scope} reference id from {self._resource_type}")

        if self._file_resource_type in [entry.value for entry in UpsertFileType]:
            # validate resource fields
            validator.validate()
            return upsert_resource_entity(self._file_tenant, validator.resource, client_entity, scope, scope_id)
        else:
            resource_id = ""
            if self._resource_type in [entry.value for entry in MatchingResourceType]:
                # check if resource already exists using match endpoint
                resource_id = match_resource_entity(
                    self._file_tenant, validator.resource, client_entity, scope, scope_id
                )

                if resource_id:
                    # # update not allowed on Organization resource
                    # if self._resource_type in [ResourceType.Organization.value]:
                    #     return resource_id

                    # # get resource by match resource id and update with new resource information
                    # resource = get_resource_entity(
                    # self._file_tenant, resource_id, client_entity, scope, scope_id
                    # )
                    # updated_resource = self._merge_resource(resource, validator.resource)

                    # # update resource if there is difference in resource information
                    # if self._compare_resource(resource, updated_resource):
                    #     return resource_id
                    # validator.resource = updated_resource

                    # # validate updated resource fields
                    # validator.validate()

                    # put_resource_entity(
                    # self._file_tenant, resource_id, validator.resource, client_entity, scope, scope_id
                    # )
                    return resource_id

            # validate resource fields
            validator.validate()
            resource_id = post_resource_entity(self._file_tenant, validator.resource, client_entity, scope, scope_id)
            return resource_id

    def _process_organization(self):
        """
        Iterate the organization resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.Organization.value
        try:
            entity_ids = {}
            ids = set()
            for org_data in self._resources.get(self._resource_type, []):
                temp_resource_id = org_data.pop("id")
                logging.info(f"Processing organization resource with temp id {temp_resource_id}")
                validator = Organization(org_data)
                resource_id = self._process_resource(validator, OrganizationEntity)
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def _process_location(self):
        """
        Iterate the location resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.Location.value
        try:
            entity_ids = {}
            ids = set()
            org_ref_ids = self._ref_id_by_type[ResourceType.Organization.value]
            for loc_data in self._resources.get(self._resource_type, []):
                temp_resource_id = loc_data.pop("id")
                logging.info(f"Processing location resource with temp id {temp_resource_id}")
                validator = Location(loc_data, org_ref_ids, self._src_organization_id)
                resource_id = self._process_resource(validator, LocationEntity, "Organization", "managingOrganization")
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def _process_practitioner(self):
        """
        Iterate the practitioner resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.Practitioner.value
        try:
            entity_ids = {}
            ids = set()
            for prac_data in self._resources.get(self._resource_type, []):
                temp_resource_id = prac_data.pop("id")
                logging.info(f"Processing practitioner resource with temp id {temp_resource_id}")
                validator = Prcatitioner(prac_data)
                resource_id = self._process_resource(validator, PractitionerEntity)
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def _process_insurance_plan(self):
        """
        Iterate the insurance plan resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.InsurancePlan.value
        try:
            entity_ids = {}
            ids = set()
            org_ref_ids = self._ref_id_by_type[ResourceType.Organization.value]
            for ins_data in self._resources.get(self._resource_type, []):
                temp_resource_id = ins_data.pop("id")
                logging.info(f"Processing insurance plan resource with temp id {temp_resource_id}")
                validator = InsurancePlan(ins_data, org_ref_ids)
                resource_id = self._process_resource(validator, InsurancePlanEntity)
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def _process_medication(self):
        """
        Iterate the medication resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.Medication.value
        try:
            entity_ids = {}
            ids = set()
            org_ref_ids = self._ref_id_by_type[ResourceType.Organization.value]
            for med_data in self._resources.get(self._resource_type, []):
                temp_resource_id = med_data.pop("id")
                logging.info(f"Processing medication resource with temp id {temp_resource_id}")
                validator = Medication(med_data, org_ref_ids)
                resource_id = self._process_resource(validator, MedicationEntity)
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def _process_patient(self):
        """
        Iterate the patient resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.Patient.value
        try:
            entity_ids = {}
            ids = set()
            org_ref_ids = self._ref_id_by_type[ResourceType.Organization.value]
            prac_ref_ids = self._ref_id_by_type[ResourceType.Practitioner.value]
            for pat_data in self._resources.get(self._resource_type, []):
                temp_resource_id = pat_data.pop("id")
                logging.info(f"Processing patient resource with temp id {temp_resource_id}")
                validator = Patient(pat_data, org_ref_ids, prac_ref_ids, self._src_organization_id)
                resource_id = self._process_resource(validator, PatientEntity)
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def _process_practitioner_role(self):
        """
        Iterate the practitioner_role resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.PractitionerRole.value
        try:
            entity_ids = {}
            ids = set()
            org_ref_ids = self._ref_id_by_type[ResourceType.Organization.value]
            prac_ref_ids = self._ref_id_by_type[ResourceType.Practitioner.value]
            loc_ref_ids = self._ref_id_by_type[ResourceType.Location.value]
            for prc_rol_data in self._resources.get(self._resource_type, []):
                temp_resource_id = prc_rol_data.pop("id")
                logging.info(f"Processing practitioner role resource with temp id {temp_resource_id}")
                validator = PractitionerRole(
                    prc_rol_data, org_ref_ids, prac_ref_ids, loc_ref_ids, self._src_organization_id
                )
                resource_id = self._process_resource(validator, PractitionerRoleEntity, "Practitioner", "practitioner")
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def _process_coverage(self):
        """
        Iterate the coverage resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.Coverage.value
        try:
            entity_ids = {}
            ids = set()
            org_ref_ids = self._ref_id_by_type[ResourceType.Organization.value]
            pat_ref_ids = self._ref_id_by_type[ResourceType.Patient.value]
            ins_ref_ids = self._ref_id_by_type[ResourceType.InsurancePlan.value]
            for cov_data in self._resources.get(self._resource_type, []):
                temp_resource_id = cov_data.pop("id")
                logging.info(f"Processing coverage resource with temp id {temp_resource_id}")
                validator = Coverage(cov_data, org_ref_ids, pat_ref_ids, ins_ref_ids)
                resource_id = self._process_resource(validator, CoverageEntity, "Patient", "beneficiary")
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def _process_encounter(self):
        """
        Iterate the encounter resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.Encounter.value
        try:
            entity_ids = {}
            ids = set()
            org_ref_ids = self._ref_id_by_type[ResourceType.Organization.value]
            pat_ref_ids = self._ref_id_by_type[ResourceType.Patient.value]
            prac_ref_ids = self._ref_id_by_type[ResourceType.Practitioner.value]
            loc_ref_ids = self._ref_id_by_type[ResourceType.Location.value]
            for enc_data in self._resources.get(self._resource_type, []):
                temp_resource_id = enc_data.pop("id")
                logging.info(f"Processing encounter resource with temp id {temp_resource_id}")
                validator = Encounter(
                    enc_data, pat_ref_ids, prac_ref_ids, org_ref_ids, loc_ref_ids, self._src_organization_id
                )
                resource_id = self._process_resource(validator, EncounterEntity, "Patient", "subject")
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def _process_condition(self):
        """
        Iterate the condition resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.Condition.value
        try:
            entity_ids = {}
            ids = set()
            enc_ref_ids = self._ref_id_by_type[ResourceType.Encounter.value]
            pat_ref_ids = self._ref_id_by_type[ResourceType.Patient.value]
            prac_ref_ids = self._ref_id_by_type[ResourceType.Practitioner.value]
            for con_data in self._resources.get(self._resource_type, []):
                temp_resource_id = con_data.pop("id")
                logging.info(f"Processing condition resource with temp id {temp_resource_id}")
                validator = Condition(con_data, pat_ref_ids, enc_ref_ids, prac_ref_ids)
                resource_id = self._process_resource(validator, ConditionEntity, "Patient", "subject")
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def _process_service_request(self):
        """
        Iterate the Service Request resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.ServiceRequest.value
        try:
            entity_ids = {}
            ids = set()
            enc_ref_ids = self._ref_id_by_type[ResourceType.Encounter.value]
            pat_ref_ids = self._ref_id_by_type[ResourceType.Patient.value]
            prac_ref_ids = self._ref_id_by_type[ResourceType.Practitioner.value]
            for serv_req in self._resources.get(self._resource_type, []):
                temp_resource_id = serv_req.pop("id")
                logging.info(f"Processing ServiceRequest resource with temp id {temp_resource_id}")
                validator = ServiceRequest(serv_req, pat_ref_ids, enc_ref_ids, prac_ref_ids)
                resource_id = self._process_resource(validator, ServiceRequestEntity, "Patient", "subject")
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def _process_procedure(self):
        """
        Iterate the procedure resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.Procedure.value
        try:
            entity_ids = {}
            ids = set()
            con_ref_ids = self._ref_id_by_type[ResourceType.Condition.value]
            enc_ref_ids = self._ref_id_by_type[ResourceType.Encounter.value]
            pat_ref_ids = self._ref_id_by_type[ResourceType.Patient.value]
            prac_ref_ids = self._ref_id_by_type[ResourceType.Practitioner.value]
            loc_ref_ids = self._ref_id_by_type[ResourceType.Location.value]
            for pro_data in self._resources.get(self._resource_type, []):
                temp_resource_id = pro_data.pop("id")
                logging.info(f"Processing procedure resource with temp id {temp_resource_id}")
                validator = Procedure(pro_data, pat_ref_ids, enc_ref_ids, prac_ref_ids, loc_ref_ids, con_ref_ids)
                resource_id = self._process_resource(validator, ProcedureEntity, "Patient", "subject")
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def _process_observation(self):
        """
        Iterate the observation resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.Observation.value
        try:
            entity_ids = {}
            ids = set()
            enc_ref_ids = self._ref_id_by_type[ResourceType.Encounter.value]
            pat_ref_ids = self._ref_id_by_type[ResourceType.Patient.value]
            prac_ref_ids = self._ref_id_by_type[ResourceType.Practitioner.value]
            for obs_data in self._resources.get(self._resource_type, []):
                temp_resource_id = obs_data.pop("id")
                logging.info(f"Processing observation resource with temp id {temp_resource_id}")
                validator = Observation(obs_data, pat_ref_ids, enc_ref_ids, prac_ref_ids)
                resource_id = self._process_resource(validator, ObservationEntity, "Patient", "subject")
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def _process_allergy_intolerance(self):
        """
        Iterate the allergy intolerance resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.AllergyIntolerance.value
        try:
            entity_ids = {}
            ids = set()
            enc_ref_ids = self._ref_id_by_type[ResourceType.Encounter.value]
            pat_ref_ids = self._ref_id_by_type[ResourceType.Patient.value]
            prac_ref_ids = self._ref_id_by_type[ResourceType.Practitioner.value]
            for algy_data in self._resources.get(self._resource_type, []):
                temp_resource_id = algy_data.pop("id")
                logging.info(f"Processing allergy intolerance resource with temp id {temp_resource_id}")
                validator = AllergyIntolerance(algy_data, pat_ref_ids, enc_ref_ids, prac_ref_ids)
                resource_id = self._process_resource(validator, AllergyIntoleranceEntity, "Patient", "patient")
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def _process_medication_request(self):
        """
        Iterate the medication_request resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.MedicationRequest.value
        try:
            entity_ids = {}
            ids = set()
            enc_ref_ids = self._ref_id_by_type[ResourceType.Encounter.value]
            pat_ref_ids = self._ref_id_by_type[ResourceType.Patient.value]
            prac_ref_ids = self._ref_id_by_type[ResourceType.Practitioner.value]
            med_ref_ids = self._ref_id_by_type[ResourceType.Medication.value]
            for med_req_data in self._resources.get(self._resource_type, []):
                temp_resource_id = med_req_data.pop("id")
                logging.info(f"Processing medication request resource with temp id {temp_resource_id}")
                validator = MedicationRequest(med_req_data, pat_ref_ids, enc_ref_ids, prac_ref_ids, med_ref_ids)
                resource_id = self._process_resource(validator, MedicationRequestEntity, "Patient", "subject")
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def _process_medication_statement(self):
        """
        Iterate the medication_statement resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.MedicationStatement.value
        try:
            entity_ids = {}
            ids = set()
            enc_ref_ids = self._ref_id_by_type[ResourceType.Encounter.value]
            pat_ref_ids = self._ref_id_by_type[ResourceType.Patient.value]
            prac_ref_ids = self._ref_id_by_type[ResourceType.Practitioner.value]
            med_ref_ids = self._ref_id_by_type[ResourceType.Medication.value]
            con_ref_ids = self._ref_id_by_type[ResourceType.Condition.value]
            for med_stm_data in self._resources.get(self._resource_type, []):
                temp_resource_id = med_stm_data.pop("id")
                logging.info(f"Processing medication statement resource with temp id {temp_resource_id}")
                validator = MedicationStatement(
                    med_stm_data, pat_ref_ids, enc_ref_ids, prac_ref_ids, med_ref_ids, con_ref_ids
                )
                resource_id = self._process_resource(validator, MedicationStatementEntity, "Patient", "subject")
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def _process_immunization(self):
        """
        Iterate the immunization resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.Immunization.value
        try:
            entity_ids = {}
            ids = set()
            org_ref_ids = self._ref_id_by_type[ResourceType.Organization.value]
            enc_ref_ids = self._ref_id_by_type[ResourceType.Encounter.value]
            pat_ref_ids = self._ref_id_by_type[ResourceType.Patient.value]
            prac_ref_ids = self._ref_id_by_type[ResourceType.Practitioner.value]
            loc_ref_ids = self._ref_id_by_type[ResourceType.Location.value]
            con_ref_ids = self._ref_id_by_type[ResourceType.Condition.value]
            for immu_data in self._resources.get(self._resource_type, []):
                temp_resource_id = immu_data.pop("id")
                logging.info(f"Processing immunization resource with temp id {temp_resource_id}")
                validator = Immunization(
                    immu_data,
                    org_ref_ids,
                    pat_ref_ids,
                    enc_ref_ids,
                    prac_ref_ids,
                    loc_ref_ids,
                    con_ref_ids,
                )
                resource_id = self._process_resource(validator, ImmunizationEntity, "Patient", "patient")
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def _process_claim(self):
        """
        Iterate the claim resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.Claim.value
        try:
            entity_ids = {}
            ids = set()
            org_ref_ids = self._ref_id_by_type[ResourceType.Organization.value]
            enc_ref_ids = self._ref_id_by_type[ResourceType.Encounter.value]
            pat_ref_ids = self._ref_id_by_type[ResourceType.Patient.value]
            prac_ref_ids = self._ref_id_by_type[ResourceType.Practitioner.value]
            loc_ref_ids = self._ref_id_by_type[ResourceType.Location.value]
            con_ref_ids = self._ref_id_by_type[ResourceType.Condition.value]
            pro_ref_ids = self._ref_id_by_type[ResourceType.Procedure.value]
            med_req_ref_ids = self._ref_id_by_type[ResourceType.MedicationRequest.value]
            cov_ref_ids = self._ref_id_by_type[ResourceType.Coverage.value]
            for clm_data in self._resources.get(self._resource_type, []):
                temp_resource_id = clm_data.pop("id")
                logging.info(f"Processing claim resource with temp id {temp_resource_id}")
                validator = Claim(
                    clm_data,
                    org_ref_ids,
                    pat_ref_ids,
                    enc_ref_ids,
                    prac_ref_ids,
                    loc_ref_ids,
                    con_ref_ids,
                    pro_ref_ids,
                    med_req_ref_ids,
                    cov_ref_ids,
                )
                resource_id = self._process_resource(validator, ClaimEntity, "Patient", "patient")
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def _process_claim_response(self):
        """
        Iterate the claim_response resources and perform insert/update resource through service.
        """
        self._resource_type = ResourceType.ClaimResponse.value
        try:
            entity_ids = {}
            ids = set()
            org_ref_ids = self._ref_id_by_type[ResourceType.Organization.value]
            pat_ref_ids = self._ref_id_by_type[ResourceType.Patient.value]
            prac_ref_ids = self._ref_id_by_type[ResourceType.Practitioner.value]
            clm_ref_ids = self._ref_id_by_type[ResourceType.Claim.value]
            for clm_res_data in self._resources.get(self._resource_type, []):
                temp_resource_id = clm_res_data.pop("id")
                logging.info(f"Processing claim response resource with temp id {temp_resource_id}")
                validator = ClaimResponse(clm_res_data, org_ref_ids, pat_ref_ids, prac_ref_ids, clm_ref_ids)
                resource_id = self._process_resource(validator, ClaimResponseEntity, "Patient", "patient")
                ids.add(resource_id)
                entity_ids.update({temp_resource_id: resource_id})
            self._ref_id_by_type[self._resource_type] = json.dumps(entity_ids)
            self._resource_ids[self._resource_type] = list(ids)
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_VALIDATION_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)

    def process_bundle(self) -> dict:
        """
        Invoke all process function and post the resource if respective resource type present.
        @param bundle: processing row identifier
        @return : dict
        """
        try:
            bundle_id = self._group_by_resource_type()
            logging.info(f"Bundle process started for bundle id -> {bundle_id}")
            for resource in self._resource_process_list:
                # if resource in self._resources:
                self._resource_process_list[resource]()
            logging.info(f"Bundle process ended for bundle id -> {bundle_id}")
            bundle_process_status = {
                "bundle_id": bundle_id,
                "file_tenant": self._file_tenant,
                "file_source": self._file_source,
                "file_resource_type": self._file_resource_type,
                "file_batch_id": self._file_batch_id,
                "src_file_name": self._src_file_name,
                "src_organization_id": self._src_organization_id,
                "resource_ids": self._resource_ids,
            }
            logging.info(f"Bundle process status -> {bundle_process_status}")
            return bundle_process_status
        except ValueError as e:
            publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_PROCESSING_ERROR.value}: {str(e)}")
            raise AirflowFailException(e)
