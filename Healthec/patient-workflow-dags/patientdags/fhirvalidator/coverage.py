import re
from typing import Union

from patientdags.fhirvalidator.base import Base
from patientdags.utils.enum import ResourceType


class Coverage(Base):
    def __init__(
        self, resource: Union[dict, str], organization_ids: str, patient_ids: str, insurance_plan_ids: str
    ) -> None:
        self.organization_ids = organization_ids
        self.patient_ids = patient_ids
        self.insurance_plan_ids = insurance_plan_ids
        super().__init__(resource)

    def update_period(self):
        if not self.resource.get("period"):
            return
        start, end = self._format_period(self.resource.get("period"))
        self.resource.update({"period": {"start": start, "end": end}})

    def update_assigner_organization(self, resource_type: str):
        identifiers = self.resource.get("identifier", {})
        updated_identifiers = []
        for identifier in identifiers:
            if identifier.get("assigner") and len(identifier.get("type", {}).get("coding", [])) > 0:
                resource_field_str = identifier.get("assigner", {}).get("reference")
                regex_pattern = resource_type + r"/(\w{8}-\w{4}-\w{4}-\w{4}-\w{12})"
                ids = re.findall(regex_pattern, resource_field_str)
                if len(ids) and ids[0] and self.organization_ids.get(ids[0]):
                    identifier.update({"assigner": self.organization_ids.get(ids[0])})
                else:
                    identifier.pop("assigner")
            updated_identifiers.append(identifier)
        self.resource.update({"identifier": updated_identifiers})

    def validate_beneficiary(self):
        beneficiary = self.resource.get("beneficiary", {})
        if not beneficiary.get("reference") and not beneficiary.get("id"):
            raise ValueError("Beneficiary reference is required for coverage resource")

    def validate_insurer(self):
        insurer = self.resource.get("insurer", {})
        if not insurer.get("reference") and not insurer.get("id"):
            raise ValueError("Insurer reference is required for coverage resource")

    def validate_insurance_plan(self):
        insurance_plan = self.resource.get("insurancePlan", {})
        if not insurance_plan.get("reference") and not insurance_plan.get("id"):
            raise ValueError("Insurance plan reference is required for coverage resource")

    def validate_status(self):
        status = self.resource.get("status", "")
        if not status:
            raise ValueError("Status is required for coverage resource")

    def validate_kind(self):
        kind = self.resource.get("kind", "")
        if not kind:
            raise ValueError("Kind is required for coverage resource")

    def validate(self):
        self.validate_resource_identifier()
        self.validate_beneficiary()
        self.validate_insurer()
        # self.validate_insurance_plan()
        self.validate_status()
        self.validate_kind()

    def update_references(self):
        self.update_reference_id("subscriber", self.patient_ids, ResourceType.Patient.value)
        self.update_reference_id("beneficiary", self.patient_ids, ResourceType.Patient.value)
        self.update_reference_id("insurer", self.organization_ids, ResourceType.Organization.value)
        self.update_reference_id("insurancePlan", self.insurance_plan_ids, ResourceType.InsurancePlan.value)

    def update_resource(self):
        self.update_resource_identifier()
        # self.update_assigner_organization(resource_type=ResourceType.Organization.value)
        self.update_references()
        self.update_period()
