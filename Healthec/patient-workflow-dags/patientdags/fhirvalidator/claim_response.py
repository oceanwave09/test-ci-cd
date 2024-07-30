from typing import Union

from patientdags.fhirvalidator.base import Base
from patientdags.utils.enum import ResourceType


class ClaimResponse(Base):
    def __init__(
        self, resource: Union[dict, str], organization_ids: str, patient_ids: str, practitioner_ids: str, claim_ids: str
    ) -> None:
        self.organization_ids = organization_ids
        self.patient_ids = patient_ids
        self.practitioner_ids = practitioner_ids
        self.claim_ids = claim_ids
        super().__init__(resource)

    def update_created_date_time(self):
        if not self.resource.get("created"):
            return
        created_date_time = self._format_datetime(self.resource.get("created"))
        if created_date_time:
            self.resource.update({"created": created_date_time})

    def update_preauth_period(self):
        if not self.resource.get("preAuthPeriod"):
            return
        start, end = self._format_period(self.resource.get("preAuthPeriod"))
        self.resource.update({"preAuthPeriod": {"start": start, "end": end}})

    def validate_patient(self):
        patient = self.resource.get("patient", {})
        if not patient.get("reference") and not patient.get("id"):
            raise ValueError("Patient reference is required for claim response resource")

    def validate_request(self):
        request = self.resource.get("request", {})
        if not request.get("reference") and not request.get("id"):
            raise ValueError("Request claim reference is required for claim response resource")

    def validate(self):
        self.validate_patient()
        self.validate_request()

    def update_references(self):
        self.update_reference_id("patient", self.patient_ids, ResourceType.Patient.value)
        self.update_reference_id("insurer", self.organization_ids, ResourceType.Organization.value)
        self.update_reference_id("requestor", self.organization_ids, ResourceType.Organization.value)
        self.update_reference_id("requestor", self.practitioner_ids, ResourceType.Practitioner.value)
        self.update_reference_id("request", self.claim_ids, ResourceType.Claim.value)

    def update_resource(self):
        self.update_resource_identifier()
        self.update_references()
        self.update_created_date_time()
        self.update_preauth_period()
