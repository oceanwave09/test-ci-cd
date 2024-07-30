from typing import Union

from patientdags.fhirvalidator.base import Base
from patientdags.utils.enum import ResourceType


class ServiceRequest(Base):
    def __init__(
        self, resource: Union[dict, str], patient_ids: str, encounter_ids: str, practitioner_ids: str
    ) -> None:
        self.patient_ids = patient_ids
        self.encounter_ids = encounter_ids
        self.practitioner_ids = practitioner_ids
        super().__init__(resource)

    def update_authored_date_time(self):
        if not self.resource.get("authoredOn"):
            return
        effective_date_time = self._format_datetime(self.resource.get("authoredOn"))
        if effective_date_time:
            self.resource.update({"authoredOn": effective_date_time})

    def validate_subject(self):
        subject = self.resource.get("subject", {})
        if not subject.get("reference") and not subject.get("id"):
            raise ValueError("Subject reference is required for service_request resource")

    def validate_encounter(self):
        subject = self.resource.get("encounter", {})
        if not subject.get("reference") and not subject.get("id"):
            raise ValueError("Encounter reference is required for service_request resource")

    def validate(self):
        self.validate_subject()
        self.validate_encounter()

    def update_references(self):
        self.update_reference_id("subject", self.patient_ids, ResourceType.Patient.value)
        self.update_reference_id("encounter", self.encounter_ids, ResourceType.Encounter.value)
        self.update_reference_id("requester", self.practitioner_ids, ResourceType.Practitioner.value)

    def update_resource(self):
        self.update_resource_identifier()
        self.update_references()
        self.update_authored_date_time()
