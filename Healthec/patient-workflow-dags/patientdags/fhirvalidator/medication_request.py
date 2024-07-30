from typing import Union

from patientdags.fhirvalidator.base import Base
from patientdags.utils.enum import ResourceType


class MedicationRequest(Base):
    def __init__(
        self,
        resource: Union[dict, str],
        patient_ids: str,
        encounter_ids: str,
        practitioner_ids: str,
        medication_ids: str,
    ) -> None:
        self.patient_ids = patient_ids
        self.encounter_ids = encounter_ids
        self.practitioner_ids = practitioner_ids
        self.medication_ids = medication_ids
        super().__init__(resource)

    def update_authored_date_time(self):
        if not self.resource.get("authoredOn"):
            return
        authored_date_time = self._format_datetime(self.resource.get("authoredOn"))
        if authored_date_time:
            self.resource.update({"authoredOn": authored_date_time})

    def validate_subject(self):
        subject = self.resource.get("subject", {})
        if not subject.get("reference") and not subject.get("id"):
            raise ValueError("Subject reference is required for medication request resource")

    def validate_medication(self):
        has_medication_reference = False
        has_medication_code = False
        if self.resource.get("medicationReference"):
            medication_reference = self.resource.get("medicationReference", {})
            if medication_reference.get("reference") or medication_reference.get("id"):
                has_medication_reference = True
        elif self.resource.get("medicationCodeableConcept"):
            medication_code = self.resource.get("medicationCodeableConcept", {})
            if medication_code.get("text") or (
                len(medication_code.get("coding", [])) > 0 and medication_code.get("coding", [])[0].get("code")
            ):
                has_medication_code = True
        if not has_medication_reference and not has_medication_code:
            raise ValueError("Medication is required for medication request resource")

    def validate(self):
        self.validate_subject()
        self.validate_medication()

    def update_references(self):
        self.update_reference_id("subject", self.patient_ids, ResourceType.Patient.value)
        self.update_reference_id("encounter", self.encounter_ids, ResourceType.Encounter.value)
        self.update_reference_id("performer", self.practitioner_ids, ResourceType.Practitioner.value)
        self.update_reference_id("medicationReference", self.medication_ids, ResourceType.Medication.value)

    def update_resource(self):
        self.update_resource_identifier()
        self.update_references()
        self.update_authored_date_time()
