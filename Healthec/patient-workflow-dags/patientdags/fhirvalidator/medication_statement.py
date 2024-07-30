from typing import Union

from patientdags.fhirvalidator.base import Base
from patientdags.utils.enum import ResourceType


class MedicationStatement(Base):
    def __init__(
        self,
        resource: Union[dict, str],
        patient_ids: str,
        encounter_ids: str,
        practitioner_ids: str,
        medication_ids: str,
        condition_ids: str,
    ) -> None:
        self.patient_ids = patient_ids
        self.encounter_ids = encounter_ids
        self.practitioner_ids = practitioner_ids
        self.medication_ids = medication_ids
        self.condition_ids = condition_ids
        super().__init__(resource)

    def update_effective_date_time(self):
        if not self.resource.get("effectiveDateTime"):
            return
        effective_date_time = self._format_datetime(self.resource.get("effectiveDateTime"))
        if effective_date_time:
            self.resource.update({"effectiveDateTime": effective_date_time})

    def update_effective_period(self):
        if not self.resource.get("effectivePeriod"):
            return
        start, end = self._format_period(self.resource.get("effectivePeriod"))
        self.resource.update({"effectivePeriod": {"start": start, "end": end}})

    def update_asserted_date_time(self):
        if not self.resource.get("dateAsserted"):
            return
        asserted_date_time = self._format_datetime(self.resource.get("dateAsserted"))
        if asserted_date_time:
            self.resource.update({"dateAsserted": asserted_date_time})

    def validate_subject(self):
        subject = self.resource.get("subject", {})
        if not subject.get("reference") and not subject.get("id"):
            raise ValueError("Subject reference is required for medication statement resource")

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
            raise ValueError("Medication is required for medication statement resource")

    def validate(self):
        self.validate_subject()
        self.validate_medication()

    def update_references(self):
        self.update_reference_id("subject", self.patient_ids, ResourceType.Patient.value)
        self.update_reference_id("context", self.encounter_ids, ResourceType.Encounter.value)
        self.update_reference_id("informationSource", self.practitioner_ids, ResourceType.Practitioner.value)
        self.update_reference_id("medicationReference", self.medication_ids, ResourceType.Medication.value)
        self.update_reference_id("reasonReference", self.condition_ids, ResourceType.Condition.value)

    def update_resource(self):
        self.update_resource_identifier()
        self.update_references()
        self.update_effective_date_time()
        self.update_effective_period()
        self.update_asserted_date_time()
