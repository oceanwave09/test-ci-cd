from typing import Union

from patientdags.fhirvalidator.base import Base
from patientdags.utils.enum import ResourceType


class AllergyIntolerance(Base):
    def __init__(self, resource: Union[dict, str], patient_ids: str, encounter_ids: str, practitioner_ids: str) -> None:
        self.patient_ids = patient_ids
        self.encounter_ids = encounter_ids
        self.practitioner_ids = practitioner_ids
        super().__init__(resource)

    def update_type(self):
        if self.resource.get("type") not in ["allergy", "intolerance"]:
            self.resource.update({"type": "allergy"})

    def update_onset_date_time(self):
        if not self.resource.get("onsetDateTime"):
            return
        onset_date_time = self._format_datetime(self.resource.get("onsetDateTime"))
        if onset_date_time:
            self.resource.update({"onsetDateTime": onset_date_time})

    def update_onset_period(self):
        if not self.resource.get("onsetPeriod"):
            return
        start, end = self._format_period(self.resource.get("onsetPeriod"))
        self.resource.update({"onsetPeriod": {"start": start, "end": end}})

    def update_recorded_date_time(self):
        if not self.resource.get("recordedDate"):
            return
        recorded_date_time = self._format_datetime(self.resource.get("recordedDate"))
        if recorded_date_time:
            self.resource.update({"recordedDate": recorded_date_time})

    def validate_patient(self):
        patient = self.resource.get("patient", {})
        if not patient.get("reference") and not patient.get("id"):
            raise ValueError("Patient reference is required for allergy intolerance resource")

    def validate_code(self):
        code = self.resource.get("code", {})
        if not code.get("text") and (len(code.get("coding", [])) == 0 or not code.get("coding")[0].get("code")):
            raise ValueError("Code is required for allergy intolerance resource")

    def validate(self):
        self.validate_patient()
        self.validate_code()

    def update_references(self):
        self.update_reference_id("patient", self.patient_ids, ResourceType.Patient.value)
        self.update_reference_id("encounter", self.encounter_ids, ResourceType.Encounter.value)
        self.update_reference_id("asserter", self.practitioner_ids, ResourceType.Practitioner.value)

    def update_resource(self):
        self.update_type()
        self.update_resource_identifier()
        self.update_references()
        self.update_onset_date_time()
        self.update_onset_period()
        self.update_recorded_date_time()
