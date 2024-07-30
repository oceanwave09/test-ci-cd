from typing import Union

from patientdags.fhirvalidator.base import Base
from patientdags.utils.enum import ResourceType

CONDITION_CLINICAL_SYSTEM = "http://terminology.hl7.org/CodeSystem/condition-clinical"


class Condition(Base):
    def __init__(self, resource: Union[dict, str], patient_ids: str, encounter_ids: str, practitioner_ids: str) -> None:
        self.patient_ids = patient_ids
        self.encounter_ids = encounter_ids
        self.practitioner_ids = practitioner_ids
        super().__init__(resource)

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

    def update_abatement_date_time(self):
        if not self.resource.get("abatementDateTime"):
            return
        abatement_date_time = self._format_datetime(self.resource.get("abatementDateTime"))
        if abatement_date_time:
            self.resource.update({"abatementDateTime": abatement_date_time})

    def update_abatement_period(self):
        if not self.resource.get("abatementPeriod"):
            return
        start, end = self._format_period(self.resource.get("abatementPeriod"))
        self.resource.update({"abatementPeriod": {"start": start, "end": end}})

    def update_clinical_status(self):
        if not self.resource.get("clinicalStatus"):
            self.resource.update(
                {"clinicalStatus": {"coding": [{"code": "active", "system": CONDITION_CLINICAL_SYSTEM}]}}
            )

    def validate_subject(self):
        subject = self.resource.get("subject", {})
        if not subject.get("reference") and not subject.get("id"):
            raise ValueError("Subject reference is required for condition resource")

    def validate_code(self):
        code = self.resource.get("code", {})
        if not code.get("text") and (len(code.get("coding", [])) == 0 or not code.get("coding")[0].get("code")):
            raise ValueError("Code is required for condition resource")

    def validate(self):
        self.validate_subject()
        self.validate_code()

    def update_references(self):
        self.update_reference_id("subject", self.patient_ids, ResourceType.Patient.value)
        self.update_reference_id("encounter", self.encounter_ids, ResourceType.Encounter.value)
        self.update_reference_id("asserter", self.practitioner_ids, ResourceType.Practitioner.value)

    def update_resource(self):
        self.update_resource_identifier()
        self.update_references()
        self.update_onset_date_time()
        self.update_onset_period()
        self.update_abatement_date_time()
        self.update_abatement_period()
        self.update_clinical_status()
