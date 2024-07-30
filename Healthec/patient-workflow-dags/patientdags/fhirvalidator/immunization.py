from typing import Union

from patientdags.fhirvalidator.base import Base
from patientdags.utils.enum import ResourceType


class Immunization(Base):
    def __init__(
        self,
        resource: Union[dict, str],
        organization_ids: str,
        patient_ids: str,
        encounter_ids: str,
        practitioner_ids: str,
        location_ids: str,
        condition_ids: str,
    ) -> None:
        self.organization_ids = organization_ids
        self.patient_ids = patient_ids
        self.encounter_ids = encounter_ids
        self.practitioner_ids = practitioner_ids
        self.location_ids = location_ids
        self.condition_ids = condition_ids
        super().__init__(resource)

    def update_occurrence_date_time(self):
        if not self.resource.get("occurrenceDateTime"):
            return
        occurrence_date_time = self._format_datetime(self.resource.get("occurrenceDateTime"))
        if occurrence_date_time:
            self.resource.update({"occurrenceDateTime": occurrence_date_time})

    def update_recorded_date_time(self):
        if not self.resource.get("recorded"):
            return
        recorded_date_time = self._format_datetime(self.resource.get("recorded"))
        if recorded_date_time:
            self.resource.update({"recorded": recorded_date_time})

    def validate_patient(self):
        patient = self.resource.get("patient", {})
        if not patient.get("reference") and not patient.get("id"):
            raise ValueError("Patient reference is required for immunization resource")

    def validate_vaccine_code(self):
        vaccine_code = self.resource.get("vaccineCode", {})
        if not vaccine_code.get("text") and (
            len(vaccine_code.get("coding", [])) == 0 or not vaccine_code.get("coding")[0].get("code")
        ):
            raise ValueError("Vaccine code is required for immunization resource")

    def validate(self):
        self.validate_patient()
        self.validate_vaccine_code()

    def update_references(self):
        self.update_reference_id("patient", self.patient_ids, ResourceType.Patient.value)
        self.update_reference_id("encounter", self.encounter_ids, ResourceType.Encounter.value)
        self.update_reference_id("performer", self.practitioner_ids, ResourceType.Practitioner.value)
        self.update_reference_id("location", self.location_ids, ResourceType.Location.value)
        self.update_reference_id("reasonReference", self.condition_ids, ResourceType.Condition.value)
        self.update_reference_id("manufacturer", self.organization_ids, ResourceType.Organization.value)

    def update_resource(self):
        self.update_resource_identifier()
        self.update_references()
        self.update_occurrence_date_time()
        self.update_recorded_date_time()
