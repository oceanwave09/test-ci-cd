from typing import Union

from patientdags.fhirvalidator.base import Base
from patientdags.utils.enum import ResourceType


class Procedure(Base):
    def __init__(
        self,
        resource: Union[dict, str],
        patient_ids: str,
        encounter_ids: str,
        practitioner_ids: str,
        location_ids: str,
        condition_ids: str,
    ) -> None:
        self.patient_ids = patient_ids
        self.encounter_ids = encounter_ids
        self.practitioner_ids = practitioner_ids
        self.location_ids = location_ids
        self.condition_ids = condition_ids
        super().__init__(resource)

    def update_performed_date_time(self):
        if not self.resource.get("performedDateTime"):
            return
        performed_date_time = self._format_datetime(self.resource.get("performedDateTime"))
        if performed_date_time:
            self.resource.update({"performedDateTime": performed_date_time})

    def update_performed_period(self):
        if not self.resource.get("performedPeriod"):
            return
        start, end = self._format_period(self.resource.get("performedPeriod"))
        self.resource.update({"performedPeriod": {"start": start, "end": end}})

    def validate_subject(self):
        subject = self.resource.get("subject", {})
        if not subject.get("reference") and not subject.get("id"):
            raise ValueError("Subject reference is required for procedure resource")

    def validate_code(self):
        code = self.resource.get("code", {})
        if not code.get("text") and (len(code.get("coding", [])) == 0 or not code.get("coding")[0].get("code")):
            raise ValueError("Code is required for procedure resource")

    def validate_encounter(self):
        encounter = self.resource.get("encounter", {})
        if not encounter.get("reference") and not encounter.get("id"):
            raise ValueError("Encounter reference is required for procedure resource")

    def validate(self):
        self.validate_subject()
        # self.validate_encounter()
        self.validate_code()

    def update_references(self):
        self.update_reference_id("subject", self.patient_ids, ResourceType.Patient.value)
        self.update_reference_id("encounter", self.encounter_ids, ResourceType.Encounter.value)
        self.update_reference_id("performer", self.practitioner_ids, ResourceType.Practitioner.value)
        self.update_reference_id("location", self.location_ids, ResourceType.Location.value)
        self.update_reference_id("reasonReference", self.condition_ids, ResourceType.Condition.value)

    def update_resource(self):
        self.update_resource_identifier()
        self.update_references()
        self.update_performed_date_time()
        self.update_performed_period()
