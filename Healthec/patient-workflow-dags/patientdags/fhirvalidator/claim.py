from typing import Union

from patientdags.fhirvalidator.base import Base
from patientdags.utils.enum import ResourceType


class Claim(Base):
    def __init__(
        self,
        resource: Union[dict, str],
        organization_ids: str,
        patient_ids: str,
        encounter_ids: str,
        practitioner_ids: str,
        location_ids: str,
        condition_ids: str,
        procedure_ids: str,
        medication_request_ids: str,
        coverage_ids: str,
    ) -> None:
        self.organization_ids = organization_ids
        self.patient_ids = patient_ids
        self.encounter_ids = encounter_ids
        self.practitioner_ids = practitioner_ids
        self.location_ids = location_ids
        self.condition_ids = condition_ids
        self.procedure_ids = procedure_ids
        self.medication_request_ids = medication_request_ids
        self.coverage_ids = coverage_ids
        super().__init__(resource)

    def update_created_date_time(self):
        if not self.resource.get("created"):
            return
        created_date_time = self._format_datetime(self.resource.get("created"))
        if created_date_time:
            self.resource.update({"created": created_date_time})

    def update_billable_period(self):
        if not self.resource.get("billablePeriod"):
            return
        start, end = self._format_period(self.resource.get("billablePeriod"))
        self.resource.update({"billablePeriod": {"start": start, "end": end}})

    def update_serviced_period(self):
        if len(self.resource.get("item", [])) == 0:
            return
        items = self.resource.get("item", [])
        for item in items:
            if item.get("servicedDate"):
                serviced_date = self._format_date(item.get("servicedDate"))
                if serviced_date:
                    item.update({"servicedDate": serviced_date})
            if item.get("servicedPeriod"):
                start, end = self._format_period(item.get("servicedPeriod", {}))
                if start or end:
                    item.update({"servicedPeriod": {"start": start, "end": end}})

    def validate_patient(self):
        patient = self.resource.get("patient", {})
        if not patient.get("reference") and not patient.get("id"):
            raise ValueError("Patient reference is required for claim resource")

    def validate_type(self):
        type = self.resource.get("type", {})
        if not type.get("text") and (len(type.get("coding", [])) == 0 or not type.get("coding")[0].get("code")):
            raise ValueError("Type is required for claim resource")

    def validate(self):
        self.validate_resource_identifier()
        self.validate_patient()
        self.validate_type()

    def update_references(self):
        self.update_reference_id("patient", self.patient_ids, ResourceType.Patient.value)
        self.update_reference_id("enterer", self.practitioner_ids, ResourceType.Practitioner.value)
        self.update_reference_id("insurer", self.organization_ids, ResourceType.Organization.value)
        self.update_reference_id("provider", self.practitioner_ids, ResourceType.Practitioner.value)
        self.update_reference_id("provider", self.organization_ids, ResourceType.Organization.value)
        self.update_reference_id("prescription", self.medication_request_ids, ResourceType.MedicationRequest.value)
        self.update_reference_id("facility", self.location_ids, ResourceType.Location.value)
        self.update_reference_id("diagnosis", self.condition_ids, ResourceType.Condition.value)
        self.update_reference_id("procedure", self.procedure_ids, ResourceType.Procedure.value)
        self.update_reference_id("insurance", self.coverage_ids, ResourceType.Coverage.value)
        self.update_reference_id("item", self.encounter_ids, ResourceType.Encounter.value)
        self.update_reference_id("item", self.location_ids, ResourceType.Location.value)

    def update_resource(self):
        self.update_resource_identifier()
        self.update_references()
        self.update_billable_period()
        self.update_created_date_time()
        self.update_serviced_period()
