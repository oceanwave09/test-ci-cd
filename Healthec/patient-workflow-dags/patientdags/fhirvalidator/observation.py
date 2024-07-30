from typing import Union

from patientdags.fhirvalidator.base import Base
from patientdags.utils.enum import ResourceType


class Observation(Base):
    def __init__(self, resource: Union[dict, str], patient_ids: str, encounter_ids: str, practitioner_ids: str) -> None:
        self.patient_ids = patient_ids
        self.encounter_ids = encounter_ids
        self.practitioner_ids = practitioner_ids
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

    def validate_subject(self):
        subject = self.resource.get("subject", {})
        if not subject.get("reference") and not subject.get("id"):
            raise ValueError("Subject reference is required for observation resource")

    def validate_code(self):
        code = self.resource.get("code", {})
        components = self.resource.get("component", [])
        code_exists = False
        component_code_exists = False
        if code.get("text") or (len(code.get("coding", [])) > 0 and code.get("coding")[0].get("code")):
            code_exists = True
        if len(components) > 0:
            component_code = components[0].get("code", {})
            if component_code.get("text") or (
                len(component_code.get("coding", [])) > 0 and component_code.get("coding")[0].get("code")
            ):
                component_code_exists = True
        if not code_exists and not component_code_exists:
            raise ValueError("Code is required for observation resource")

    def validate_value(self):
        has_value_string = False
        has_value_quantity = False
        has_value_code = False
        if self.resource.get("valueString"):
            has_value_string = True
        elif self.resource.get("valueQuantity"):
            quantity = self.resource.get("valueQuantity", {})
            if quantity.get("value"):
                has_value_quantity = True
        elif self.resource.get("valueCodeableConcept"):
            value_code = self.resource.get("valueCodeableConcept", {})
            if value_code.get("text") or (
                len(value_code.get("coding", [])) > 0 and value_code.get("coding", [])[0].get("code")
            ):
                has_value_code = True
        if not has_value_string and not has_value_quantity and not has_value_code:
            raise ValueError("Value is required for observation resource")

    def validate(self):
        self.validate_subject()
        self.validate_code()
        # self.validate_value()

    def update_references(self):
        self.update_reference_id("subject", self.patient_ids, ResourceType.Patient.value)
        self.update_reference_id("encounter", self.encounter_ids, ResourceType.Encounter.value)
        self.update_reference_id("performer", self.practitioner_ids, ResourceType.Practitioner.value)

    def update_resource(self):
        self.update_resource_identifier()
        self.update_references()
        self.update_effective_date_time()
        self.update_effective_period()
