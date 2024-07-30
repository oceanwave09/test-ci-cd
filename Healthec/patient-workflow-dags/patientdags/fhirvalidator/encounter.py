from typing import Union

from patientdags.fhirvalidator.base import Base
from patientdags.utils.enum import ResourceType


class Encounter(Base):
    def __init__(
        self,
        resource: Union[dict, str],
        patient_ids: str,
        practitioner_ids: str,
        organization_ids: str,
        location_ids: str,
        src_organization_ids: str = None,
    ) -> None:
        self.patient_ids = patient_ids
        self.practitioner_ids = practitioner_ids
        self.organization_ids = organization_ids
        self.location_ids = location_ids
        self.src_organization_ids = src_organization_ids
        super().__init__(resource)

    def update_period(self):
        if not self.resource.get("period"):
            return
        start, end = self._format_period(self.resource.get("period"))
        self.resource.update({"period": {"start": start, "end": end}})

    def validate_subject(self):
        subject = self.resource.get("subject", {})
        if not subject.get("reference") and not subject.get("id"):
            raise ValueError("Subject reference is required for encounter resource")

    def validate(self):
        self.validate_subject()

    def update_references(self):
        self.update_reference_id("subject", self.patient_ids, ResourceType.Patient.value)
        self.update_reference_id("participant", self.practitioner_ids, ResourceType.Practitioner.value)
        self.update_reference_id("location", self.location_ids, ResourceType.Location.value)
        self.update_reference_id("hospitalization", self.location_ids, ResourceType.Location.value)
        self.update_reference_id(
            "serviceProvider", self.organization_ids, ResourceType.Organization.value, self.src_organization_ids
        )

    def update_resource(self):
        self.update_resource_identifier()
        self.update_references()
        self.update_period()
