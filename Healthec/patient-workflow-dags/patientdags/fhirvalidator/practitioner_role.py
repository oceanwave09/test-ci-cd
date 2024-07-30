from typing import Union

from patientdags.fhirvalidator.base import Base
from patientdags.utils.enum import ResourceType


class PractitionerRole(Base):
    def __init__(
        self,
        resource: Union[dict, str],
        organization_ids: str,
        practitioner_ids: str,
        location_ids: str,
        src_organization_id: str = None,
    ) -> None:
        self.organization_ids = organization_ids
        self.practitioner_ids = practitioner_ids
        self.location_ids = location_ids
        self.src_organization_id = src_organization_id
        super().__init__(resource)

    def update_period(self):
        if not self.resource.get("period"):
            return
        start, end = self._format_period(self.resource.get("period"))
        self.resource.update({"period": {"start": start, "end": end}})

    def update_organization(self):
        organization = self.resource.get("organization", {})
        if (not organization.get("reference") and not organization.get("id")) and self.src_organization_id:
            self.resource.update({"organization": {"reference": f"Organization/{self.src_organization_id}"}})

    def validate_organization(self):
        organization = self.resource.get("organization", {})
        if not organization.get("reference") and not organization.get("id"):
            raise ValueError("Organization reference is required for practitioner role resource")

    def validate_practitioner(self):
        practitioner = self.resource.get("practitioner", {})
        if not practitioner.get("reference") and not practitioner.get("id"):
            raise ValueError("Practitioner reference is required for practitioner role resource")

    def validate(self):
        self.validate_practitioner()
        self.validate_organization()

    def update_references(self):
        self.update_reference_id(
            "organization", self.organization_ids, ResourceType.Organization.value, self.src_organization_id
        )
        self.update_reference_id("practitioner", self.practitioner_ids, ResourceType.Practitioner.value)
        self.update_reference_id("location", self.location_ids, ResourceType.Location.value)

    def update_resource(self):
        self.update_resource_identifier()
        self.update_references()
        self.update_period()
        self.update_organization()
