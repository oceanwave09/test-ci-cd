from typing import Union

from patientdags.fhirvalidator.base import Base
from patientdags.utils.enum import ResourceType


class Location(Base):
    def __init__(self, resource: Union[dict, str], organization_ids: str, src_organization_id: str = None) -> None:
        self.organization_ids = organization_ids
        self.src_organization_id = src_organization_id
        super().__init__(resource)

    def update_managing_organization(self):
        managing_organization = self.resource.get("managingOrganization", {})
        if (
            not managing_organization.get("reference") and not managing_organization.get("id")
        ) and self.src_organization_id:
            self.resource.update({"managingOrganization": {"reference": f"Organization/{self.src_organization_id}"}})

    def validate_name(self):
        if not self.resource.get("name"):
            raise ValueError("Location name should not be empty or null")

    def validate_address(self):
        address = self.resource.get("address", {})
        if not address.get("city") and not address.get("state") and not address.get("zip"):
            raise ValueError("Address is required for location resource")

    def validate_managing_organization(self):
        managing_organization = self.resource.get("managingOrganization", {})
        if not managing_organization.get("reference") and not managing_organization.get("id"):
            raise ValueError("Managing organization reference is required for location resource")

    def validate(self):
        self.validate_name()
        self.validate_managing_organization()
        self.validate_address()

    def update_references(self):
        self.update_reference_id(
            "managingOrganization", self.organization_ids, ResourceType.Organization.value, self.src_organization_id
        )

    def update_resource(self):
        self.update_resource_identifier()
        self.update_references()
        self.update_managing_organization()
