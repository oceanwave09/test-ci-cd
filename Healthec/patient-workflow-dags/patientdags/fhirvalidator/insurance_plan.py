from typing import Union

from patientdags.fhirvalidator.base import Base
from patientdags.utils.enum import ResourceType


class InsurancePlan(Base):
    def __init__(self, resource: Union[dict, str], organization_ids: str) -> None:
        self.organization_ids = organization_ids
        super().__init__(resource)

    def update_period(self):
        if not self.resource.get("period"):
            return
        start, end = self._format_period(self.resource.get("period"))
        self.resource.update({"period": {"start": start, "end": end}})

    def validate_name(self):
        if not self.resource.get("name"):
            raise ValueError("Name is required for insurance plan resource")

    def validate(self):
        self.validate_name()

    def update_references(self):
        self.update_reference_id("ownedBy", self.organization_ids, ResourceType.Organization.value)
        self.update_reference_id("administeredBy", self.organization_ids, ResourceType.Organization.value)
        self.update_reference_id("network", self.organization_ids, ResourceType.Organization.value)

    def update_resource(self):
        self.update_resource_identifier()
        self.update_references()
        self.update_period()
