from typing import Union

from patientdags.fhirvalidator.base import Base
from patientdags.utils.enum import ResourceType


class Medication(Base):
    def __init__(self, resource: Union[dict, str], organization_ids: str) -> None:
        self.organization_ids = organization_ids
        super().__init__(resource)

    def validate_code(self):
        code = self.resource.get("code", {})
        if not code.get("text") and (len(code.get("coding", [])) == 0 or not code.get("coding")[0].get("code")):
            raise ValueError("Code is required for medication resource")

    def validate(self):
        self.validate_code()

    def update_references(self):
        self.update_reference_id("manufacturer", self.organization_ids, ResourceType.Organization.value)

    def update_resource(self):
        self.update_resource_identifier()
        self.update_references()
