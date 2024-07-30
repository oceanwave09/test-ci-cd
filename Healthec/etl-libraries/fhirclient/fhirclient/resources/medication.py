# coding: utf-8

from fhirclient.resources.base import Base
from fhirclient.resources.organization import Organization
from fhirclient.client import FHIRClient
from fhirclient.constants import ResourceType


class Medication(Base):
    """Medication resource API client"""

    def __init__(self, client: FHIRClient) -> None:
        super().__init__(
            client,
            ResourceType.Medication.value,
            resource_path=ResourceType.Medication.get_resource_path(),
        )

    def _update_resource_reference(self, resource: dict) -> dict:
        # update organization internal reference
        if resource.get("manufacturer"):
            org_client = Organization(self._client)
            resource["manufacturer"] = org_client._update_reference(
                resource.get("manufacturer")
            )
        return resource
