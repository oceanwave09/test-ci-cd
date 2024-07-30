# coding: utf-8

from fhirclient.resources.base import Base
from fhirclient.client import FHIRClient
from fhirclient.constants import ResourceType


class Organization(Base):
    """Organization resource API client"""

    def __init__(self, client: FHIRClient) -> None:
        super().__init__(
            client,
            ResourceType.Organization.value,
            resource_path=ResourceType.Organization.get_resource_path(),
        )

    def _update_resource_reference(self, resource: dict) -> dict:
        # update organization internal reference
        if resource.get("partOf"):
            resource["partOf"] = self._update_reference(resource.get("partOf"))
        return resource
