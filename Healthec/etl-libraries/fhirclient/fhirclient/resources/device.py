# coding: utf-8

from fhirclient.resources.base import Base
from fhirclient.resources.patient import Patient
from fhirclient.resources.organization import Organization
from fhirclient.resources.location import Location
from fhirclient.client import FHIRClient
from fhirclient.constants import ResourceType


class Device(Base):
    """Device resource API client"""

    def __init__(self, client: FHIRClient) -> None:
        super().__init__(
            client,
            ResourceType.Device.value,
            resource_path=ResourceType.Device.get_resource_path(),
        )

    def _update_resource_reference(self, resource: dict) -> dict:
        # update patient internal reference
        if resource.get("patient"):
            patient_client = Patient(self._client)
            resource["patient"] = patient_client._update_reference(
                resource.get("patient")
            )

        # update organization internal reference
        if resource.get("owner"):
            org_client = Organization(self._client)
            resource["owner"] = org_client._update_reference(
                resource.get("owner")
            )

        # update location internal reference
        if resource.get("location"):
            loc_client = Location(self._client)
            resource["location"] = loc_client._update_reference(
                resource.get("location")
            )

        # update device internal reference
        if resource.get("parent"):
            resource["parent"] = self._update_reference(resource.get("parent"))
        return resource
