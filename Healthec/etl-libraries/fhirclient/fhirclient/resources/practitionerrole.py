# coding: utf-8

import json

from fhirclient.resources.base import Base
from fhirclient.resources.practitioner import Practitioner
from fhirclient.resources.organization import Organization
from fhirclient.resources.location import Location
from fhirclient.client import FHIRClient
from fhirclient.constants import ResourceType


class PractitionerRole(Base):
    """PractitionerRole resource API client"""

    def __init__(
        self, client: FHIRClient, scope: str = "Practitioner", scope_id: str = None
    ) -> None:
        resource_path = None
        if scope_id:
            resource_path = ResourceType.PractitionerRole.get_resource_path(scope, scope_id)
        super().__init__(
            client,
            ResourceType.PractitionerRole.value,
            scope=scope,
            scope_id=scope_id,
            resource_path=resource_path,
        )

    def _update_resource_reference(self, resource: dict) -> dict:
        # update practitioner internal reference
        if resource.get("practitioner"):
            pract_client = Practitioner(self._client)
            resource["practitioner"] = pract_client._update_reference(
                resource.get("practitioner")
            )
    
        # update location internal reference
        if resource.get("location"):
            loc_client = Location(self._client)
            resource["location"] = loc_client._update_reference(
                resource.get("location")
            )

        # update organization internal reference
        if resource.get("organization"):
            org_client = Organization(self._client)
            resource["organization"] = org_client._update_reference(
                resource.get("organization")
            )
        return resource

    def _resolve_scope(self, resource: dict) -> None:
        if resource.get("practitioner"):
            practitioner_reference = resource.get("practitioner")

            if practitioner_reference.get("id"):
                self._scope_id = practitioner_reference.get("id")

            elif practitioner_reference.get("reference"):
                reference = str(practitioner_reference.get("reference"))
                if len(reference.split("/")) == 2:
                    self._scope_id = reference.split("/")[1]

            elif practitioner_reference.get("identifier"):
                pract_client = Practitioner(self._client)
                match_attributes = json.dumps(
                    {"identifier": [practitioner_reference.get("identifier")]}
                )
                entities = pract_client.match(match_attributes)
                pract_id = self._get_id(entities)
                if pract_id:
                    self._scope_id = pract_id

            self._resource_path = ResourceType.PractitionerRole.get_resource_path(
                "Practitioner",
                self._scope_id
            )