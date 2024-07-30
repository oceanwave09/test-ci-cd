# coding: utf-8
import json

from fhirclient.resources.base import Base
from fhirclient.resources.organization import Organization
from fhirclient.client import FHIRClient
from fhirclient.constants import ResourceType


class Location(Base):
    """Location resource API client"""

    def __init__(
        self, client: FHIRClient, scope: str = "Organization", scope_id: str = None
    ) -> None:
        resource_path = None
        if scope_id:
            resource_path = ResourceType.Location.get_resource_path(scope, scope_id)
        super().__init__(
            client,
            ResourceType.Location.value,
            scope=scope,
            scope_id=scope_id,
            resource_path=resource_path,
        )

    def _update_resource_reference(self, resource: dict) -> dict:
        # update organization internal reference
        if resource.get("managingOrganization"):
            org_client = Organization(self._client)
            resource["managingOrganization"] = org_client._update_reference(
                resource.get("managingOrganization")
            )
        
        # update location internal reference
        if resource.get("partOf"):
            resource["partOf"] = self._update_reference(resource.get("partOf"))
        return resource

    def _resolve_scope(self, resource: dict) -> None:
        if resource.get("managingOrganization"):
            managing_organization = resource.get("managingOrganization")

            if managing_organization.get("id"):
                self._scope_id = managing_organization.get("id")

            elif managing_organization.get("reference"):
                reference = str(managing_organization.get("reference"))
                if len(reference.split("/")) == 2:
                    self._scope_id = reference.split("/")[1]

            elif managing_organization.get("identifier"):
                org_client = Organization(self._client)
                match_attributes = json.dumps(
                    {"identifier": [managing_organization.get("identifier")]}
                )
                entities = org_client.match(match_attributes)
                org_id = self._get_id(entities)
                if org_id:
                    self._scope_id = org_id

            self._resource_path = ResourceType.Location.get_resource_path(
                "Organization",
                self._scope_id
            )