# coding: utf-8
from fhirclient.resources.base import Base
from fhirclient.client import FHIRClient
from fhirclient.constants import ResourceType


class OrganizationAffiliation(Base):
    """OrgnizationAffiliation resource API client"""

    def __init__(
        self, client: FHIRClient, scope: str = "Organization", scope_id: str = None
    ) -> None:
        resource_path = None
        if scope_id:
            resource_path = ResourceType.OrganizationAffiliation.get_resource_path(scope, scope_id)
        super().__init__(
            client,
            ResourceType.OrganizationAffiliation.value,
            scope=scope,
            scope_id=scope_id,
            resource_path=resource_path,
        )

    def _update_resource_reference(self, resource: dict) -> dict:
        # update Organizationaffliation internal reference
        if resource.get("organization"):
            resource["organization"] = self._update_reference(resource.get("organization"))
        return resource
    

    def _resolve_scope(self, resource: dict) -> None:
         if resource.get("organization"):
            organization = resource.get("organization")

            if organization.get("id"):
                self._scope_id = organization.get("id")

            elif organization.get("reference"):
                reference = str(organization.get("reference"))
                if len(reference.split("/")) == 2:
                    self._scope_id = reference.split("/")[1]
            self._resource_path = ResourceType.OrganizationAffiliation.get_resource_path(
                "Organization",
                self._scope_id
            )
