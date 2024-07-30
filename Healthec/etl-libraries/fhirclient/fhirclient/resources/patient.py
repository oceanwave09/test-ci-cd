# coding: utf-8

from fhirclient.resources.base import Base
from fhirclient.resources.organization import Organization
from fhirclient.resources.practitioner import Practitioner
from fhirclient.resources.practitionerrole import PractitionerRole
from fhirclient.client import FHIRClient
from fhirclient.constants import ResourceType


class Patient(Base):
    """Patient resource API client"""

    def __init__(self, client: FHIRClient) -> None:
        super().__init__(
            client,
            ResourceType.Patient.value,
            resource_path=ResourceType.Patient.get_resource_path(),
        )

    def _update_resource_reference(self, resource: dict) -> dict:
        # update organization internal reference
        if resource.get("managingOrganization"):
            org_client = Organization(self._client)
            resource["managingOrganization"] = org_client._update_reference(
                resource.get("managingOrganization")
            )

        # update practitioner or practitioner role internal reference
        if resource.get("generalPractitioner") and len(resource.get("generalPractitioner")) > 0:
            updated_references = []
            references = resource.get("generalPractitioner")
            for reference in references:
                if reference.get("type") == ResourceType.PractitionerRole.value:
                    client = PractitionerRole(self._client)
                else:
                    client = Practitioner(self._client)
                updated_references.append(client._update_reference(reference))
            resource["generalPractitioner"] = updated_references
        return resource
