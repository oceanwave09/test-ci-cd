# coding: utf-8

from fhirclient.resources.base import Base
from fhirclient.resources.patient import Patient
from fhirclient.resources.encounter import Encounter
from fhirclient.resources.condition import Condition
from fhirclient.resources.practitioner import Practitioner
from fhirclient.resources.practitionerrole import PractitionerRole
from fhirclient.client import FHIRClient
from fhirclient.constants import ResourceType


class CarePlan(Base):
    """CarePlan resource API client"""

    def __init__(self, client: FHIRClient) -> None:
        super().__init__(
            client,
            ResourceType.CarePlan.value,
            resource_path=ResourceType.CarePlan.get_resource_path(),
        )

    def _update_resource_reference(self, resource: dict) -> dict:
        # update careplan internal reference
        if resource.get("partOf") and len(resource.get("partOf")) > 0:
            updated_references = []
            references = resource.get("partOf")
            for reference in references:
                updated_references.append(self._update_reference(reference))
            resource["partOf"] = updated_references

        # update patient internal reference
        patient_id = ""
        if resource.get("subject"):
            patient_client = Patient(self._client)
            resource["subject"] = patient_client._update_reference(
                resource.get("subject")
            )
            patient_id = self._get_id_from_reference(resource.get("subject"))

        # update encounter internal reference
        if resource.get("encounter") and patient_id:
            enc_client = Encounter(self._client, scope_id=patient_id)
            resource["encounter"] = enc_client._update_reference(
                resource.get("encounter")
            )

        # update practitioner or practitioner role internal reference
        if resource.get("author"):
            reference = resource.get("author")
            if reference.get("type") == ResourceType.PractitionerRole.value:
                client = PractitionerRole(self._client)
            else:
                client = Practitioner(self._client)
            resource["author"] = client._update_reference(reference)

        # update condition internal reference
        if resource.get("addresses") and len(resource.get("addresses")) > 0:
            updated_references = []
            references = resource.get("addresses")
            cond_client = Condition(self._client)
            for reference in references:
                updated_references.append(cond_client._update_reference(reference))
            resource["addresses"] = updated_references
        return resource
