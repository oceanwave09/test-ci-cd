# coding: utf-8

import json

from fhirclient.resources.base import Base
from fhirclient.resources.patient import Patient
from fhirclient.resources.coverage import Coverage
from fhirclient.resources.organization import Organization
from fhirclient.client import FHIRClient
from fhirclient.constants import ResourceType


class Claim(Base):
    """Claim resource API client"""

    def __init__(
        self, client: FHIRClient, scope: str = "Patient", scope_id: str = None
    ) -> None:
        resource_path = None
        if scope_id:
            resource_path = ResourceType.Claim.get_resource_path(scope, scope_id)
        super().__init__(
            client,
            ResourceType.Claim.value,
            scope=scope,
            scope_id=scope_id,
            resource_path=resource_path,
        )

    def _update_resource_reference(self, resource: dict) -> dict:
        # update patient internal reference
        if resource.get("patient"):
            patient_client = Patient(self._client)
            resource["patient"] = patient_client._update_reference(
                resource.get("patient"), id=self._scope_id
            )

        # update coverage internal reference
        if resource.get("insurance") and len(resource.get("insurance")) > 0:
            updated_references = []
            references = resource.get("insurance")
            cov_client = Coverage(self._client)
            for reference in references:
                reference["coverage"] = cov_client._update_reference(
                    reference.get("coverage")
                )
                updated_references.append(reference)
            resource["insurance"] = updated_references

        # update organization internal reference
        if resource.get("provider"):
            organization_client = Organization(self._client)
            resource["provider"] = organization_client._update_reference(
                resource.get("provider")
            )
        return resource
    

    def _resolve_scope(self, resource: dict) -> None:
        if resource.get("patient"):
            patient_subject = resource.get("patient")

            if patient_subject.get("id"):
                self._scope_id = patient_subject.get("id")

            elif patient_subject.get("reference"):
                reference = str(patient_subject.get("reference"))
                if len(reference.split("/")) == 2:
                    self._scope_id = reference.split("/")[1]

            elif patient_subject.get("identifier"):
                patient_client = Patient(self._client)
                match_attributes = json.dumps(
                    {"identifier": [patient_subject.get("identifier")]}
                )
                entities = patient_client.match(match_attributes)
                patient_id = self._get_id(entities)
                if patient_id:
                    self._scope_id = patient_id

            self._resource_path = ResourceType.Claim.get_resource_path(
                "Patient",
                self._scope_id
            )
