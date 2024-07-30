# coding: utf-8

import json

from fhirclient.resources.base import Base
from fhirclient.resources.patient import Patient
from fhirclient.resources.organization import Organization
from fhirclient.resources.claim import Claim
from fhirclient.client import FHIRClient
from fhirclient.constants import ResourceType


class ClaimResponse(Base):
    """ClaimResponse resource API client"""

    def __init__(
        self, client: FHIRClient, scope: str = "Patient", scope_id: str = None
    ) -> None:
        resource_path = None
        if scope_id:
            resource_path = ResourceType.ClaimResponse.get_resource_path(scope, scope_id)
        super().__init__(
            client,
            ResourceType.ClaimResponse.value,
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

        # update payer organization internal reference
        if resource.get("insurer"):
            org_client = Organization(self._client)
            resource["insurer"] = org_client._update_reference(resource.get("insurer"))

        # update provider organization internal reference
        if resource.get("requestor"):
            org_client = Organization(self._client)
            resource["requestor"] = org_client._update_reference(resource.get("requestor"))

        # update claim internal reference
        if resource.get("request"):
            clm_client = Claim(self._client)
            resource["request"] = clm_client._update_reference(resource.get("request"))

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

            self._resource_path = ResourceType.ClaimResponse.get_resource_path(
                "Patient",
                self._scope_id
            )
