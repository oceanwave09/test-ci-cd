# coding: utf-8

import json

from fhirclient.resources.base import Base
from fhirclient.resources.patient import Patient
from fhirclient.client import FHIRClient
from fhirclient.constants import ResourceType


class RelatedPerson(Base):
    """RelatedPerson resource API client"""

    def __init__(
        self, client: FHIRClient, scope: str = "Patient", scope_id: str = None
    ) -> None:
        resource_path = None
        if scope_id:
            resource_path = ResourceType.RelatedPerson.get_resource_path(scope, scope_id)
        super().__init__(
            client,
            ResourceType.RelatedPerson.value,
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
        return resource


    def _resolve_scope(self, resource: dict) -> None:
        if resource.get("patient"):
            patient = resource.get("patient")

            if patient.get("id"):
                self._scope_id = patient.get("id")

            elif patient.get("reference"):
                reference = str(patient.get("reference"))
                if len(reference.split("/")) == 2:
                    self._scope_id = reference.split("/")[1]

            elif patient.get("identifier"):
                patient_client = Patient(self._client)
                match_attributes = json.dumps(
                    {"identifier": [patient.get("identifier")]}
                )
                entities = patient_client.match(match_attributes)
                patient_id = self._get_id(entities)
                if patient_id:
                    self._scope_id = patient_id

            self._resource_path = ResourceType.RelatedPerson.get_resource_path(
                "Patient",
                self._scope_id
            )

