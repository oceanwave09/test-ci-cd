# coding: utf-8

import json

from fhirclient.resources.base import Base
from fhirclient.resources.condition import Condition
from fhirclient.resources.patient import Patient
from fhirclient.resources.location import Location
from fhirclient.resources.encounter import Encounter
from fhirclient.resources.practitioner import Practitioner
from fhirclient.resources.practitionerrole import PractitionerRole
from fhirclient.client import FHIRClient
from fhirclient.constants import ResourceType


class Procedure(Base):
    """Procedure resource API client"""

    def __init__(
        self, client: FHIRClient, scope: str = "Patient", scope_id: str = None
    ) -> None:
        resource_path = None
        if scope_id:
            resource_path = ResourceType.Procedure.get_resource_path(scope, scope_id)
        super().__init__(
            client,
            ResourceType.Procedure.value,
            scope=scope,
            scope_id=scope_id,
            resource_path=resource_path,
        )

    def _update_resource_reference(self, resource: dict) -> dict:
        # update patient internal reference
        if resource.get("subject"):
            patient_client = Patient(self._client)
            resource["subject"] = patient_client._update_reference(
                resource.get("subject"), id=self._scope_id
            )

        # update encounter internal reference
        if resource.get("encounter"):
            enc_client = Encounter(self._client, scope_id=self._scope_id)
            resource["encounter"] = enc_client._update_reference(
                resource.get("encounter")
            )

        # update practitioner internal reference
        if resource.get("performer") and len(resource.get("performer")) > 0:
            updated_references = []
            references = resource.get("performer")
            for reference in references:
                if reference.get("actor"):
                    if reference["actor"].get("type") == ResourceType.PractitionerRole.value:
                        client = PractitionerRole(self._client)
                    else:
                        client = Practitioner(self._client)
                    reference["actor"] = client._update_reference(reference.get("actor"))
                updated_references.append(reference)
            resource["performer"] = updated_references

        # update location internal reference
        if resource.get("location"):
            loc_client = Location(self._client)
            resource["location"] = loc_client._update_reference(
                resource.get("location")
            )

        # update condition internal reference
        if resource.get("complicationDetail") and len(resource.get("complicationDetail")) > 0:
            updated_references = []
            references = resource.get("complicationDetail")
            cond_client = Condition(self._client)
            for reference in references:
                updated_references.append(cond_client._update_reference(reference))
            resource["complicationDetail"] = updated_references
        return resource

    def _resolve_scope(self, resource: dict) -> None:
        if resource.get("subject"):
            patient_subject = resource.get("subject")

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

            self._resource_path = ResourceType.Procedure.get_resource_path(
                "Patient",
                self._scope_id
            )
