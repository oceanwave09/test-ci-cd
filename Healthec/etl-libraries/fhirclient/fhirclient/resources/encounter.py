# coding: utf-8

import json

from fhirclient.resources.base import Base
from fhirclient.resources.patient import Patient
from fhirclient.resources.organization import Organization
from fhirclient.resources.location import Location
from fhirclient.resources.practitioner import Practitioner
from fhirclient.resources.practitionerrole import PractitionerRole
from fhirclient.client import FHIRClient
from fhirclient.constants import ResourceType


class Encounter(Base):
    """Encounter resource API client"""

    def __init__(
        self, client: FHIRClient, scope: str = "Patient", scope_id: str = None
    ) -> None:
        resource_path = None
        if scope_id:
            resource_path = ResourceType.Encounter.get_resource_path(scope, scope_id)
        super().__init__(
            client,
            ResourceType.Encounter.value,
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

        # update practitioner or practitioner role internal reference
        if resource.get("participant") and len(resource.get("participant")) > 0:
            updated_references = []
            references = resource.get("participant")
            loc_client = Location(self._client)
            for reference in references:
                if reference.get("individual"):
                    if reference["individual"].get("type") == ResourceType.PractitionerRole.value:
                        client = PractitionerRole(self._client)
                    else:
                        client = Practitioner(self._client)
                    reference["individual"] = client._update_reference(
                        reference.get("individual")
                    )
                updated_references.append(reference)
            resource["participant"] = updated_references

        # update location internal reference
        if resource.get("location") and len(resource.get("location")) > 0:
            updated_references = []
            references = resource.get("location")
            loc_client = Location(self._client)
            for reference in references:
                if reference.get("location"):
                    reference["location"] = loc_client._update_reference(
                        reference.get("location")
                    )
                updated_references.append(reference)
            resource["location"] = updated_references

        # update organization internal reference
        if resource.get("serviceProvider"):
            org_client = Organization(self._client)
            resource["serviceProvider"] = org_client._update_reference(
                resource.get("serviceProvider")
            )

        # update parent encounter internal reference
        if resource.get("partOf"):
            resource["partOf"] = self._update_reference(resource.get("partOf"))
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

            self._resource_path = ResourceType.Encounter.get_resource_path(
                "Patient",
                self._scope_id
            )
