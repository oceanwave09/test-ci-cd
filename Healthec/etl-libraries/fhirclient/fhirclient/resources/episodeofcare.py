# coding: utf-8

import json

from fhirclient.resources.base import Base
from fhirclient.resources.patient import Patient
from fhirclient.resources.organization import Organization
from fhirclient.resources.practitioner import Practitioner
from fhirclient.resources.practitionerrole import PractitionerRole
from fhirclient.resources.condition import Condition
from fhirclient.client import FHIRClient
from fhirclient.constants import ResourceType


class EpisodeOfCare(Base):
    """EpisodeOfCare resource API client"""

    def __init__(
        self, client: FHIRClient, scope: str = "Patient", scope_id: str = None
    ) -> None:
        resource_path = None
        if scope_id:
            resource_path = ResourceType.EpisodeOfCare.get_resource_path(scope, scope_id)
        super().__init__(
            client,
            ResourceType.EpisodeOfCare.value,
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

        # update organization internal reference
        if resource.get("managingOrganization"):
            org_client = Organization(self._client)
            resource["managingOrganization"] = org_client._update_reference(
                resource.get("managingOrganization")
            )

        # update practitioner or practitioner role internal reference
        if resource.get("careManager"):
            reference = resource.get("careManager")
            if reference.get("type") == ResourceType.PractitionerRole.value:
                client = PractitionerRole(self._client)
            else:
                client = Practitioner(self._client)
            resource["careManager"] = client._update_reference(reference)

        # update condition internal reference
        if resource.get("diagnosis") and len(resource.get("diagnosis")) > 0:
            updated_references = []
            references = resource.get("diagnosis")
            cond_client = Condition(self._client)
            for reference in references:
                if reference.get("condition"):
                    reference["condition"] = cond_client._update_reference(
                        reference.get("condition")
                    )
                updated_references.append(reference)
            resource["diagnosis"] = updated_references
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

            self._resource_path = ResourceType.EpisodeOfCare.get_resource_path(
                "Patient",
                self._scope_id
            )
