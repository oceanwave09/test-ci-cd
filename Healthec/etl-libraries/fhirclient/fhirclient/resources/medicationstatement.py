# coding: utf-8

import json

from fhirclient.resources.base import Base
from fhirclient.resources.patient import Patient
from fhirclient.resources.encounter import Encounter
from fhirclient.resources.practitioner import Practitioner
from fhirclient.resources.practitionerrole import PractitionerRole
from fhirclient.resources.medication import Medication
from fhirclient.resources.condition import Condition
from fhirclient.client import FHIRClient
from fhirclient.constants import ResourceType


class MedicationStatement(Base):
    """Medication Statement resource API client"""

    def __init__(
        self, client: FHIRClient, scope: str = "Patient", scope_id: str = None
    ) -> None:
        resource_path = None
        if scope_id:
            resource_path = ResourceType.MedicationStatement.get_resource_path(scope, scope_id)
        super().__init__(
            client,
            ResourceType.MedicationStatement.value,
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
        if resource.get("context"):
            encounter_client = Encounter(self._client, scope_id=self._scope_id)
            resource["context"] = encounter_client._update_reference(
                resource.get("context")
            )

        # update medication internal reference
        if resource.get("medication") and resource["medication"].get("medicationReference"):
            medi_client = Medication(self._client)
            resource["medication"]["medicationReference"] = medi_client._update_reference(
                resource["medication"].get("medicationReference")
            )

        # update practitioner or practitioner role internal reference
        if resource.get("informationSource"):
            reference = resource.get("informationSource")
            if reference.get("type") == ResourceType.PractitionerRole.value:
                client = PractitionerRole(self._client)
            else:
                client = Practitioner(self._client)
            resource["informationSource"] = client._update_reference(reference)
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

            self._resource_path = ResourceType.MedicationStatement.get_resource_path(
                "Patient",
                self._scope_id
            )
