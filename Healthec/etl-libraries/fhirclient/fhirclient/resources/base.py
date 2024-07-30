# coding: utf-8

import requests
import json

from fhirclient.client import FHIRClient
from fhirclient.constants import (
    ResourceType,
    RESPONSE_CODE_201,
    RESPONSE_CODE_200,
    RESPONSE_CODE_204,
)
from fhirclient.utils import remove_none_fields, is_null_or_empty
from fhirclient.exceptions import ApiException, ApiValueError


class Base(object):
    """Base resource class"""

    def __init__(
        self,
        client: FHIRClient,
        resource_type: str,
        scope: str = None,
        scope_id: str = None,
        resource_path: str = None,
    ) -> None:
        self._client = client
        self._resource_type = resource_type
        self._scope = scope
        self._scope_id = scope_id
        self._resource_path = resource_path

    def create(self, resource: str) -> dict:
        """Creates new resource"""
        self._client.prepare()
        try:
            resource_json = json.loads(resource)
            # resolve Patient/Organization scope
            if self._scope and is_null_or_empty(self._scope_id):
                self._resolve_scope(resource_json)

            # update resource internal reference
            if self._client._resolve_reference:
                resource_json = self._update_resource_reference(resource_json)

            url = self._get_url()
            headers = self._get_headers()
            payload = json.dumps(remove_none_fields(resource_json))
            response = requests.post(url, headers=headers, data=payload, verify=False)
            if response != None and response.status_code == RESPONSE_CODE_201:
                return response.json().get("data")
            else:
                self._raise_exception(
                    f"Failed to create resource {self._resource_type}", response
                )
        except ValueError as e:
            raise ApiValueError(
                f"Failed to parse the resource {self._resource_type} as json. Error: {str(e)}"
            )
        except requests.RequestException as e:
            raise ApiException(
                f"Failed to create resource {self._resource_type}", str(e)
            )

    def upsert(self, resource: str) -> dict:
        """Create/Update resource"""
        self._client.prepare()
        try:
            resource_json = json.loads(resource)
            # resolve Patient/Organization scope
            if self._scope and is_null_or_empty(self._scope_id):
                self._resolve_scope(resource_json)

            # update resource internal reference
            if self._client._resolve_reference:
                resource_json = self._update_resource_reference(resource_json)

            url = self._get_url()
            headers = self._get_headers()
            payload = json.dumps(remove_none_fields(resource_json))
            response = requests.put(url, headers=headers, data=payload, verify=False)
            if response != None and response.status_code == RESPONSE_CODE_201:
                return response.json().get("data")
            else:
                self._raise_exception(
                    f"Failed to create/update resource {self._resource_type}", response
                )
        except ValueError as e:
            raise ApiValueError(
                f"Failed to parse the resource {self._resource_type} as json. Error: {str(e)}"
            )
        except requests.RequestException as e:
            raise ApiException(
                f"Failed to create/update resource {self._resource_type}", str(e)
            )
        
    def update(self, id: str, resource: str) -> bool:
        """Updates the existing resource with internally generated FHIR resource id"""
        if is_null_or_empty(id):
            raise ApiValueError(
                f"Failed to update resource {self._resource_type}, resource id should be provided"
            )

        self._client.prepare()
        try:
            resource_json = json.loads(resource)
            # resolve Patient/Organization/Practitioner scope
            if self._scope and is_null_or_empty(self._scope_id):
                self._resolve_scope(resource)

            # update resource internal reference
            if self._client._resolve_reference:
                resource_json = self._update_resource_reference(resource_json)

            url = f"{self._get_url()}{id}"
            headers = self._get_headers()
            payload = json.dumps(remove_none_fields(resource_json))
            response = requests.put(url, headers=headers, data=payload, verify=False)
            if response != None and response.status_code == RESPONSE_CODE_204:
                return True
            else:
                self._raise_exception(
                    f"Failed to update resource {self._resource_type}", response
                )
        except ValueError as e:
            raise ApiValueError(
                f"Failed to parse the resource {self._resource_type} as json. Error: {str(e)}"
            )
        except requests.RequestException as e:
            raise ApiException(
                f"Failed to update resource {self._resource_type}", str(e)
            )

    def get(self, id: str) -> dict:
        """Gets resource by internally generated FHIR resource id"""
        if is_null_or_empty(id):
            raise ApiValueError(
                f"Failed to get resource {self._resource_type}, resource id should be provided"
            )

        self._client.prepare()
        url = f"{self._get_url()}{id}"
        headers = self._get_headers()
        try:
            response = requests.get(url, headers=headers, verify=False)
            if response != None and response.status_code == RESPONSE_CODE_200:
                return response.json().get("data")
            else:
                self._raise_exception(
                    f"Failed to get resource {self._resource_type}", response
                )
        except requests.RequestException as e:
            raise ApiException(f"Failed to get resource {self._resource_type}", str(e))

    def match(self, attributes: str) -> list:
        """Finds matching resource by given resource attributes"""
        self._client.prepare()
        try:
            attributes_json = json.loads(attributes)
            url = f"{self._get_url()}match"
            headers = self._get_headers()
            payload = json.dumps(attributes_json)
            response = requests.post(url, headers=headers, data=payload, verify=False)
            if response != None and response.status_code == RESPONSE_CODE_200:
                response_data = response.json()
                match_rules = response_data.get("data", [])
                result = []
                for match_rule in match_rules:
                    match_attribute = match_rule.get("attributes", {})
                    if match_attribute.get("isMatch", False):
                        for entity in match_attribute.get("entities", []):
                            match_entry = {}
                            match_entry["rule"] = match_rule.get("id")
                            match_entry["id"] = entity.get("id")
                            result.append(match_entry)
                return result
            else:
                # self._raise_exception("Failed to get match resource", response)
                return None
        except ValueError as e:
            raise ApiValueError(
                f"Failed to parse the resource {self._resource_type} as json. Error: {str(e)}"
            )
        except requests.RequestException as e:
            raise ApiException(
                f"Failed to get match resource {self._resource_type}", str(e)
            )

    def _get_url(self) -> str:
        if self._resource_type in [
            ResourceType.Organization.value,
            ResourceType.Location.value,
        ]:
            return f"{self._client._provider_base_url}{self._resource_path}"
        elif self._resource_type in [
            ResourceType.ClaimResponse.value,
            ResourceType.Claim.value,
            ResourceType.InsurancePlan.value,
            ResourceType.Coverage.value,
        ]:
            return f"{self._client._financial_base_url}{self._resource_path}"
        elif self._resource_type in [
            ResourceType.Medication.value
        ]:
            return f"{self._client._medication_base_url}{self._resource_path}"
        elif self._resource_type in [
            ResourceType.Practitioner.value,
            ResourceType.PractitionerRole.value,
        ]:
            return f"{self._client._user_base_url}{self._resource_path}"
        else:
            return f"{self._client._service_base_url}{self._resource_path}"

    def _get_headers(self) -> dict:
        """Generic function to generate headers for all requests"""
        headers = {"Content-Type": "application/json"}
        if not self._client._auth_disabled:
            headers["Authorization"] = self._client._auth_bearer_token
        return headers

    def _raise_exception(self, status: str, response: requests.Response) -> None:
        """Generic function to handle http exceptions"""
        error_message = response.text
        if is_null_or_empty(error_message):
            raise ApiException(status, f"{str(response.status_code)},{response.reason}")
        error_message_json = json.loads(error_message)
        if error_message_json.get("errors"):
            raise ApiException(status, http_err_resp=error_message_json)
        else:
            raise ApiException(
                status,
                f'{str(response.status_code)}, {response.reason}. {error_message_json.get("message", "")}',
            )

    def _update_reference(self, reference: dict, id: str = None) -> dict:
        if id is None and reference.get("identifier"):
            match_attributes = json.dumps({"identifier": [reference.get("identifier")]})
            entities = self.match(match_attributes)
            id = self._get_id(entities)
        if id:
            reference["reference"] = f"{self._resource_type}/{id}"
            reference["identifier"] = None
        return reference

    def _update_resource_reference(self, resource: dict) -> dict:
        return resource

    def _resolve_scope(self, resource: dict) -> None:
        pass

    def _get_id(self, entities: list = None) -> str:
        if entities and len(entities) > 0 and entities[0].get("id"):
            return entities[0].get("id")
        return None

    def _get_id_from_reference(self, reference: dict) -> str:
        if reference.get("id"):
            return reference.get("id")
        elif reference.get("reference") and len(reference.split("/")) == 2:
            return reference.split("/")[1]
