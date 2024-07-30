import json
import uuid

from benedict import benedict
from fhirtransformer.transformer import FHIRTransformer
from patientdags.hl7parser.enum import ResourceType
from patientdags.hl7parser.constants import (
    RESOURCE_IDENTIFIER_SYSTEM,
    MRN_IDENTIFIER_SYSTEM,
    MEMBER_SYSTEM,
    MBI_SYSTEM,
    MEDICAID_SYSTEM,
    ADDITIONAL_KEYS,
    SUBSCRIBER_SYSTEM,
    # DRIVING_LICENSE_SYSTEM ,
    # STATE_LICENSE_SYSTEM ,
    # MEDICARE_SYSTEM ,
    PATIENT_INTERNAL_SYSTEM,
    PATIENT_EXTERNAL_SYSTEM,
)


class Base(object):
    def __init__(self, data: benedict, resource_type: str, references: dict = {}, metadata: dict = {}) -> None:
        self._data = data
        self._resource_type = resource_type
        self._references = references
        self._metadata = metadata
        self._transformer = FHIRTransformer()
        self.resource = {}
        self._data_dict = {}

    def _is_valid(self) -> None:
        _valid_flag = True
        for _key in self._data_dict:
            if _key not in ADDITIONAL_KEYS and self._data_dict.get(_key):
                self._set_internal_id()
                _valid_flag = False
                break
        if _valid_flag:
            self._data_dict = {}

    def _get_organization_reference(self):
        if self._metadata.get("child_org_id", ""):
            return self._metadata.get("child_org_id", "")
        elif self._references.get("organization_reference", []):
            return self._references.get("organization_reference", [])[0]
        return ""

    def _prepare_resource(self, resource: str) -> dict:
        res_dict = json.loads(resource)
        res_id = res_dict.get("id")
        self.resource = {
            "fullUrl": f"urn:uuid:{res_id}",
            "resource": res_dict,
            "request": {"method": "PUT", "url": f"{res_dict.get('resourceType')}/{res_id}"},
        }

    def _construct_reference(self, ref_id: str, temp_key: str, return_type: str) -> any:
        if return_type in [list, dict]:
            return {temp_key: ref_id}
        else:
            return ref_id

    def _get_reference(self, key: str, return_type: any = str, temp_key: str = "id"):
        get_reference = self._references.get(key, [])
        references = []
        if get_reference:
            if isinstance(get_reference, list):
                for ref_id in get_reference:
                    references.append(self._construct_reference(ref_id, temp_key, return_type))
            else:
                references.append(self._construct_reference(get_reference, temp_key, return_type))
            if references:
                references = references if return_type is list else references[0]
        return references

    def _set_internal_id(self):
        self._data_dict["internal_id"] = str(uuid.uuid4())

    def _update_identifier(self):
        if self._data_dict.get("mrn"):
            self._data_dict["mrn_system"] = MRN_IDENTIFIER_SYSTEM
        if self._data_dict.get("member_id"):
            self._data_dict["member_system"] = MEMBER_SYSTEM
        if self._data_dict.get("medicaid_id"):
            self._data_dict["medicaid_system"] = MEDICAID_SYSTEM
        if self._data_dict.get("mbi"):
            self._data_dict["mbi_system"] = MBI_SYSTEM
        if self._data_dict.get("subscriber_id"):
            self._data_dict["subscriber_system"] = SUBSCRIBER_SYSTEM
        if self._data_dict.get("patient_internal_id"):
            self._data_dict["patient_internal_system"] = PATIENT_INTERNAL_SYSTEM
        if self._data_dict.get("patient_external_id"):
            self._data_dict["patient_external_system"] = PATIENT_EXTERNAL_SYSTEM
        if not self._data_dict.get("source_system"):
            if "source_id" in self._data_dict and self._data_dict.get("source_id"):
                self._data_dict["source_system"] = RESOURCE_IDENTIFIER_SYSTEM
        if self._resource_type != ResourceType.Organization.value:
            self._data_dict["source_file_name"] = self._metadata.get("file_format", "")
            self._data_dict["assigner_organization_id"] = self._metadata.get("child_org_id", "")

    def build(self):
        pass

    def transform(self):
        if not self._data_dict:
            return {}
        # render resource
        resource = self._transformer.render_resource(self._resource_type, self._data_dict)
        # self._prepare_resource(resource)
        return resource
