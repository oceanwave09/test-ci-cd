import json
import uuid
from collections import OrderedDict

from benedict import benedict
from fhirtransformer.transformer import FHIRTransformer

from patientdags.ccdaparser.constants import (
    NPI_IDENTIFIER_SYSTEMS,
    SSN_IDENTIFIER_SYSTEMS,
    TAX_IDENTIFIER_SYSTEMS,
)


class Base(object):
    def __init__(self, data: benedict, resource_type: str, references: dict = {}, metadata: dict = {}) -> None:
        self._data = data
        self._resource_type = resource_type
        self._references = references
        self._metadata = metadata
        self._transformer = FHIRTransformer()
        self.resource = {}
        self.data_dict = {}

    def _get_organization_reference(self):
        if self._metadata.get("src_organization_id", ""):
            return self._metadata.get("src_organization_id", "")
        if self._references.get("Organization", []):
            return self._references.get("Organization", [])[0]
        return ""

    def _prepare_resource(self, resource: str) -> dict:
        res_dict = json.loads(resource)
        res_id = res_dict.get("id")
        self.resource = {
            "fullUrl": f"urn:uuid:{res_id}",
            "resource": res_dict,
            "request": {"method": "PUT", "url": f"{res_dict.get('resourceType')}/{res_id}"},
        }

    def _get_patient_reference(self):
        if self._references.get("Patient", []):
            return self._references.get("Patient", [])[0]
        return ""

    def _set_internal_id(self):
        self.data_dict["internal_id"] = str(uuid.uuid4())

    def _parse_identifier(self):
        for identifier in self._data.get_list("identifier"):
            # do not process if the identifier value does not exist
            if not identifier.get_str("value"):
                continue
            if identifier.get_str("system") in TAX_IDENTIFIER_SYSTEMS:
                self.data_dict["tax_id"] = identifier.get_str("value")
            elif identifier.get_str("system") in NPI_IDENTIFIER_SYSTEMS:
                self.data_dict["npi"] = identifier.get_str("value")
            elif identifier.get_str("system") in SSN_IDENTIFIER_SYSTEMS:
                self.data_dict["ssn"] = identifier.get_str("value")
            elif self._resource_type == "Organization":
                self.data_dict["source_id"] = identifier.get_str("value")
                self.data_dict["source_system"] = identifier.get_str("system")
            elif self._resource_type == "Patient":
                self.data_dict["mrn"] = identifier.get_str("value")
                self.data_dict["mrn_system"] = identifier.get_str("system")
                self.data_dict["assigner_organization_id"] = self._get_organization_reference()
            else:
                self.data_dict["source_id"] = identifier.get_str("value")
                self.data_dict["source_system"] = identifier.get_str("system")
                self.data_dict["source_file_name"] = self._metadata.get("file_format", "")
                self.data_dict["assigner_organization_id"] = self._get_organization_reference()

    def _parse_name(self):
        self.data_dict["name"] = self._data.get_str("name")

    def _parse_human_name(self):
        self.data_dict["lastname"] = self._data.get_str("name[0].family")
        self.data_dict["firstname"] = self._data.get_str("name[0].given[0]")
        self.data_dict["middleinitials"] = self._data.get_str("name[0].given[1]")

    def _parse_address(self):
        address = self._data.get("address")
        if address and isinstance(address, list):
            self.data_dict["street_address_1"] = self._data.get_str("address[0].line[0]")
            self.data_dict["street_address_2"] = self._data.get_str("address[0].line[1]")
            self.data_dict["city"] = self._data.get_str("address[0].city")
            self.data_dict["state"] = self._data.get_str("address[0].state")
            self.data_dict["country"] = self._data.get_str("address[0].country")
            self.data_dict["zip"] = self._data.get_str("address[0].postal_code")
        elif address and isinstance(address, dict):
            self.data_dict["street_address_1"] = self._data.get_str("address.line[0]")
            self.data_dict["street_address_2"] = self._data.get_str("address.line[1]")
            self.data_dict["city"] = self._data.get_str("address.city")
            self.data_dict["state"] = self._data.get_str("address.state")
            self.data_dict["country"] = self._data.get_str("address.country")
            self.data_dict["zip"] = self._data.get_str("address.postal_code")

    def _parse_phone_work(self):
        phone_work = self._data.get("phone_work")
        if phone_work and isinstance(phone_work, list):
            self.data_dict["phone_work"] = self._data.get("phone_work[0]").replace("tel:", "")
        elif phone_work and isinstance(phone_work, str):
            self.data_dict["phone_work"] = self._data.get_str("phone_work").replace("tel:", "")

    def _parse_phone_home(self):
        phone_home = self._data.get("phone_home")
        if phone_home and isinstance(phone_home, list):
            self.data_dict["phone_home"] = self._data.get("phone_home[0]").replace("tel:", "")
        elif phone_home and isinstance(phone_home, str):
            self.data_dict["phone_home"] = self._data.get_str("phone_home").replace("tel:", "")

    def _parse_phone_mobile(self):
        phone_mobile = self._data.get("phone_mobile")
        if phone_mobile and isinstance(phone_mobile, list):
            self.data_dict["phone_mobile"] = self._data.get("phone_mobile[0]").replace("mob:", "").replace("tel:", "")
        elif phone_mobile and isinstance(phone_mobile, str):
            self.data_dict["phone_mobile"] = self._data.get_str("phone_mobile").replace("mob:", "").replace("tel:", "")

    def _parse_email(self):
        email = self._data.get("email")
        if email and isinstance(email, list):
            self.data_dict["email"] = self._data.get("email[0]").replace("mailto:", "")
        elif email and isinstance(email, str):
            self.data_dict["email"] = self._data.get_str("email").replace("mailto:", "")

    def _parse_patient_reference(self):
        self.data_dict["patient_id"] = self._get_patient_reference()

    def _parse_organization_reference(self):
        self.data_dict["organization_id"] = self._get_organization_reference()

    def _parse_type(self):
        type = self._data.get("type")
        if type and isinstance(type, list):
            self.data_dict["type_code"] = self._data.get_str("type[0].code")
            self.data_dict["type_system"] = self._data.get_str("type[0].system")
            self.data_dict["type_display"] = self._data.get_str("type[0].display")
            self.data_dict["type_text"] = (
                self._data.get_str("type[0].display")
                if self._data.get_str("type[0].display")
                else self._data.get_str("type[0].text")
            )
        elif type and isinstance(type, dict):
            self.data_dict["type_code"] = self._data.get_str("type.code")
            self.data_dict["type_system"] = self._data.get_str("type.system")
            self.data_dict["type_display"] = self._data.get_str("type.display")
            self.data_dict["type_text"] = (
                self._data.get_str("type.display")
                if self._data.get_str("type.display")
                else self._data.get_str("type.text")
            )

    def _parse_category(self):
        category = self._data.get("category")
        if category and isinstance(category, list):
            self.data_dict["category_code"] = self._data.get_str("category[0].code")
            self.data_dict["category_system"] = self._data.get_str("category[0].system")
            self.data_dict["category_display"] = self._data.get_str("category[0].display")
            self.data_dict["category_text"] = (
                self._data.get_str("category[0].display")
                if self._data.get_str("category[0].display")
                else self._data.get_str("category[0].text")
            )
        elif category and isinstance(category, dict):
            self.data_dict["category_code"] = self._data.get_str("category.code")
            self.data_dict["category_system"] = self._data.get_str("category.system")
            self.data_dict["category_display"] = self._data.get_str("category.display")
            self.data_dict["category_text"] = (
                self._data.get_str("category.display")
                if self._data.get_str("category.display")
                else self._data.get_str("category.text")
            )

    def _parse_code(self, prefer_code_systems: list = []):
        code_dict = OrderedDict()
        if self._data.get_str("code.code"):
            code_entry = {
                "code": self._data.get_str("code.code"),
                "code_system": self._data.get_str("code.system"),
                "code_display": self._data.get_str("code.display"),
                "code_text": (
                    self._data.get_str("code.display")
                    if self._data.get_str("code.display")
                    else self._data.get_str("code.text")
                ),
            }
            key = code_entry.get("code_system") if code_entry.get("code_system") else code_entry.get("code")
            code_dict[key] = code_entry
        for entry in self._data.get_list("code.translation"):
            if entry.get_str("code"):
                code_entry = {
                    "code": entry.get_str("code"),
                    "code_system": entry.get_str("system"),
                    "code_display": entry.get_str("display"),
                    "code_text": (entry.get_str("display") if entry.get_str("display") else entry.get_str("text")),
                }
                key = code_entry.get("code_system") if code_entry.get("code_system") else code_entry.get("code")
                code_dict[key] = code_entry
        if not code_dict:
            return
        if not prefer_code_systems:
            self.data_dict.update(code_dict.popitem(last=False)[1])
            return
        keys = code_dict.keys()
        code_added = False
        for code_system in prefer_code_systems:
            for key in keys:
                if code_system in key:
                    self.data_dict.update(code_dict.get(key))
                    code_added = True
                    break
            if code_added:
                break
        if not code_added:
            self.data_dict.update(code_dict.popitem(last=False)[1])
        return

    def _parse_site(self):
        site = self._data.get("site")
        if site and isinstance(site, list):
            self.data_dict["site_code"] = self._data.get_str("site[0].code")
            self.data_dict["site_system"] = self._data.get_str("site[0].system")
            self.data_dict["site_display"] = self._data.get_str("site[0].display")
            self.data_dict["site_text"] = (
                self._data.get_str("site[0].display")
                if self._data.get_str("site[0].display")
                else self._data.get_str("site[0].text")
            )
        elif site and isinstance(site, dict):
            self.data_dict["site_code"] = self._data.get_str("site.code")
            self.data_dict["site_system"] = self._data.get_str("site.system")
            self.data_dict["site_display"] = self._data.get_str("site.display")
            self.data_dict["site_text"] = (
                self._data.get_str("site.display")
                if self._data.get_str("site.display")
                else self._data.get_str("site.text")
            )

    def _parse_route(self):
        route = self._data.get("route")
        if route and isinstance(route, list):
            self.data_dict["route_code"] = self._data.get_str("route[0].code")
            self.data_dict["route_system"] = self._data.get_str("route[0].system")
            self.data_dict["route_display"] = self._data.get_str("route[0].display")
            self.data_dict["route_text"] = (
                self._data.get_str("route[0].display")
                if self._data.get_str("route[0].display")
                else self._data.get_str("route[0].text")
            )
        elif route and isinstance(route, dict):
            self.data_dict["route_code"] = self._data.get_str("route.code")
            self.data_dict["route_system"] = self._data.get_str("route.system")
            self.data_dict["route_display"] = self._data.get_str("route.display")
            self.data_dict["route_text"] = (
                self._data.get_str("route.display")
                if self._data.get_str("route.display")
                else self._data.get_str("route.text")
            )

    def _parse_reason_code(self, code_system: str = "icd"):
        added_prefer_code = False
        added_code = False
        if self._data.get_str("reasons[0].code.code"):
            self.data_dict["reason_code"] = self._data.get_str("reasons[0].code.code")
            self.data_dict["reason_system"] = self._data.get_str("reasons[0].code.system")
            self.data_dict["reason_display"] = self._data.get_str("reasons[0].code.display")
            self.data_dict["reason_text"] = (
                self._data.get_str("reasons[0].code.display")
                if self._data.get_str("reasons[0].code.display")
                else self._data.get_str("reasons[0].code.text")
            )
            if code_system and code_system in self._data.get_str("reasons[0].code.system"):
                added_prefer_code = True
            added_code = True
        if not added_prefer_code and len(self._data.get_list("reasons[0].code.translation")) > 0:
            for entry in self._data.get_list("reasons[0].code.translation"):
                if code_system and code_system in entry.get_str("system_name").lower():
                    self.data_dict["reason_code"] = entry.get_str("code")
                    self.data_dict["reason_system"] = entry.get_str("system")
                    self.data_dict["reason_display"] = entry.get_str("display")
                    self.data_dict["reason_text"] = (
                        entry.get_str("display") if entry.get_str("display") else entry.get_str("text")
                    )
                    added_prefer_code = True
                    added_code = True
            if not added_code:
                self.data_dict["reason_code"] = self._data.get_str("reasons[0].code.translation[0].code")
                self.data_dict["reason_system"] = self._data.get_str("reasons[0].code.translation[0].system")
                self.data_dict["reason_display"] = self._data.get_str("reasons[0].code.translation[0].display")
                self.data_dict["reason_text"] = (
                    self._data.get_str("reasons[0].code.translation[0].display")
                    if self._data.get_str("reasons[0].code.translation[0].display")
                    else self._data.get_str("reasons[0].code.translation[0].text")
                )

    def build(self):
        pass

    def transform(self):
        if not self.data_dict:
            self.build()
        # render resource
        resource = self._transformer.render_resource(self._resource_type, self.data_dict)
        self._prepare_resource(resource)
        return self.resource
