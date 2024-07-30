import json
import re
from datetime import timezone
from typing import Tuple, Union

import dateparser

from patientdags.utils.enum import ResourceType
from patientdags.utils.utils import is_empty


class Base(object):
    GENDER_DICT = {
        "M": "male",
        "F": "female",
        "T": "other",
        "O": "other",
        "U": "unknown",
    }

    def __init__(self, resource: Union[dict, str]) -> None:
        if type(resource) is str:
            self.resource = json.loads(resource)
        else:
            self.resource = resource

    def _format_datetime_value(self, value: str):
        if not value:
            return ""
        if value.isdigit():
            if len(value) == 8:
                value = f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
            elif len(value) == 12:
                value = f"{value[0:4]}-{value[4:6]}-{value[6:8]} {value[8:10]}:{value[10:12]}"
        else:
            return value

    def _format_date(self, value: str) -> str:
        value = self._format_datetime_value(value)
        value_datetime = dateparser.parse(value)
        if not value_datetime:
            return ""
        return value_datetime.strftime("%Y-%m-%d")

    def _format_datetime(self, value: str) -> str:
        value = self._format_datetime_value(value)
        value_datetime = dateparser.parse(value)
        if not value_datetime:
            return ""
        value_datetime = (
            value_datetime.replace(tzinfo=timezone.utc).isoformat()
            if value_datetime.tzname() is None
            else value_datetime.isoformat()
        )
        return value_datetime

    def _format_period(self, period: dict) -> Tuple[str, str]:
        start = period.get("start", "")
        end = period.get("end", "")
        if start:
            start = self._format_datetime(start)
        if end:
            end = self._format_datetime(end)

        if start and not end:
            end = start
        elif end and not start:
            start = end
        return start, end

    def _format_gender(self, value: str):
        first_char = value[0].upper() if value and len(value) > 0 else "U"
        return self.GENDER_DICT.get(first_char, "unknown")

    def _build_identifier_coding(self, code: str, display: str) -> dict:
        return {
            "type": {
                "coding": [
                    {
                        "code": code,
                        "display": display,
                        "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                    }
                ]
            }
        }

    def update_reference_id(self, field: str, reference_ids: str, resource_type: str, default_reference_id: str = None):
        resource_field = self.resource.get(field)
        if not resource_field:
            return
        resource_field_str = json.dumps(resource_field)
        if default_reference_id:
            regex_pattern = resource_type + r"/(\w{8}-\w{4}-\w{4}-\w{4}-\w{12})"
            ids = re.findall(regex_pattern, resource_field_str)
            for id in ids:
                resource_field_str = resource_field_str.replace(
                    f"{resource_type}/{id}", f"{resource_type}/{default_reference_id}"
                )
        else:
            reference_ids_dict = json.loads(reference_ids) if not is_empty(reference_ids) else {}
            for temp_id, actual_id in reference_ids_dict.items():
                resource_field_str = resource_field_str.replace(
                    f"{resource_type}/{temp_id}", f"{resource_type}/{actual_id}"
                )
        self.resource.update({field: json.loads(resource_field_str)})

    def update_resource_identifier(self):
        identifiers = self.resource.get("identifier", [])
        for identifier in identifiers:
            coding = identifier.get("type").get("coding", []) if identifier.get("type") else []
            system = identifier.get("system")
            if len(coding) > 0:
                continue
            if system and "https://healthec.com/identifiers/file" in system:
                continue
            # Assign default code, display
            code = "RI"
            display = "Resource Identifier"
            # update coding for standard identifiers like SSN, NPI, TIN if it's not exists
            if system:
                # SSN
                if system == "http://hl7.org/fhir/sid/us-ssn" or "2.16.840.1.113883.4.1" in system:
                    code = "SS"
                    display = "Social Security Number"
                # NPI
                elif system == "http://hl7.org/fhir/sid/us-npi" or "2.16.840.1.113883.4.6" in system:
                    code = "NPI"
                    display = "National Provider Identifier"
                # TIN
                elif "2.16.840.1.113883.4.4" in system:
                    code = "TAX"
                    display = "TAX Identifier Number"
            identifier.update(self._build_identifier_coding(code, display))

    def validate_resource_identifier(self):
        identifiers = self.resource.get("identifier", [])
        resource_type = self.__class__.__name__
        if len(identifiers) == 0 and resource_type in (
            ResourceType.Organization.value,
            ResourceType.Patient.value,
            ResourceType.Practitioner.value,
            ResourceType.Coverage.value,
            ResourceType.Claim.value,
        ):
            raise ValueError(f"Resource {resource_type} should have atleast one identifier")

    def validate(self):
        pass

    def update_resource(self):
        pass

    def update_references(self):
        pass
