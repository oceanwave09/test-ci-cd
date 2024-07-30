import logging
from typing import Union

from patientdags.fhirvalidator.base import Base


class Organization(Base):
    def __init__(self, resource: Union[dict, str]) -> None:
        super().__init__(resource)

    def validate_name(self):
        if not self.resource.get("name"):
            raise ValueError("Organization name should not be empty or null")

    def update_type(self):
        if not self.resource.get("type"):
            logging.info("Organization type is not provided, updating with default type `other`")
            self.resource.update(
                {
                    "type": [
                        {
                            "coding": [
                                {
                                    "code": "other",
                                    "display": "Other",
                                    "system": "http://terminology.hl7.org/CodeSystem/organization-type",
                                }
                            ]
                        }
                    ]
                }
            )

    def validate_tin(self):
        type = "other"
        if len(self.resource.get("type", [])) > 0:
            types_struct = self.resource.get("type")
            for type_struct in types_struct:
                if len(type_struct.get("coding", [])) > 0:
                    type = type_struct["coding"][0]["code"]
                    break
        if type == "prov":
            identifiers = self.resource.get("identifier", [])
            for identifier in identifiers:
                if (
                    identifier.get("type")
                    and len(identifier.get("type").get("coding", [])) > 0
                    and identifier["type"]["coding"][0]["code"] == "TAX"
                ):
                    return
            raise ValueError("Tax id is required for organization of type `prov`")

    def validate_identifier(self):
        identifiers = self.resource.get("identifier", [])
        has_identifier = False
        for identifier in identifiers:
            if identifier.get("value"):
                has_identifier = True
                break
        if not has_identifier:
            raise ValueError("Atleast one identifier is required for organization resource")

    def validate(self):
        self.validate_resource_identifier()
        self.validate_name()
        self.validate_tin()
        self.validate_identifier()

    def update_resource(self):
        self.update_resource_identifier()
        self.update_references()
        self.update_type()
