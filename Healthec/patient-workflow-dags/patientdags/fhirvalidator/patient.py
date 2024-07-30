from typing import Union

from patientdags.fhirvalidator.base import Base
from patientdags.utils.enum import ResourceType


class Patient(Base):
    COMMA_SYMBOL = ","
    RESTRICT_SRC_ORG_BY_FILE_TYPE = ["HL7", "MEDICAL_CLAIM", "RX_CLAIM"]
    UNKNOWN_RACE_EXTENSION = [
        {
            "system": "http://terminology.hl7.org/CodeSystem/v3-Race",
            "url": "ombCategory",
            "valueCoding": {
                "code": "UNK",
                "display": "unknown",
                "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
            },
        },
        {
            "url": "text",
            "valueString": "unknown",
        },
    ]
    UNKNOWN_ETHNICITY_EXTENSION = [
        {
            "system": "http://terminology.hl7.org/CodeSystem/v3-Ethnicity",
            "url": "ombCategory",
            "valueCoding": {
                "code": "UNK",
                "display": "unknown",
                "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
            },
        },
        {
            "url": "text",
            "valueString": "unknown",
        },
    ]
    ASSIGNER_IDENTIFIER_CODES = ["MR", "MB"]

    def __init__(
        self,
        resource: Union[dict, str],
        organization_ids: str,
        practitioner_ids: str,
        src_organization_id: str = None,
    ) -> None:
        self.organization_ids = organization_ids
        self.practitioner_ids = practitioner_ids
        self.src_organization_id = src_organization_id
        super().__init__(resource)

    def update_name(self):
        if len(self.resource.get("name", [])) == 0:
            return
        names = self.resource.get("name", [])
        for name in names:
            if name.get("family") and self.COMMA_SYMBOL in name.get("family") and len(name.get("given", [])) == 0:
                name["given"] = [name.get("family").split(",")[0]]
                name["family"] = name.get("family").split(",")[1]

    def _check_extensions(self, extensions: list, unknown_extension: list) -> list:
        if len(extensions) == 0:
            return unknown_extension
        else:
            ombcategory = False
            ombcategory_count = 0
            text = False
            value = ""
            for extension in extensions:
                if extension.get("url") == "ombCategory" and extension.get("valueCoding"):
                    value = extension.get("valueCoding", {}).get("display", "")
                    # TODO: Remove `none` if platform service accepts race, ethinicity without display, text
                    if not value:
                        value = "none"
                        extension.get("valueCoding").update({"display": value})
                    ombcategory = True
                    ombcategory_count = ombcategory_count + 1
                if extension.get("url") == "text" and extension.get("valueString"):
                    text = True
            if not ombcategory:
                return unknown_extension
            if not text and ombcategory:
                if ombcategory_count == 1:
                    extensions.append(
                        {
                            "url": "text",
                            "valueString": value,
                        }
                    )
                else:
                    extensions.append(
                        {
                            "url": "text",
                            "valueString": "mixed",
                        }
                    )
        return extensions

    def update_birth_date(self):
        if not self.resource.get("birthDate"):
            return
        birth_date = self._format_date(self.resource.get("birthDate"))
        if birth_date:
            self.resource.update({"birthDate": birth_date})

    def update_gender(self):
        if not self.resource.get("gender"):
            return
        gender = self._format_gender(self.resource.get("gender"))
        self.resource.update({"gender": gender})

    def update_managing_organization(self):
        managing_organization = self.resource.get("managingOrganization", {})
        if (
            not managing_organization.get("reference") and not managing_organization.get("id")
        ) and self.src_organization_id:
            self.resource.update({"managingOrganization": {"reference": f"Organization/{self.src_organization_id}"}})

    def update_assigner_organization(self, identifier_codes: list):
        if not self.resource.get("managingOrganization", {}):
            return
        managing_organization = self.resource.get("managingOrganization", {})
        identifiers = self.resource.get("identifier", {})
        updated_identifiers = []
        for identifier in identifiers:
            if (
                identifier.get("assigner")
                and len(identifier.get("type", {}).get("coding", [])) > 0
                and identifier.get("type").get("coding")[0].get("code", "") in identifier_codes
            ):
                identifier.update({"assigner": managing_organization})
            updated_identifiers.append(identifier)
        self.resource.update({"identifier": updated_identifiers})

    def validate_race_and_ethnicity(self):
        if not self.resource.get("extension"):
            self.resource.update({"extension": []})
        extensions = self.resource.get("extension")
        has_race = False
        has_ethnicity = False
        for extension in extensions:
            if extension.get("url") == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race":
                race_extensions = extension.get("extension", [])
                updated_race_extensions = self._check_extensions(race_extensions, self.UNKNOWN_RACE_EXTENSION)
                extension.update(
                    {
                        "extension": updated_race_extensions,
                    }
                )
                has_race = True
            elif extension.get("url") == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity":
                ethnicity_extensions = extension.get("extension", [])
                updated_ethnicity_extensions = self._check_extensions(
                    ethnicity_extensions, self.UNKNOWN_ETHNICITY_EXTENSION
                )
                extension.update(
                    {
                        "extension": updated_ethnicity_extensions,
                    }
                )
                has_ethnicity = True
        # Add "unknown" race if it does not exists
        if not has_race:
            self.resource["extension"].append(
                {
                    "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                    "extension": self.UNKNOWN_RACE_EXTENSION,
                }
            )
        # Add "unknown" ethnicity if it does not exists
        if not has_ethnicity:
            self.resource["extension"].append(
                {
                    "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
                    "extension": self.UNKNOWN_ETHNICITY_EXTENSION,
                }
            )

    def validate_identifier(self):
        identifiers = self.resource.get("identifier", [])
        has_identifier = False
        for identifier in identifiers:
            if identifier.get("value"):
                has_identifier = True
                break
        if not has_identifier:
            raise ValueError("Atleast one identifier is required for patient resource")

    def validate_managing_organization(self):
        managing_organization = self.resource.get("managingOrganization", {})
        if not managing_organization.get("reference") and not managing_organization.get("id"):
            raise ValueError("Managing organization reference is required for patient resource")

    def validate_name(self):
        if len(self.resource.get("name", [])) == 0:
            raise ValueError("Firstname is required for patient resource")
        names = self.resource.get("name", [])
        for name in names:
            if len(name.get("given", [])) == 0:
                raise ValueError("Firstname is required for patient resource")

    def validate_birth_date(self):
        birth_date = self.resource.get("birthDate", "")
        if not birth_date:
            raise ValueError("Birth date is required for patient resource")

    def validate_gender(self):
        gender = self.resource.get("gender", "")
        if not gender:
            raise ValueError("Gender is required for patient resource")

    def validate(self):
        self.validate_resource_identifier()
        self.validate_name()
        self.validate_identifier()
        self.validate_managing_organization()
        self.validate_race_and_ethnicity()
        self.validate_birth_date()
        self.validate_gender()

    def update_references(self, file_resource_type: str = None):
        get_src_organization_id = self.src_organization_id
        if file_resource_type and file_resource_type in self.RESTRICT_SRC_ORG_BY_FILE_TYPE:
            get_src_organization_id = None
        self.update_reference_id(
            "managingOrganization", self.organization_ids, ResourceType.Organization.value, get_src_organization_id
        )
        self.update_reference_id("generalPractitioner", self.practitioner_ids, ResourceType.Practitioner.value)

    def update_resource(self, file_resource_type: str = None):
        self.update_resource_identifier()
        self.update_references(file_resource_type=file_resource_type)
        self.update_name()
        self.update_birth_date()
        self.update_gender()
        self.update_managing_organization()
        self.update_assigner_organization(identifier_codes=self.ASSIGNER_IDENTIFIER_CODES)
