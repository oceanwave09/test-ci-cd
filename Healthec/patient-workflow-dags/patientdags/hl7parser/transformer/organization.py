from benedict import benedict
from patientdags.hl7parser.enum import ResourceType
from patientdags.hl7parser.transformer.base import Base


class Organization(Base):
    def __init__(
        self,
        data: benedict,
        role_type: str,
        resource_type: str = ResourceType.Organization.value,
        references: dict = {},
        metadata: dict = {},
    ) -> None:
        super().__init__(data, resource_type, references, metadata)
        self._role = role_type
        self._transformer.load_template("organization.j2")

    def _parse_identifiers(self):
        self._data_dict["source_id"] = self._data.get_str(f"{self._role}_identifier")

    def _parse_name(self):
        self._data_dict["name"] = self._data.get_str(f"{self._role}_name")
        # Immunizaion only
        if self._data.get_str(f"{self._role}_text"):
            self._data_dict["name"] = self._data.get_str(f"{self._role}_text")

    def _parse_address(self):
        self._data_dict["street_address_1"] = self._data.get_str(f"{self._role}_street_address")
        self._data_dict["city"] = self._data.get_str(f"{self._role}_city")
        self._data_dict["state"] = self._data.get_str(f"{self._role}_state")
        self._data_dict["zip"] = self._data.get_str(f"{self._role}_zip")
        self._data_dict["country"] = self._data.get_str(f"{self._role}_country")

    def _parse_references(self):
        self._is_valid()
        if self._data_dict:
            self._data_dict["parent_organization_id"] = self._data.get_str("parent_organization_id")

    def build(self):
        # parse identifiers
        self._parse_identifiers()

        # update identifier system
        self._update_identifier()

        # parse name
        self._parse_name()

        # parse address
        self._parse_address()

        # parse reference
        self._parse_references()

        return self._data_dict
