from benedict import benedict
from patientdags.hl7parser.enum import ResourceType
from patientdags.hl7parser.transformer.base import Base


class Location(Base):
    def __init__(
        self,
        data: benedict,
        role_type: str,
        resource_type: str = ResourceType.Location.value,
        references: dict = {},
        metadata: dict = {},
    ) -> None:
        super().__init__(data, resource_type, references, metadata)
        self._role = role_type
        self._transformer.load_template("location.j2")

    def _parse_identifiers(self):
        self._data_dict["source_id"] = self._data.get_str(f"{self._role}_location_identifier")

    def _parse_name(self):
        self._data_dict["name"] = self._data.get_str(f"{self._role}_facility")

    def _parse_status(self):
        self._data_dict["status"] = self._data.get_str(f"{self._role}_location_status")

    def _parse_location_type(self):
        self._data_dict["type_text"] = self._data.get_str(f"{self._role}_location_type")

    def _parse_address(self):
        self._data_dict["street_address_1"] = self._data.get_str(f"{self._role}_location_building")
        self._data_dict["street_address_2"] = self._data.get_str(f"{self._role}_location_floor")

    def _parse_references(self):
        self._is_valid()
        if self._data_dict:
            self._data_dict["organization_id"] = self._metadata.get("child_org_id", "")

    def build(self):
        # parse identifiers
        self._parse_identifiers()

        # update identifier system
        self._update_identifier()

        # parse name
        self._parse_name()

        # parse status
        self._parse_status()

        # parse location type
        self._parse_location_type()

        # parse address
        self._parse_address()

        # parse reference
        self._parse_references()

        return self._data_dict
