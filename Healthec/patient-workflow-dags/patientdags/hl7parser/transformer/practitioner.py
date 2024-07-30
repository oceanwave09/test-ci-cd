from benedict import benedict
from patientdags.hl7parser.enum import ResourceType
from patientdags.hl7parser.transformer.base import Base


class Practitioner(Base):
    def __init__(
        self,
        data: benedict,
        role_type: str,
        resource_type: str = ResourceType.Practitioner.value,
        references: dict = {},
        metadata: dict = {},
    ) -> None:
        super().__init__(data, resource_type, references, metadata)
        self._role = role_type
        self._transformer.load_template("practitioner.j2")

    def _parse_identifiers(self):
        self._data_dict["npi"] = self._data.get_str(f"{self._role}_identifier")

    def _parse_name(self):
        self._data_dict["lastname"] = self._data.get_str(f"{self._role}_family_name")
        self._data_dict["firstname"] = self._data.get_str(f"{self._role}_given_name")
        self._data_dict["middleinitials"] = self._data.get_str(f"{self._role}_middle_name")
        self._data_dict["suffix"] = self._data.get_str(f"{self._role}_suffix")
        self._data_dict["prefix"] = self._data.get_str(f"{self._role}_prefix")

    def _parse_qualification(self):
        self._data_dict["qualification_code"] = self._data.get_str(f"{self._role}_degree")

    def build(self):
        # parse identifiers
        self._parse_identifiers()

        # update identifier system
        self._update_identifier()

        # parse name
        self._parse_name()

        # parse address
        self._parse_qualification()

        # check data_dict is valid
        self._is_valid()

        return self._data_dict
