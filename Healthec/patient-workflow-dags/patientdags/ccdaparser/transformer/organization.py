from benedict import benedict

from patientdags.ccdaparser.transformer.base import Base


class Organization(Base):
    def __init__(self, data: benedict, type: str = "prov", references: dict = {}, metadata: dict = {}) -> None:
        super().__init__(data, "Organization", references, metadata)
        self._type = type
        self._transformer.load_template("organization.j2")

    def _parse_type(self):
        if self._type == "prov":
            self.data_dict["type_code"] = self._type
            self.data_dict["type_display"] = "Healthcare Provider"
        elif self._type == "pay":
            self.data_dict["type_code"] = self._type
            self.data_dict["type_display"] = "Payer"
        else:
            self.data_dict["type_code"] = "other"
            self.data_dict["type_display"] = "Other"

    def build(self):
        # internal id
        self._set_internal_id()
        # identifier
        self._parse_identifier()
        # name
        self._parse_name()
        # type
        self._parse_type()
        # address
        self._parse_address()
        # phone work
        self._parse_phone_work()
        return self.data_dict
