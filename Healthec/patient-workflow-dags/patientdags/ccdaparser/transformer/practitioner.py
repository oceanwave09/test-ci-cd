from benedict import benedict

from patientdags.ccdaparser.transformer.base import Base


class Practitioner(Base):
    def __init__(self, data: benedict, references: dict = {}, metadata: dict = {}) -> None:
        super().__init__(data, "Practitioner", references, metadata)
        self._type = type
        self._transformer.load_template("practitioner.j2")

    def _clean_identifier(self):
        if self.data_dict.get("npi") and self.data_dict.get("source_id"):
            self.data_dict.pop("source_id", "")
            self.data_dict.pop("source_system", "")

    def build(self):
        # internal id
        self._set_internal_id()
        # identifier
        self._parse_identifier()
        # clean up identifier
        self._clean_identifier()
        # name
        self._parse_human_name()
        # address
        self._parse_address()
        # phone work
        self._parse_phone_work()
        return self.data_dict
