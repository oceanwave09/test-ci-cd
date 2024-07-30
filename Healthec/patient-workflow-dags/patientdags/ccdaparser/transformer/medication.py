from benedict import benedict

from patientdags.ccdaparser.transformer.base import Base


class Medication(Base):
    def __init__(self, data: benedict, references: dict = {}, metadata: dict = {}) -> None:
        super().__init__(data, "Medication", references, metadata)
        self._type = type
        self._transformer.load_template("medication.j2")

    def build(self):
        # internal id
        self._set_internal_id()
        # identifier
        self._parse_identifier()
        # code
        self._parse_code()
        return self.data_dict
