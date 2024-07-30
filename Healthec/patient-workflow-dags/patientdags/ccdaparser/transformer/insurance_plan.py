from benedict import benedict

from patientdags.ccdaparser.transformer.base import Base
from patientdags.ccdaparser.utils import parse_date_time


class InsurancePlan(Base):
    def __init__(self, data: benedict, references: dict = {}, metadata: dict = {}) -> None:
        super().__init__(data, "InsurancePlan", references, metadata)
        self._type = type
        self._transformer.load_template("insurance_plan.j2")

    def _parse_period(self):
        self.data_dict["period_start_date"] = parse_date_time(self._data.get_str("effective_period.start"))
        self.data_dict["period_end_date"] = parse_date_time(self._data.get_str("effective_period.end"))

    def build(self):
        # internal id
        self._set_internal_id()
        # identifier
        self._parse_identifier()
        # name
        self._parse_name()
        # period
        self._parse_period()
        return self.data_dict
