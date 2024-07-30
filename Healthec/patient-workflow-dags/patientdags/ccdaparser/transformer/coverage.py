from benedict import benedict

from patientdags.ccdaparser.transformer.base import Base
from patientdags.ccdaparser.utils import parse_date_time


class Coverage(Base):
    def __init__(
        self,
        data: benedict,
        references: dict = {},
        metadata: dict = {},
        beneficiary_id: str = "",
        insurer_id: str = "",
        insurance_plan_id: str = "",
    ) -> None:
        super().__init__(data, "Coverage", references, metadata)
        self._type = type
        self._transformer.load_template("coverage.j2")
        self._beneficiary_id = beneficiary_id
        self._insurer_id = insurer_id
        self._insurance_plan_id = insurance_plan_id

    def _parse_period(self):
        self.data_dict["period_start_date"] = parse_date_time(self._data.get_str("period.start"))
        self.data_dict["period_end_date"] = parse_date_time(self._data.get_str("period.end"))

    def _parse_beneficiary_reference(self):
        if self._beneficiary_id:
            self.data_dict["beneficiary_patient_id"] = self._get_patient_reference()
        else:
            self.data_dict["beneficiary_patient_id"] = self._get_patient_reference()

    def _parse_insurer_reference(self):
        self.data_dict["insurer_organization_id"] = self._insurer_id

    # TODO: Add insurance plan id in coverage
    # def _parse_insurance_plan_reference(self):
    #     self.data_dict["insurance_plan_id"] = self._insurance_plan_id

    def build(self):
        # internal id
        self._set_internal_id()
        # identifier
        self._parse_identifier()
        # type
        self._parse_type()
        # period
        self._parse_period()
        # beneficiary reference
        self._parse_beneficiary_reference()
        # insurer reference
        self._parse_insurer_reference()
        return self.data_dict
