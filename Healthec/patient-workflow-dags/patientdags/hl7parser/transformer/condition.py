from benedict import benedict
from patientdags.hl7parser.enum import ResourceType
from patientdags.hl7parser.transformer.base import Base
from patientdags.hl7parser.utils import (
    parse_date_time,
)


class Condition(Base):
    def __init__(
        self,
        data: benedict,
        resource_type: str = ResourceType.Condition.value,
        references: dict = {},
        metadata: dict = {},
    ) -> None:
        super().__init__(data, resource_type, references, metadata)
        self._transformer.load_template("condition.j2")

    def _parse_identifiers(self):
        self._data_dict["source_id"] = self._data.get_str("diagnosis_identifier")

    def _parse_condition_codes(self):
        if self._data.get_str("diagnosis_code_identifier"):
            self._data_dict["code"] = self._data.get_str("diagnosis_code_identifier")
            self._data_dict["code_text"] = self._data.get_str("diagnosis_description")
            self._data_dict["code_system"] = self._data.get_str("diagnosis_coding_method")
        elif self._data.get_str("associated_diagnosis_code_identifier"):
            # Procedure
            self._data_dict["code"] = self._data.get_str("associated_diagnosis_code_identifier")
            self._data_dict["code_text"] = self._data.get_str("associated_diagnosis_code_text")
            self._data_dict["code_system"] = self._data.get_str("associated_diagnosis_code_system")

    def _parse_onset_and_recorded_date(self):
        self._data_dict["onset_date_time"] = parse_date_time(self._data.get_str("diagnosis_date_time"))
        self._data_dict["recorded_date_time"] = parse_date_time(self._data.get_str("attestation_date_time"))

    def _parse_references(self) -> None:
        self._is_valid()
        if self._data_dict:
            get_enc_ref = self._get_reference(key="encounter_reference")
            if get_enc_ref:
                self._data_dict["encounter_id"] = get_enc_ref
            get_pat_ref = self._get_reference(key="patient_reference")
            if get_pat_ref:
                self._data_dict["patient_id"] = get_pat_ref

    def build(self):
        # parse identifiers
        self._parse_identifiers()

        # update identifier system
        self._update_identifier()

        # parse codes
        self._parse_condition_codes()

        # parse period
        self._parse_onset_and_recorded_date()

        # parse references
        self._parse_references()

        return self._data_dict
