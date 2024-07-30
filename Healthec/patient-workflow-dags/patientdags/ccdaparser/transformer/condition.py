from benedict import benedict

from patientdags.ccdaparser.transformer.base import Base
from patientdags.ccdaparser.utils import parse_date_time


class Condition(Base):
    CONDITION_CLINICAL_STATUS = ["active", "inactive", "recurrence", "relapse", "remission", "resolved"]
    CONDITION_VERIFICATION_STATUS = [
        "unconfirmed",
        "provisional",
        "differential",
        "confirmed",
        "refuted",
        "entered-in-error",
    ]
    PREFER_CODE_PRIORITIES = ["2.16.840.1.113883.6.90", "2.16.840.1.113883.6.103", "2.16.840.1.113883.6.96"]

    def __init__(
        self,
        data: benedict,
        references: dict = {},
        metadata: dict = {},
        encounter_id: str = "",
        practitioner_id: str = "",
    ) -> None:
        super().__init__(data, "Condition", references, metadata)
        self._type = type
        self._transformer.load_template("condition.j2")
        self._encounter_id = encounter_id
        self._practitioner_id = practitioner_id

    def _parse_onset(self):
        self.data_dict["onset_date_time"] = parse_date_time(self._data.get_str("onset_date_time"))

    def _parse_abatement(self):
        self.data_dict["abatement_date_time"] = parse_date_time(self._data.get_str("abatement_date_time"))

    def _parse_clinical_status(self):
        clinical_status = ""
        clinical_status_code = str(self._data.get_str("clinical_status.code")).lower()
        clinical_status_text = str(
            self._data.get_str("clinical_status.display")
            if self._data.get_str("clinical_status.display")
            else self._data.get_str("clinical_status.text")
        ).lower()
        if clinical_status_code in self.CONDITION_CLINICAL_STATUS:
            clinical_status = clinical_status_code
        elif clinical_status_text in self.CONDITION_CLINICAL_STATUS:
            clinical_status = clinical_status_text
        self.data_dict["clinical_status"] = clinical_status

    def _parse_verification_status(self):
        verification_status = str(self._data.get_str("verification_status")).lower()
        if verification_status == "completed":
            verification_status = "confirmed"
        if verification_status in self.CONDITION_VERIFICATION_STATUS:
            self.data_dict["verification_status"] = verification_status

    def _parse_encounter_reference(self):
        self.data_dict["encounter_id"] = self._encounter_id

    def _parse_practitioner_reference(self):
        self.data_dict["practitioner_id"] = self._practitioner_id

    def build(self):
        # internal id
        self._set_internal_id()
        # identifier
        self._parse_identifier()
        # category
        self._parse_category()
        # code
        self._parse_code(self.PREFER_CODE_PRIORITIES)
        # onset
        self._parse_onset()
        # abatement
        self._parse_abatement()
        # clinical status
        self._parse_clinical_status()
        # verification status
        self._parse_verification_status()
        # patient reference
        self._parse_patient_reference()
        # encounter reference
        self._parse_encounter_reference()
        # practitioner reference
        self._parse_practitioner_reference()
        return self.data_dict
