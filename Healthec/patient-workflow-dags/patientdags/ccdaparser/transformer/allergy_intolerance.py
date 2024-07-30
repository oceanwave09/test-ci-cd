from benedict import benedict

from patientdags.ccdaparser.transformer.base import Base
from patientdags.ccdaparser.utils import parse_date_time


class AllergyIntolerance(Base):
    ALLERGY_CATEGORY = {
        "food": ["food"],
        "medication": ["medication", "drug", "substance"],
        "environment": ["environment"],
        "biologic": ["biologic"],
    }

    def __init__(self, data: benedict, references: dict = {}, metadata: dict = {}, practitioner_id: str = "") -> None:
        super().__init__(data, "AllergyIntolerance", references, metadata)
        self._type = type
        self._transformer.load_template("allergy_intolerance.j2")
        self._practitioner_id = practitioner_id

    def _get_category(self, value):
        if not value:
            return ""
        for key, entries in self.ALLERGY_CATEGORY.items():
            for entry in entries:
                if entry in value:
                    return key
        return ""

    def _parse_category(self):
        category_text = (
            self._data.get_str("category.display")
            if self._data.get_str("category.display")
            else self._data.get_str("category.text")
        )
        category = self._get_category(category_text)
        if category:
            self.data_dict["category"] = category

    def _parse_onset(self):
        onset_date_time = parse_date_time(self._data.get_str("onset_date_time"))
        if onset_date_time:
            self.data_dict["onset_date_time"] = onset_date_time
        elif self._data.get_str("onset_period.start"):
            self.data_dict["onset_period_start"] = parse_date_time(self._data.get_str("onset_period.start"))
            self.data_dict["onset_period_end"] = parse_date_time(self._data.get_str("onset_period.end"))

    def _parse_manifestation(self):
        self.data_dict["manifestation_code"] = self._data.get_str("reaction[0].manifestation.code")
        self.data_dict["manifestation_system"] = self._data.get_str("reaction[0].manifestation.system")
        self.data_dict["manifestation_display"] = self._data.get_str("reaction[0].manifestation.display")
        self.data_dict["manifestation_text"] = (
            self._data.get_str("reaction[0].manifestation.display")
            if self._data.get_str("reaction[0].manifestation.display")
            else self._data.get_str("reaction[0].manifestation.text")
        )

    def _parse_severity(self):
        severity = ""
        severity_code = str(self._data.get_str("reaction[0].severity.code")).lower()
        severity_text = str(
            self._data.get_str("reaction[0].severity.display")
            if self._data.get_str("reaction[0].severity.display")
            else self._data.get_str("reaction[0].severity.text")
        ).lower()
        if "mild" in severity_code or "mild" in severity_text:
            severity = "mild"
        elif "moderate" in severity_code or "moderate" in severity_text:
            severity = "moderate"
        elif "severe" in severity_code or "severe" in severity_text:
            severity = "severe"
        self.data_dict["severity_code"] = severity

    def _parse_criticality(self):
        criticality = ""
        criticality_code = str(self._data.get_str("criticality.code")).lower()
        criticality_text = str(
            self._data.get_str("criticality.display")
            if self._data.get_str("criticality.display")
            else self._data.get_str("criticality.text")
        ).lower()
        if "low" in criticality_code or "low" in criticality_text:
            criticality = "low"
        elif "high" in criticality_code or "high" in criticality_text:
            criticality = "high"
        elif "unable" in criticality_code or "unable" in criticality_text:
            criticality = "unable-to-assess"
        self.data_dict["criticality"] = criticality

    def _parse_clinical_status(self):
        clinical_status = str(self._data.get_str("clinical_status")).lower()
        if clinical_status in ["active", "inactive", "resolved"]:
            self.data_dict["clinical_status"] = clinical_status

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
        self._parse_code()
        # onset
        self._parse_onset()
        # criticality
        self._parse_criticality()
        # clinical status
        self._parse_clinical_status()
        # manifestation
        self._parse_manifestation()
        # severity
        self._parse_severity()
        # patient reference
        self._parse_patient_reference()
        # practitioner reference
        self._parse_practitioner_reference()
        return self.data_dict
