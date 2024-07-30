from benedict import benedict
from patientdags.hl7parser.enum import ResourceType
from patientdags.hl7parser.transformer.base import Base
from patientdags.hl7parser.utils import (
    parse_date_time,
)
from patientdags.hl7parser.constants import ALLERGY_TYPE


class AllergyIntolerance(Base):
    def __init__(
        self,
        data: benedict,
        resource_type: str = ResourceType.AllergyIntolerance.value,
        references: dict = {},
        metadata: dict = {},
    ) -> None:
        super().__init__(data, resource_type, references, metadata)
        self._transformer.load_template("allergy_intolerance.j2")

    def _parse_type(self):
        self._data_dict["type"] = "allergy"

    def _parse_category(self):
        get_allery_type = self._data.get_str("allergy_type_code_identifier")
        if get_allery_type and get_allery_type.lower() not in ALLERGY_TYPE:
            allergy_type_lower, allergy_type_upper = get_allery_type.lower(), get_allery_type.upper()
            if "drug" in allergy_type_lower or allergy_type_upper == "DA":
                self._data_dict["category"] = "medication"
            elif "food" in allergy_type_lower or allergy_type_upper == "FA":
                self._data_dict["category"] = "food"
            elif "environment" in allergy_type_lower or allergy_type_upper == "EA":
                self._data_dict["category"] = "environment"
            else:
                self._data_dict["category"] = "biologic"

    def _parse_code(self):
        self._data_dict["code"] = self._data.get_str("allergy_code_identifier")
        self._data_dict["code_system"] = self._data.get_str("allergy_code_system")
        self._data_dict["code_text"] = self._data.get_str("allergy_code_text")

    def _parse_manifestation(self):
        self._data_dict["manifestation_text"] = self._data.get_str("allergy_reaction_code")

    def _parse_onset_date(self):
        self._data_dict["onset_date_time"] = parse_date_time(self._data.get_str("identification_date"))

    def _parse_criticality(self):
        get_critical = self._data.get_str("allergy_severity_code_identifier").upper()
        critical = "unable-to-assess"
        if get_critical:
            if get_critical[0] == "S":
                critical = "high"
            elif get_critical[0] == "M":
                critical = "low"
        self._data_dict["criticality"] = critical

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
        # parse type
        self._parse_type()

        # parse category
        self._parse_category()

        # parse codes
        self._parse_code()

        # parse manifestation
        self._parse_manifestation()

        # parse period
        self._parse_onset_date()

        # parse criticality
        self._parse_criticality()

        # parse references
        self._parse_references()

        return self._data_dict
