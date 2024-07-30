from benedict import benedict
from patientdags.hl7parser.enum import ResourceType
from patientdags.hl7parser.transformer.base import Base
from patientdags.hl7parser.utils import (
    parse_date_time,
)
from patientdags.hl7parser.constants import (
    PROCEDURE_PRACTITIONER_TYPE,
)


class Procedure(Base):
    def __init__(
        self,
        data: benedict,
        resource_type: str = ResourceType.Procedure.value,
        references: dict = {},
        metadata: dict = {},
    ) -> None:
        super().__init__(data, resource_type, references, metadata)
        self._transformer.load_template("procedure.j2")

    def _parse_identifiers(self):
        self._data_dict["source_id"] = self._data.get_str("procedure_identifier")

    def _construct_practitioner_id(self, practitioner_type: str, id: str) -> dict:
        return {
            "function_code": PROCEDURE_PRACTITIONER_TYPE.get(practitioner_type, {}).get("code"),
            "function_display": PROCEDURE_PRACTITIONER_TYPE.get(practitioner_type, {}).get("display"),
            "practitioner_id": id,
        }

    def _parse_practitioners(self):
        practitioners = []
        if self._references.get("anesthesiologist_reference", ""):
            practitioners.append(
                self._construct_practitioner_id(
                    "anesthesiologist", self._references.get("anesthesiologist_reference")
                )
            )
        if self._references.get("surgeon_reference", ""):
            practitioners.append(
                self._construct_practitioner_id("surgeon", self._references.get("surgeon_reference"))
            )
        if self._references.get("procedure_practitioner_reference", ""):
            practitioners.append(
                self._construct_practitioner_id(
                    "procedure_practitioner", self._references.get("procedure_practitioner_reference")
                )
            )
        return practitioners

    def _parse_codes(self):
        self._data_dict["code"] = self._data.get_str("procedure_code_identifier")
        self._data_dict["code_text"] = self._data.get_str("procedure_description")
        self._data_dict["code_system"] = self._data.get_str("procedure_coding_method")

    def _parse_performed_date(self):
        self._data_dict["performed_date_time"] = parse_date_time(self._data.get_str("procedure_date_time"))

    def _parse_references(self) -> None:
        self._is_valid()
        if self._data_dict:
            get_enc_ref = self._get_reference(key="encounter_reference")
            if get_enc_ref:
                self._data_dict["encounter_id"] = get_enc_ref
            get_pat_ref = self._get_reference(key="patient_reference")
            if get_pat_ref:
                self._data_dict["patient_id"] = get_pat_ref
            get_con_ref = self._get_reference(key="condition_reference", return_type=list, temp_key="condition_id")
            if get_con_ref:
                self._data_dict["reason_references"] = get_con_ref
            self._data_dict["performer"] = self._parse_practitioners()

    def build(self):
        # parse identifiers
        self._parse_identifiers()

        # update identifier system
        self._update_identifier()

        # parse codes
        self._parse_codes()

        # parse period
        self._parse_performed_date()

        # parse references
        self._parse_references()

        return self._data_dict
