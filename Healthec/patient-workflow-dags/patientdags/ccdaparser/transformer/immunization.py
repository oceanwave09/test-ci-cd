from collections import OrderedDict

from benedict import benedict

from patientdags.ccdaparser.transformer.base import Base
from patientdags.ccdaparser.utils import parse_date_time, to_float


class Immunization(Base):
    IMMUNIZATION_STATUS = ["completed", "entered-in-error", "not-done"]

    def __init__(
        self,
        data: benedict,
        references: dict = {},
        metadata: dict = {},
        practitioner_id: str = "",
        encounter_id: str = "",
    ) -> None:
        super().__init__(data, "Immunization", references, metadata)
        self._type = type
        self._transformer.load_template("immunization.j2")
        self._practitioner_id = practitioner_id
        self._encounter_id = encounter_id

    def _parse_vaccine_code(self, prefer_code_systems: list = []):
        code_dict = OrderedDict()
        if self._data.get_str("vaccine_code.code"):
            code_entry = {
                "vaccine_code": self._data.get_str("vaccine_code.code"),
                "vaccine_code_system": self._data.get_str("vaccine_code.system"),
                "vaccine_code_display": self._data.get_str("vaccine_code.display"),
                "vaccine_code_text": (
                    self._data.get_str("vaccine_code.display")
                    if self._data.get_str("vaccine_code.display")
                    else self._data.get_str("vaccine_code.text")
                ),
            }
            key = code_entry.get("code_system") if code_entry.get("code_system") else code_entry.get("code")
            code_dict[key] = code_entry
        for entry in self._data.get_list("vaccine_code.translation"):
            if entry.get_str("code"):
                code_entry = {
                    "vaccine_code": entry.get_str("code"),
                    "vaccine_code_system": entry.get_str("system"),
                    "vaccine_code_display": entry.get_str("display"),
                    "vaccine_code_text": (
                        entry.get_str("display") if entry.get_str("display") else entry.get_str("text")
                    ),
                }
                key = code_entry.get("code_system") if code_entry.get("code_system") else code_entry.get("code")
                code_dict[key] = code_entry
        if not code_dict:
            return
        if not prefer_code_systems:
            self.data_dict.update(code_dict.popitem(last=False)[1])
            return
        keys = code_dict.keys()
        code_added = False
        for code_system in prefer_code_systems:
            for key in keys:
                if code_system in key:
                    self.data_dict.update(code_dict.get(key))
                    code_added = True
                    break
            if code_added:
                break
        if not code_added:
            self.data_dict.update(code_dict.popitem(last=False)[1])
        return

    def _parse_occurrence(self):
        occurrence_date_time = parse_date_time(self._data.get_str("occurrence_date_time"))
        if occurrence_date_time:
            self.data_dict["occurrence_date_time"] = occurrence_date_time
        else:
            self.data_dict["occurrence_date_time"] = parse_date_time(self._data.get_str("occurrence_period.start"))

    def _parse_status(self):
        status = str(self._data.get_str("status")).lower()
        if status in self.IMMUNIZATION_STATUS:
            self.data_dict["status"] = status

    def _parse_lot_number(self):
        self.data_dict["lotNumber"] = self._data.get_str("lot_number")

    def _parse_dose_quantity(self):
        dose_quantity = to_float(self._data.get_str("dose_quantity.value"))
        if dose_quantity:
            self.data_dict["dose_quantity_value"] = dose_quantity
            self.data_dict["dose_quantity_unit"] = self._data.get_str("dose_quantity.unit")
        self.data_dict["dose_number"] = self._data.get_str("dose_number")

    def _parse_practitioner_reference(self):
        self.data_dict["practitioner_id"] = self._practitioner_id

    def _parse_encounter_reference(self):
        self.data_dict["encounter_id"] = self._encounter_id

    def build(self):
        # internal id
        self._set_internal_id()
        # identifier
        self._parse_identifier()
        # type
        self._parse_type()
        # vaccine code
        self._parse_vaccine_code()
        # occurrence
        self._parse_occurrence()
        # status
        self._parse_status()
        # lot number
        self._parse_lot_number()
        # dose quantity
        self._parse_dose_quantity()
        # site
        self._parse_site()
        # route
        self._parse_route()
        # reason code
        self._parse_reason_code()
        # TODO: could not find proper mapping for reaction code in FHIR Immunization
        # TODO: could not find proper mapping for dose number (repeat number) in FHIR Immunization
        # patient reference
        self._parse_patient_reference()
        # practitioner reference
        self._parse_practitioner_reference()
        # encounter reference
        self._parse_encounter_reference()
        return self.data_dict
