from benedict import benedict

from patientdags.ccdaparser.transformer.base import Base
from patientdags.ccdaparser.utils import parse_date_time


class Encounter(Base):
    ENCOUNTER_STATUS = [
        "planned",
        "arrived",
        "triaged",
        "in-progress",
        "onleave",
        "finished",
        "cancelled",
        "entered-in-error",
        "unknown",
    ]

    def __init__(self, data: benedict, references: dict = {}, metadata: dict = {}, practitioner_id: str = "") -> None:
        super().__init__(data, "Encounter", references, metadata)
        self._type = type
        self._transformer.load_template("encounter.j2")
        self._practitioner_id = practitioner_id

    def _parse_discharge_disposition(self):
        discharge_disposition = self._data.get("discharge_disposition")
        if discharge_disposition and isinstance(discharge_disposition, list):
            self.data_dict["discharge_disposition_code"] = self._data.get_str("discharge_disposition[0].code")
            self.data_dict["discharge_disposition_system"] = self._data.get_str("discharge_disposition[0].system")
            self.data_dict["discharge_disposition_display"] = self._data.get_str("discharge_disposition[0].display")
            self.data_dict["discharge_disposition_text"] = (
                self._data.get_str("discharge_disposition[0].display")
                if self._data.get_str("discharge_disposition[0].display")
                else self._data.get_str("discharge_disposition[0].text")
            )
        elif discharge_disposition and isinstance(discharge_disposition, dict):
            self.data_dict["discharge_disposition_code"] = self._data.get_str("discharge_disposition.code")
            self.data_dict["discharge_disposition_system"] = self._data.get_str("discharge_disposition.system")
            self.data_dict["discharge_disposition_display"] = self._data.get_str("discharge_disposition.display")
            self.data_dict["discharge_disposition_text"] = (
                self._data.get_str("discharge_disposition.display")
                if self._data.get_str("discharge_disposition.display")
                else self._data.get_str("discharge_disposition.text")
            )

    def _parse_period(self):
        self.data_dict["period_start_date"] = parse_date_time(self._data.get_str("effective_period.start"))
        self.data_dict["period_end_date"] = parse_date_time(self._data.get_str("effective_period.end"))

    def _parse_reason_code(self, code_system: str = "icd"):
        added_prefer_code = False
        added_code = False
        if self._data.get_str("reasons[0].code"):
            self.data_dict["reason_code"] = self._data.get_str("reasons[0].code")
            self.data_dict["reason_system"] = self._data.get_str("reasons[0].system")
            self.data_dict["reason_display"] = self._data.get_str("reasons[0].display")
            self.data_dict["reason_text"] = (
                self._data.get_str("reasons[0].display")
                if self._data.get_str("reasons[0].display")
                else self._data.get_str("reasons[0].text")
            )
            if code_system and code_system in self._data.get_str("reasons[0].system"):
                added_prefer_code = True
            added_code = True
        if not added_prefer_code and len(self._data.get_list("reasons[0].translation")) > 0:
            for entry in self._data.get_list("reasons[0].translation"):
                if code_system and code_system in entry.get_str("system_name").lower():
                    self.data_dict["reason_code"] = entry.get_str("code")
                    self.data_dict["reason_system"] = entry.get_str("system")
                    self.data_dict["reason_display"] = entry.get_str("display")
                    self.data_dict["reason_text"] = (
                        entry.get_str("display") if entry.get_str("display") else entry.get_str("text")
                    )
                    added_prefer_code = True
                    added_code = True
            if not added_code:
                self.data_dict["reason_code"] = self._data.get_str("reasons[0].translation[0].code")
                self.data_dict["reason_system"] = self._data.get_str("reasons[0].translation[0].system")
                self.data_dict["reason_display"] = self._data.get_str("reasons[0].translation[0].display")
                self.data_dict["reason_text"] = (
                    self._data.get_str("reasons[0].translation[0].display")
                    if self._data.get_str("reasons[0].translation[0].display")
                    else self._data.get_str("reasons[0].translation[0].text")
                )

    def _parse_status(self):
        status = str(self._data.get_str("status")).lower()
        if status in self.ENCOUNTER_STATUS:
            self.data_dict["status"] = status

    def _parse_practitioner_reference(self):
        self.data_dict["practitioner_id"] = self._practitioner_id

    def build(self):
        # internal id
        self._set_internal_id()
        # identifier
        self._parse_identifier()
        # type
        self._parse_type()
        # discharge disposition
        self._parse_discharge_disposition()
        # period
        self._parse_period()
        # reason code
        self._parse_reason_code()
        # status
        self._parse_status()
        # patient reference
        self._parse_patient_reference()
        # practitioner reference
        self._parse_practitioner_reference()
        return self.data_dict
