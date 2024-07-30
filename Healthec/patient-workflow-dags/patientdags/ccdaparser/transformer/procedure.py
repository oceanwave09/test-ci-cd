from benedict import benedict

from patientdags.ccdaparser.transformer.base import Base
from patientdags.ccdaparser.utils import parse_date_time


class Procedure(Base):
    PROCEDURE_STATUS = [
        "preparation",
        "in-progress",
        "not-done",
        "on-hold",
        "stopped",
        "completed",
        "entered-in-error",
        "unknown",
    ]

    def __init__(
        self,
        data: benedict,
        references: dict = {},
        metadata: dict = {},
        practitioner_id: str = "",
        encounter_id: str = "",
    ) -> None:
        super().__init__(data, "Procedure", references, metadata)
        self._type = type
        self._transformer.load_template("procedure.j2")
        self._practitioner_id = practitioner_id
        self._encounter_id = encounter_id

    def _parse_performed(self):
        performed_date_time = parse_date_time(self._data.get_str("performed_date_time"))
        if performed_date_time:
            self.data_dict["performed_date_time"] = performed_date_time
        else:
            self.data_dict["performed_period_start"] = parse_date_time(self._data.get_str("performed_period.start"))
            self.data_dict["performed_period_end"] = parse_date_time(self._data.get_str("performed_period.end"))

    def _parse_status(self):
        status = str(self._data.get_str("status")).lower()
        if status in self.PROCEDURE_STATUS:
            self.data_dict["status"] = status

    def _parse_body_site(self):
        body_site = self._data.get("body_site")
        if body_site and isinstance(body_site, list):
            self.data_dict["body_site_code"] = self._data.get_str("body_site[0].code")
            self.data_dict["body_site_system"] = self._data.get_str("body_site[0].system")
            self.data_dict["body_site_display"] = self._data.get_str("body_site[0].display")
            self.data_dict["body_site_text"] = (
                self._data.get_str("body_site[0].display")
                if self._data.get_str("body_site[0].display")
                else self._data.get_str("body_site[0].text")
            )
        elif body_site and isinstance(body_site, dict):
            self.data_dict["body_site_code"] = self._data.get_str("body_site.code")
            self.data_dict["body_site_system"] = self._data.get_str("body_site.system")
            self.data_dict["body_site_display"] = self._data.get_str("body_site.display")
            self.data_dict["body_site_text"] = (
                self._data.get_str("body_site.display")
                if self._data.get_str("body_site.display")
                else self._data.get_str("body_site.text")
            )

    def _parse_encounter_reference(self):
        self.data_dict["encounter_id"] = self._encounter_id

    def _parse_practitioner_reference(self):
        self.data_dict["practitioner_id"] = self._practitioner_id

    def build(self):
        # internal id
        self._set_internal_id()
        # identifier
        self._parse_identifier()
        # code
        self._parse_code()
        # performed
        self._parse_performed()
        # reason code
        self._parse_reason_code()
        # status
        self._parse_status()
        # body site
        self._parse_body_site()
        # patient reference
        self._parse_patient_reference()
        # encounter reference
        self._parse_encounter_reference()
        # practitioner reference
        self._parse_practitioner_reference()
        return self.data_dict
