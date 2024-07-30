from benedict import benedict

from patientdags.ccdaparser.constants import (
    UNKNOWN_ETHNICITY_CODE,
    UNKNOWN_ETHNICITY_DISPLAY,
    UNKNOWN_RACE_CODE,
    UNKNOWN_RACE_DISPLAY,
)
from patientdags.ccdaparser.transformer.base import Base
from patientdags.ccdaparser.utils import parse_boolean, parse_date, parse_gender


class Patient(Base):
    def __init__(self, data: benedict, references: dict = {}, metadata: dict = {}) -> None:
        super().__init__(data, "Patient", references, metadata)
        self._type = type
        self._transformer.load_template("patient.j2")

    def _parse_birth_date(self):
        self.data_dict["dob"] = parse_date(self._data.get_str("birth_date"))

    def _parse_gender(self):
        self.data_dict["gender"] = parse_gender(self._data.get_str("gender.code"))

    def _parse_marital_status(self):
        self.data_dict["marital_status_code"] = self._data.get_str("marital_status.code")
        self.data_dict["marital_status_system"] = self._data.get_str("marital_status.system")
        self.data_dict["marital_status_display"] = self._data.get_str("marital_status.display")
        self.data_dict["marital_status_text"] = (
            self._data.get_str("marital_status.display")
            if self._data.get_str("marital_status.display")
            else self._data.get_str("marital_status.text")
        )

    def _parse_race(self):
        if self._data.get_str("race.code") or self._data.get_str("race.text"):
            self.data_dict["race_code"] = self._data.get_str("race.code")
            self.data_dict["race_display"] = self._data.get_str("race.display")
            self.data_dict["race"] = (
                self._data.get_str("race.display")
                if self._data.get_str("race.display")
                else self._data.get_str("race.text")
            )
        else:
            self.data_dict["race_code"] = UNKNOWN_RACE_CODE
            self.data_dict["race_display"] = UNKNOWN_RACE_DISPLAY
            self.data_dict["race"] = UNKNOWN_RACE_DISPLAY

    def _parse_ethnicity(self):
        if self._data.get_str("ethnicity.code") or self._data.get_str("ethnicity.text"):
            self.data_dict["ethnicity_code"] = self._data.get_str("ethnicity.code")
            self.data_dict["ethnicity_display"] = self._data.get_str("ethnicity.display")
            self.data_dict["ethnicity"] = (
                self._data.get_str("ethnicity.display")
                if self._data.get_str("ethnicity.display")
                else self._data.get_str("ethnicity.text")
            )
        else:
            self.data_dict["ethnicity_code"] = UNKNOWN_ETHNICITY_CODE
            self.data_dict["ethnicity_display"] = UNKNOWN_ETHNICITY_DISPLAY
            self.data_dict["ethnicity"] = UNKNOWN_ETHNICITY_DISPLAY

    def _parse_preferred_language(self):
        if self._data.get_str("language.spoken[0].code") or self._data.get_str("language.spoken[0].text"):
            self.data_dict["preferred_language_code"] = self._data.get_str("language.spoken[0].code")
            self.data_dict["preferred_language_system"] = self._data.get_str("language.spoken[0].system")
            self.data_dict["preferred_language_display"] = self._data.get_str("language.spoken[0].display")
            self.data_dict["preferred_language_text"] = (
                self._data.get_str("language.spoken[0].display")
                if self._data.get_str("language.spoken[0].display")
                else self._data.get_str("language.spoken[0].text")
            )
        elif self._data.get_str("language.written[0].code") or self._data.get_str("language.written[0].text"):
            self.data_dict["preferred_language_code"] = self._data.get_str("language.written[0].code")
            self.data_dict["preferred_language_system"] = self._data.get_str("language.written[0].system")
            self.data_dict["preferred_language_display"] = self._data.get_str("language.written[0].display")
            self.data_dict["preferred_language_text"] = (
                self._data.get_str("language.written[0].display")
                if self._data.get_str("language.written[0].display")
                else self._data.get_str("language.written[0].text")
            )

    def _parse_deceased_flag(self):
        self.data_dict["deceased_indication"] = parse_boolean(self._data.get("deceased"))

    def build(self):
        # internal id
        self._set_internal_id()
        # identifier
        self._parse_identifier()
        # name
        self._parse_human_name()
        # address
        self._parse_address()
        # birth date
        self._parse_birth_date()
        # gender
        self._parse_gender()
        # marital status
        self._parse_marital_status()
        # phone work
        self._parse_phone_work()
        # phone mobile
        self._parse_phone_mobile()
        # phone home
        self._parse_phone_home()
        # email
        self._parse_email()
        # race
        self._parse_race()
        # ethnicity
        self._parse_ethnicity()
        # preferred language
        self._parse_preferred_language()
        # deceased
        self._parse_deceased_flag()
        # managing organization reference
        self._parse_organization_reference()
        return self.data_dict
