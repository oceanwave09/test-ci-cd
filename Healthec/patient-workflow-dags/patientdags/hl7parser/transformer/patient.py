from benedict import benedict

from patientdags.hl7parser.constants import (
    UNKNOWN_ETHNICITY_CODE,
    UNKNOWN_ETHNICITY_DISPLAY,
    UNKNOWN_RACE_CODE,
    UNKNOWN_RACE_DISPLAY,
)
from patientdags.hl7parser.enum import ResourceType
from patientdags.hl7parser.transformer.base import Base
from patientdags.hl7parser.utils import (  # get_type_code,
    parse_date,
    parse_date_time,
    update_death_indicator,
    update_gender,
)


class PatientPID(Base):
    def __init__(
        self,
        data: benedict,
        resource_type: str = ResourceType.Patient.value,
        references: dict = {},
        metadata: dict = {},
    ) -> None:
        super().__init__(data, resource_type, references, metadata)
        self._transformer.load_template("patient.j2")

    def _parse_contact_details(self) -> None:
        contacts = []
        for contact_data in self._data.get_list("NK1"):
            contacts.append(
                {
                    "relationship_code": contact_data.get("relationship_identifier"),
                    "relationship_system": contact_data.get("relationship_coding_system"),
                    "relationship_text": contact_data.get("relationship_text"),
                    "firstname": contact_data.get("given_name"),
                    "lastname": contact_data.get("family_name"),
                    "middleinitials": contact_data.get("middle_initials"),
                    "prefix": contact_data.get("preffix_name"),
                    "suffix": contact_data.get("suffix_name"),
                    "phone_home": contact_data.get("phone_number_home"),
                    "phone_mobile": contact_data.get("phone_number_mobile"),
                    "phone_work": contact_data.get("phone_number_business"),
                    "state": contact_data.get("state"),
                    "city": contact_data.get("city"),
                    "street_address_1": contact_data.get("street_address"),
                    "zip": contact_data.get("zip"),
                    "country": contact_data.get("country"),
                    "gender": update_gender(contact_data.get("administrative_sex")),
                    "period_start_date": contact_data.get("start_date"),
                    "period_end_date": contact_data.get("end_date"),
                }
            )
        self._data_dict["contacts"] = contacts

    def _parse_identifiers(self) -> None:
        if self._data.get_str("patient_identifier_id"):
            self._data_dict["patient_external_id"] = self._data.get_str("patient_identifier_id")
        if self._data.get_str("alternate_patient_id"):
            self._data_dict["patient_internal_id"] = self._data.get_str("alternate_patient_id")
        if self._data.get_str("patient_identifier_list_id"):
            self._data_dict["mrn"] = self._data.get_str("patient_identifier_list_id")
        self._data_dict["ssn"] = self._data.get_str("ssn_number_patient")
        self._data_dict["driver_no"] = self._data.get_str("patient_drivers_license_number")

    def _parse_name(self) -> None:
        self._data_dict["firstname"] = self._data.get_str("patient_given_name")
        self._data_dict["lastname"] = self._data.get_str("patient_family_name")
        self._data_dict["middleinitials"] = self._data.get_str("patient_middle_name")
        self._data_dict["suffix"] = self._data.get_str("patient_suffix_name")
        self._data_dict["prefix"] = self._data.get_str("patient_preffix_name")

    def _parse_dob(self) -> None:
        self._data_dict["dob"] = parse_date(self._data.get_str("date_time_of_birth"))

    def _parse_gender(self) -> None:
        self._data_dict["gender"] = update_gender(self._data.get_str("sex"))

    def _parse_address(self) -> None:
        self._data_dict["street_address_1"] = self._data.get_str("patient_street_address")
        self._data_dict["city"] = self._data.get_str("patient_city")
        self._data_dict["state"] = self._data.get_str("patient_state")
        self._data_dict["zip"] = self._data.get_str("patient_postal_code")
        self._data_dict["country"] = self._data.get_str("patient_country")
        self._data_dict["district_code"] = self._data.get_str("county_code")

    def _parse_phone(self) -> None:
        self._data_dict["phone_work"] = self._data.get_str("phone_number_business")
        self._data_dict["phone_home"] = self._data.get_str("phone_number_home")

    def _parse_language(self) -> None:
        self._data_dict["preferred_language_code"] = self._data.get_str("primary_language_identifier")
        self._data_dict["preferred_language_text"] = self._data.get_str("primary_language_text")
        self._data_dict["preferred_language_display"] = self._data.get_str("primary_language_text")
        self._data_dict["preferred_language_system"] = self._data.get_str("primary_language_coding_system")

    def _parse_marital_status(self) -> None:
        self._data_dict["marital_status_code"] = self._data.get_str("marital_status_identifier")
        self._data_dict["marital_status_system"] = self._data.get_str("marital_status_coding_system")
        self._data_dict["marital_status_display"] = self._data.get_str("marital_status_text")
        self._data_dict["marital_status_text"] = self._data.get_str("marital_status_text")

    def _parse_ethinicity(self) -> None:
        if self._data.get("patient_ethnic_group_identifier") or self._data.get_str("patient_ethnic_group_plan_text"):
            self._data_dict["ethnicity_code"] = self._data.get_str("patient_ethnic_group_identifier")
            self._data_dict["ethnicity_display"] = self._data.get_str("patient_ethnic_group_plan_text")
            self._data_dict["ethnicity"] = self._data.get_str("patient_ethnic_group_plan_text")
        else:
            self._data_dict["ethnicity_code"] = UNKNOWN_ETHNICITY_CODE
            self._data_dict["ethnicity_display"] = UNKNOWN_ETHNICITY_DISPLAY
            self._data_dict["ethnicity"] = UNKNOWN_ETHNICITY_DISPLAY

    def _parse_race(self) -> None:
        if self._data.get("patient_race_identifier") or self._data.get_str("patient_race_plan_text"):
            self._data_dict["race_code"] = self._data.get_str("patient_race_identifier")
            self._data_dict["race_display"] = self._data.get_str("patient_race_plan_text")
            self._data_dict["race"] = self._data.get_str("patient_race_plan_text")
        else:
            self._data_dict["race_code"] = UNKNOWN_RACE_CODE
            self._data_dict["race_display"] = UNKNOWN_RACE_DISPLAY
            self._data_dict["race"] = UNKNOWN_RACE_DISPLAY

    def _parse_deceased_date(self) -> None:
        self._data_dict["deceased_date_time"] = parse_date_time(self._data.get_str("patient_death_date_time"))
        self._data_dict["deceased_indication"] = update_death_indicator(self._data.get_str("patient_death_indicator"))

    def _parse_mothers_name(self) -> None:
        if (
            self._data.get_str("mothers_maiden_given_name")
            or self._data.get_str("mothers_maiden_middle_name")
            or self._data.get_str("mothers_maiden_family_name")
        ):
            get_mothers_name = " ".join(
                [
                    self._data.get_str("mothers_maiden_given_name"),
                    self._data.get_str("mothers_maiden_middle_name"),
                    self._data.get_str("mothers_maiden_family_name"),
                ]
            )
            self._data_dict["extensions"] = [
                {
                    "url": "http://healthec.com/extensions/patients/mothers-maiden-name",
                    "value": get_mothers_name,
                }
            ]

    def _parse_references(self) -> None:
        self._is_valid()
        if self._data_dict:
            self._data_dict["organization_id"] = self._metadata.get("child_org_id", "")
            get_pract_ref = self._get_reference(key="practitioner_reference", return_type=list)
            if get_pract_ref:
                self._data_dict["practitioners"] = get_pract_ref

    def build(self) -> dict:
        # parse identifier
        self._parse_identifiers()
        # update identifier system
        self._update_identifier()
        # parse name
        self._parse_name()
        # parse dob
        self._parse_dob()
        # parse gender
        self._parse_gender()
        # parse address
        self._parse_address()
        # parse phone
        self._parse_phone()
        # parse language
        self._parse_language()
        # parse matital status
        self._parse_marital_status()
        # parse ethinicity
        self._parse_ethinicity()
        # parse race
        self._parse_race()
        # parse mothers maiden name
        self._parse_mothers_name()
        # parse contact details
        self._parse_contact_details()
        # parse deceased details
        self._parse_deceased_date()
        # parse references
        self._parse_references()

        return self._data_dict


class PatientIN1(Base):
    def __init__(
        self,
        data: benedict,
        resource_type: str = ResourceType.Patient.value,
        references: dict = {},
        metadata: dict = {},
    ) -> None:
        super().__init__(data, resource_type, references, metadata)
        self._transformer.load_template("patient.j2")

    def _parse_identifiers(self):
        self._data_dict["source_id"] = self._data.get_str("patient_id_external_id")

    def _parse_name(self):
        self._data_dict["lastname"] = self._data.get_str("insured_family_name")
        self._data_dict["firstname"] = self._data.get_str("insured_given_name")
        self._data_dict["middleinitials"] = self._data.get_str("insured_initial")
        self._data_dict["suffix"] = self._data.get_str("insured_suffix")
        self._data_dict["prefix"] = self._data.get_str("insured_prefix")

    def _parse_dob(self):
        self._data_dict["dob"] = parse_date(self._data.get_str("insured_date_of_birth"))

    def _parse_address(self):
        self._data_dict["street_address_1"] = self._data.get_str("insured_street_address")
        self._data_dict["city"] = self._data.get_str("insured_city")
        self._data_dict["state"] = self._data.get_str("insured_state")
        self._data_dict["zip"] = self._data.get_str("insured_zip")
        self._data_dict["country"] = self._data.get_str("insured_country")
        self._data_dict["district_code"] = self._data.get_str("insured_county")

    def _parse_references(self) -> None:
        self._is_valid()
        if self._data_dict:
            self._data_dict["organization_id"] = self._metadata.get("child_org_id", "")
            get_pract_ref = self._get_reference(key="practitioner_reference", return_type=list)
            if get_pract_ref:
                self._data_dict["practitioners"] = get_pract_ref

    def build(self) -> dict:
        # parse identifier
        self._parse_identifiers()

        # update identifier system
        self._update_identifier()
        # parse name
        self._parse_name()
        # parse dob
        self._parse_dob()
        # parse address
        self._parse_address()
        # parse references
        self._parse_references()

        return self._data_dict
