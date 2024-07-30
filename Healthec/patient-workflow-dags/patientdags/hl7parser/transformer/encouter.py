from benedict import benedict

from patientdags.hl7parser.constants import (
    ENCOUNTER_PRACTITIONER_TYPE,
    PATIENT_ACCOUNT_NUMBER_SYSTEM,
    TRANSFORM_PRACTITIONER_TYPES,
    VIST_ID_SYSTEM,
)
from patientdags.hl7parser.enum import ResourceType
from patientdags.hl7parser.transformer.base import Base
from patientdags.hl7parser.utils import parse_date_time


class Encounter(Base):
    def __init__(
        self,
        data: benedict,
        resource_type: str = ResourceType.Encounter.value,
        references: dict = {},
        metadata: dict = {},
    ) -> None:
        super().__init__(data, resource_type, references, metadata)
        self._transformer.load_template("encounter.j2")

    def _parse_identifiers(self):
        if self._data.get_str("visit_id_number"):
            self._data_dict["source_id"] = self._data.get_str("visit_id_number")
            self._data_dict["source_system"] = VIST_ID_SYSTEM
        elif self._data.get_str("patient_account_number"):
            self._data_dict["source_id"] = self._data.get_str("patient_account_number")
            self._data_dict["source_system"] = PATIENT_ACCOUNT_NUMBER_SYSTEM
        else:
            get_mrn = self._data.get_str("patient_identifier_list_id")
            get_source_id = self._data.get_str("patient_identifier_id")
            get_admit_date = self._data.get_str("admit_date_time")
            get_dis_date = self._data.get_str("discharge_date_time")
            self._data_dict[
                "source_id"
            ] = f"{get_mrn if get_mrn else get_source_id}_{get_admit_date if get_admit_date else get_dis_date}"

    def _parse_different_codes(self):
        self._data_dict["class_code"] = self._data.get_str("patient_class")
        self._data_dict["pre_admission_code"] = self._data.get_str("preadmit_identifier")
        self._data_dict["type_code"] = self._data.get_str("admission_type")
        self._data_dict["admit_source_code"] = self._data.get_str("admit_source")
        self._data_dict["discharge_disposition_code"] = self._data.get_str("discharge_disposition")
        self._data_dict["service_type_code"] = self._data.get_str("hospital_service")
        self._data_dict["special_courtesy_code"] = self._data.get_str("vip_indicator")
        self._data_dict["re_admission_code"] = self._data.get_str("re_admission_indicator")
        self._data_dict["diet_preference_code"] = self._data.get_str("diet_type")

    def _parse_period(self):
        self._data_dict["period_start_date"] = parse_date_time(self._data.get_str("admit_date_time"))
        self._data_dict["period_end_date"] = parse_date_time(self._data.get_str("discharge_date_time"))

    def _parse_additiona_info(self):
        if "PV2" in self._data:
            get_addt_info = self._data.get("PV2")
            self._data_dict["length"] = get_addt_info.get_str("actual_length_of_inpatient_stay")
            self._data_dict["reason_code"] = get_addt_info.get_str("admit_reason_identifier")
            self._data_dict["reason_text"] = get_addt_info.get_str("admit_reason_identifier_text")
            self._data_dict["reason_system"] = get_addt_info.get_str("admit_reason_identifier_system")

    def _parse_practitioner_reference(self) -> None:
        practitioners = []
        for practitioner in TRANSFORM_PRACTITIONER_TYPES.get("PV1"):
            get_ref = self._get_reference(key=f"{practitioner}_reference", return_type=dict, temp_key="practitioner_id")
            if get_ref:
                get_ref.update(ENCOUNTER_PRACTITIONER_TYPE.get(practitioner))
                practitioners.append(get_ref)
        self._data_dict["participants"] = practitioners

    def _parse_references(self) -> None:
        self._is_valid()
        if self._data_dict:
            get_loc_ref = self._get_reference(key="assigned_patient_reference")
            if get_loc_ref:
                self._data_dict["location_id"] = get_loc_ref
            get_pat_ref = self._get_reference(key="patient_reference")
            if get_pat_ref:
                self._data_dict["patient_id"] = get_pat_ref
            self._parse_practitioner_reference()

    def build(self):
        # parse identifiers
        self._parse_identifiers()

        # update identifier system
        self._update_identifier()

        # parse multiple codes
        self._parse_different_codes()

        # parse period
        self._parse_period()

        # parse additional info PV2
        self._parse_additiona_info()

        # parse references
        self._parse_references()

        return self._data_dict
