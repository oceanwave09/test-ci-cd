from benedict import benedict
from patientdags.hl7parser.enum import ResourceType
from patientdags.hl7parser.transformer.base import Base
from patientdags.hl7parser.utils import (
    parse_date_time,
    to_float,
)
from patientdags.hl7parser.constants import (
    OBSERVATION_STATUS,
    LONIC_CODE_SYSTEM,
)


class Observation(Base):
    def __init__(
        self,
        data: benedict,
        resource_type: str = ResourceType.Observation.value,
        references: dict = {},
        metadata: dict = {},
    ) -> None:
        super().__init__(data, resource_type, references, metadata)
        self._transformer.load_template("observation.j2")

    def _parse_code(self):
        self._data_dict["code"] = self._data.get_str("observation_identifier")
        self._data_dict["code_display"] = self._data.get_str("observation_identifier_text")
        self._data_dict["loinc_code"] = self._data.get_str("observation_alternate_identifier")
        self._data_dict["loinc_code_display"] = self._data.get_str("observation_alternate_coding_system")
        if self._data.get_str("observation_identifier") and self._data.get_str("observation_identifier_system") == "LN":
            self._data_dict["code_system"] = LONIC_CODE_SYSTEM
            self._data_dict["code_text"] = self._data.get_str("observation_identifier_text")
        if self._data.get_str("observation_alternate_identifier"):
            self._data_dict["loinc_code_system"] = LONIC_CODE_SYSTEM
            self._data_dict["code_text"] = self._data.get_str("observation_alternate_text")

    def _parse_method_code(self):
        self._data_dict["method_code"] = self._data.get_str("observation_method_identifier")
        self._data_dict["method_system"] = self._data.get_str("observation_method_system")
        self._data_dict["method_text"] = self._data.get_str("observation_method_text")

    def _parse_effective_date(self):
        self._data_dict["effective_date_time"] = parse_date_time(self._data.get_str("date_time_of_the_observation"))

    def _parse_reference_range(self):
        reference_range_values = (
            self._data.get("reference_range").split("-") if self._data.get("reference_range") else [None, None]
        )
        self._data_dict["reference_range_low_value"] = to_float(reference_range_values[0])
        if len(reference_range_values) > 1:
            self._data_dict["reference_range_high_value"] = to_float(reference_range_values[1])

    def _parse_interpretation(self):
        self._data_dict["interpretation_text"] = self._data.get_str("abnormal_flags")

    def _parse_body_site(self):
        self._data_dict["body_site_code"] = self._data.get_str("observation_site")

    def _parse_quantity(self):
        self._data_dict["quantity_value"] = to_float(self._data.get_str("observation_value"))
        self._data_dict["quantity_unit"] = self._data.get_str("units")

    def _parse_status(self):
        get_status = self._data.get_str("observation_result_status")
        if get_status:
            get_status = get_status.upper()[0]
            self._data_dict["status"] = OBSERVATION_STATUS[get_status] if get_status in OBSERVATION_STATUS else "unknown"

    def _parse_references(self) -> None:
        self._is_valid()
        if self._data_dict:
            get_enc_ref = self._get_reference(key="encounter_reference")
            if get_enc_ref:
                self._data_dict["encounter_id"] = get_enc_ref
            get_pat_ref = self._get_reference(key="patient_reference")
            if get_pat_ref:
                self._data_dict["patient_id"] = get_pat_ref
            performer_ref = []
            get_org_ref = self._get_reference(key="performing_organization_reference")
            if get_org_ref:
                performer_ref.append({"organization_id": get_org_ref})
            get_pract_ref = self._get_reference(
                key="responsible_observer_reference", return_type=dict, temp_key="practitioner_id"
            )
            if get_pract_ref:
                performer_ref.append(get_pract_ref)
            self._data_dict["performer"] = performer_ref

    def build(self):
        # parse code
        self._parse_code()

        # parse status
        self._parse_status()

        # parse method codes
        self._parse_method_code()

        # parse period
        self._parse_effective_date()

        # parse reference_range
        self._parse_reference_range()

        # parse interpretation
        self._parse_interpretation()

        # parse body site
        self._parse_body_site()

        # parse quantity
        self._parse_quantity()

        # parse references
        self._parse_references()

        return self._data_dict
