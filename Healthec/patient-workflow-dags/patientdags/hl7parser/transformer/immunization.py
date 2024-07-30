from benedict import benedict

from patientdags.hl7parser.constants import (
    CPT_CODE_SYSTEM,
    CVX_CODE_SYSTEM,
    IMMUNIZATION_STATUS,
    NDC_CODE_SYSTEM,
)
from patientdags.hl7parser.enum import ResourceType
from patientdags.hl7parser.transformer.base import Base
from patientdags.hl7parser.utils import parse_date, parse_date_time, to_float


class Immunization(Base):
    def __init__(
        self,
        data: benedict,
        resource_type: str = ResourceType.Immunization.value,
        references: dict = {},
        metadata: dict = {},
    ) -> None:
        super().__init__(data, resource_type, references, metadata)
        self._transformer.load_template("immunization.j2")

    def _parse_status(self):
        get_status = self._data.get_str("completion_status")
        if get_status:
            status_code = IMMUNIZATION_STATUS.get(get_status.upper())
            self._data_dict["status"] = status_code if status_code else ""

    def _get_code_system(self, code_system):
        if code_system.upper() == "CVX":
            return CVX_CODE_SYSTEM
        elif code_system.upper() == "CPT":
            return CPT_CODE_SYSTEM
        elif code_system.upper() == "NDC":
            return NDC_CODE_SYSTEM
        else:
            return code_system

    def _parse_administered_code(self):
        vaccines = []
        if self._data.get_str("administered_code_identifier"):
            vaccines.append(
                {
                    "vaccine_code": self._data.get_str("administered_code_identifier"),
                    "vaccine_display": self._data.get_str("administered_code_text"),
                    "vaccine_system": self._get_code_system(
                        self._data.get_str("administered_code_name_of_coding_system")
                    ),
                }
            )
        if self._data.get_str("administered_code_alternate_identifier"):
            vaccines.append(
                {
                    "vaccine_code": self._data.get_str("administered_code_alternate_identifier"),
                    "vaccine_display": self._data.get_str("administered_code_alternate_text"),
                    "vaccine_system": self._get_code_system(
                        self._data.get_str("administered_code_name_of_alternate_coding_system")
                    ),
                }
            )
        self._data_dict["vaccines"] = vaccines

    def _parse_immunization_dates(self):
        self._data_dict["occurrence_date_time"] = parse_date_time(
            self._data.get_str("date_time_start_of_administration")
        )
        self._data_dict["expiration_date"] = parse_date(self._data.get_str("substance_expiration_date"))

    def _parse_lot_number(self):
        self._data_dict["lot_number"] = self._data.get_str("substance_lot_number")

    def _parse_dose_quantity(self):
        get_amount = self._data.get_str("administered_amount")
        if get_amount:
            self._data_dict["dose_quantity_value"] = to_float(get_amount)
        self._data_dict["dose_quantity_unit"] = self._data.get_str("administered_units_identifier")
        self._data_dict["dose_quantity_system"] = self._data.get_str("administered_units_name_of_coding_system")
        self._data_dict["dose_quantity_code"] = self._data.get_str("administered_units_text")

    def _parse_status_reason(self):
        status_reasons = []
        if self._data.get_str("substance_refusal_reason_identifier"):
            status_reasons.append(
                {
                    "status_reason_code": self._data.get_str("substance_refusal_reason_identifier"),
                    "status_reason_display": self._data.get_str("substance_refusal_reason_text"),
                    "status_reason_system": self._data.get_str("substance_refusal_reason_name_of_coding_system"),
                }
            )
        if self._data.get_str("substance_refusal_reason_alternate_identifier"):
            status_reasons.append(
                {
                    "status_reason_code": self._data.get_str("substance_refusal_reason_alternate_identifier"),
                    "status_reason_display": self._data.get_str("substance_refusal_reason_alternate_text"),
                    "status_reason_system": self._data.get_str(
                        "substance_refusal_reason_name_of_alternate_coding_system"
                    ),
                }
            )
        self._data_dict["status_reasons"] = status_reasons

    def _parse_reason_code(self):
        reason_codes = []
        if self._data.get_str("indication_identifier"):
            reason_codes.append(
                {
                    "reason_code": self._data.get_str("indication_identifier"),
                    "reason_display": self._data.get_str("indication_text"),
                    "reason_system": self._data.get_str("indication_name_of_coding_system"),
                }
            )
        if self._data.get_str("indication_alternate_identifier"):
            reason_codes.append(
                {
                    "reason_code": self._data.get_str("indication_alternate_identifier"),
                    "reason_display": self._data.get_str("indication_alternate_text"),
                    "reason_system": self._data.get_str("indication_name_of_alternate_coding_system"),
                }
            )
        self._data_dict["reason_codes"] = reason_codes

    def _parse_provider_type(self):
        if self._references.get("administering_provider_reference"):
            self._data_dict["function_code"] = "AP"
            self._data_dict["function_code_system"] = "http://terminology.hl7.org/CodeSystem/v2-0443"
            self._data_dict["function_code_display"] = "Administering Provider"
            self._data_dict["function_text"] = "Administering Provider"

    def _parse_route(self):
        routes = []
        if self._data.get_str("route_identifier"):
            routes.append(
                {
                    "route_code": self._data.get_str("route_identifier"),
                    "route_display": self._data.get_str("route_text"),
                    "route_system": self._data.get_str("route_name_of_coding_system"),
                }
            )
        if self._data.get_str("route_alternate_identifier"):
            routes.append(
                {
                    "route_code": self._data.get_str("route_alternate_identifier"),
                    "route_display": self._data.get_str("route_alternate_text"),
                    "route_system": self._data.get_str("route_name_of_alternate_coding_system"),
                }
            )
        self._data_dict["routes"] = routes

    def _parse_site(self):
        sites = []
        if self._data.get_str("administration_site_identifier"):
            sites.append(
                {
                    "site_code": self._data.get_str("administration_site_identifier"),
                    "site_display": self._data.get_str("administration_site_text"),
                    "site_system": self._data.get_str("administration_site_name_of_coding_system"),
                }
            )
        if self._data.get_str("administration_site_alternate_identifier"):
            sites.append(
                {
                    "site_code": self._data.get_str("administration_site_alternate_identifier"),
                    "site_display": self._data.get_str("administration_site_alternate_text"),
                    "site_system": self._data.get_str("administration_site_name_of_alternate_coding_system"),
                }
            )
        self._data_dict["sites"] = sites

    def _parse_references(self) -> None:
        self._is_valid()
        if self._data_dict:
            get_enc_ref = self._get_reference(key="encounter_reference")
            if get_enc_ref:
                self._data_dict["encounter_id"] = get_enc_ref
            get_pat_ref = self._get_reference(key="patient_reference")
            if get_pat_ref:
                self._data_dict["patient_id"] = get_pat_ref
            get_org_ref = self._get_reference(key="manufacturer_reference")
            if get_org_ref:
                self._data_dict["organization_id"] = get_org_ref
            get_prac_ref = self._get_reference(key="administering_provider_reference")
            if get_prac_ref:
                self._data_dict["practitioner_id"] = get_prac_ref

    def build(self):
        # parse Vacine codes
        self._parse_administered_code()

        # parse period
        self._parse_status()

        # parse immunization date
        self._parse_immunization_dates()

        # parse lot number
        self._parse_lot_number()

        # parse immunization dose
        self._parse_dose_quantity()

        # parse status reason
        self._parse_status_reason()

        # parse reason
        self._parse_reason_code()

        # parse practitioner type
        self._parse_provider_type()

        # parse route
        self._parse_route()

        # parse site
        self._parse_site()

        # parse references
        self._parse_references()

        return self._data_dict
