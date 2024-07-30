from benedict import benedict
from patientdags.hl7parser.enum import ResourceType
from patientdags.hl7parser.transformer.base import Base
from patientdags.hl7parser.utils import (
    parse_date_time,
)
from patientdags.hl7parser.constants import COVERAGE_STATUS_CODES


class Coverage(Base):
    def __init__(
        self,
        data: benedict,
        resource_type: str = ResourceType.Coverage.value,
        references: dict = {},
        metadata: dict = {},
    ) -> None:
        super().__init__(data, resource_type, references, metadata)
        self._transformer.load_template("coverage.j2")

    def _parse_identifiers(self) -> None:
        self._data_dict["subscriber_id"] = self._data.get_str("insured_id_number")

    def _parse_kind(self) -> None:
        self._data_dict["kind"] = "insurance"

    def _parse_status(self) -> None:
        get_status = self._data.get_str("billing_status")
        self._data_dict["status"] = get_status if get_status in COVERAGE_STATUS_CODES else "active"

    def _parse_type(self) -> None:
        self._data_dict["type_text"] = self._data.get_str("plan_type")

    def _parse_code(self) -> None:
        self._data_dict["relationship_code_system"] = self._data.get_str("insured_relationship_code_system")
        self._data_dict["relationship_code"] = self._data.get_str("insured_relationship_identifier")
        self._data_dict["relationship_text"] = self._data.get_str("insured_relationship_text")

    def _parse_period(self) -> None:
        self._data_dict["period_start_date"] = parse_date_time(self._data.get_str("plan_effective_date"))
        self._data_dict["period_end_date"] = parse_date_time(self._data.get_str("plan_expiration_date"))

    def _parse_references(self) -> None:
        self._is_valid()
        if self._data_dict:
            get_org_ref = self._get_reference(key="insurance_company_reference")
            if get_org_ref:
                self._data_dict["insurer_organization_id"] = get_org_ref
            get_pat_ref = self._get_reference(key="patient_reference")
            if get_pat_ref:
                self._data_dict["beneficiary_patient_id"] = get_pat_ref

    def build(self):
        # parse identifiers
        self._parse_identifiers()

        # update identifier system
        self._update_identifier()

        # parse kind
        self._parse_kind()

        # parse status
        self._parse_status()

        # parse type
        self._parse_type()

        # parse code
        self._parse_code()

        # parse period
        self._parse_period()

        # parse references
        self._parse_references()

        return self._data_dict
