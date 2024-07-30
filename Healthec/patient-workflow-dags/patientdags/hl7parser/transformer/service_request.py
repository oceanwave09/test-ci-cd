from benedict import benedict
from patientdags.hl7parser.enum import ResourceType
from patientdags.hl7parser.transformer.base import Base
from patientdags.hl7parser.utils import (
    parse_date_time,
)
from patientdags.hl7parser.constants import (
    SERVICE_REQUEST_STATUS,
)


class ServiceRequest(Base):
    def __init__(
        self,
        data: benedict,
        resource_type: str = ResourceType.ServiceRequest.value,
        references: dict = {},
        metadata: dict = {},
    ) -> None:
        super().__init__(data, resource_type, references, metadata)
        self._transformer.load_template("service_request.j2")

    def _parse_identifiers(self):
        self._data_dict["order_placer"] = self._data.get_str("placer_order_number_identifier")
        self._data_dict["order_placer_system"] = self._data.get_str("placer_order_number_namespace_id")
        self._data_dict["order_filler"] = self._data.get_str("filler_order_number_identifier")
        self._data_dict["order_filler_system"] = self._data.get_str("filler_order_number_namespace_id")

    def _parse_status(self):
        get_status = self._data.get_str("order_status")
        if get_status:
            get_status = get_status.upper()
        self._data_dict["status"] = (
            SERVICE_REQUEST_STATUS.get(get_status) if get_status in SERVICE_REQUEST_STATUS else ""
        )

    def _parse_effective_date_time(self):
        self._data_dict["authored_date_time"] = parse_date_time(self._data.get_str("order_effective_date_time"))

    def _parse_references(self) -> None:
        self._is_valid()
        if self._data_dict:
            get_enc_ref = self._get_reference(key="encounter_reference")
            if get_enc_ref:
                self._data_dict["encounter_id"] = get_enc_ref
            get_pat_ref = self._get_reference(key="patient_reference")
            if get_pat_ref:
                self._data_dict["patient_id"] = get_pat_ref
            get_prac_ref = self._get_reference(key="ordering_provider_reference")
            if get_prac_ref:
                self._data_dict["practitioner_id"] = get_prac_ref

    def build(self):
        # parse identifiers
        self._parse_identifiers()

        # parse status
        self._parse_status()

        # parse date
        self._parse_effective_date_time()

        # parse references
        self._parse_references()

        return self._data_dict
