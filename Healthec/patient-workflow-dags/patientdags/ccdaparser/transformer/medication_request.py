from benedict import benedict

from patientdags.ccdaparser.transformer.base import Base
from patientdags.ccdaparser.utils import parse_date_time, to_float, to_int


class MedicationRequest(Base):
    MED_REQ_STATUS = ["active", "on-hold", "cancelled", "completed", "entered-in-error", "stopped", "draft", "unknown"]

    def __init__(
        self,
        data: benedict,
        references: dict = {},
        metadata: dict = {},
        medication_id: str = "",
        practitioner_id: str = "",
    ) -> None:
        super().__init__(data, "MedicationRequest", references, metadata)
        self._type = type
        self._transformer.load_template("medication_request.j2")
        self._medication_id = medication_id
        self._practitioner_id = practitioner_id

    def _parse_status(self):
        status = str(self._data.get_str("status")).lower()
        if status in self.MED_REQ_STATUS:
            self.data_dict["status"] = status

    def _parse_authored(self):
        authored_date_time = parse_date_time(self._data.get_str("effective_date_time"))
        if authored_date_time:
            self.data_dict["authored_date_time"] = authored_date_time
        else:
            self.data_dict["authored_date_time"] = parse_date_time(self._data.get_str("effective_period.start"))

    def _parse_site(self):
        site_dict = {}
        site = self._data.get("site")
        if site and isinstance(site, list):
            site_dict["site_code"] = self._data.get_str("site[0].code")
            site_dict["site_system"] = self._data.get_str("site[0].system")
            site_dict["site_display"] = self._data.get_str("site[0].display")
            site_dict["site_text"] = (
                self._data.get_str("site[0].display")
                if self._data.get_str("site[0].display")
                else self._data.get_str("site[0].text")
            )
        elif site and isinstance(site, dict):
            site_dict["site_code"] = self._data.get_str("site.code")
            site_dict["site_system"] = self._data.get_str("site.system")
            site_dict["site_display"] = self._data.get_str("site.display")
            site_dict["site_text"] = (
                self._data.get_str("site.display")
                if self._data.get_str("site.display")
                else self._data.get_str("site.text")
            )
        return site_dict

    def _parse_route(self):
        route_dict = {}
        route = self._data.get("route")
        if route and isinstance(route, list):
            route_dict["route_code"] = self._data.get_str("route[0].code")
            route_dict["route_system"] = self._data.get_str("route[0].system")
            route_dict["route_display"] = self._data.get_str("route[0].display")
            route_dict["route_text"] = (
                self._data.get_str("route[0].display")
                if self._data.get_str("route[0].display")
                else self._data.get_str("route[0].text")
            )
        elif route and isinstance(route, dict):
            route_dict["route_code"] = self._data.get_str("route.code")
            route_dict["route_system"] = self._data.get_str("route.system")
            route_dict["route_display"] = self._data.get_str("route.display")
            route_dict["route_text"] = (
                self._data.get_str("route.display")
                if self._data.get_str("route.display")
                else self._data.get_str("route.text")
            )
        return route_dict

    def _parse_dosage_frequency(self):
        dosage_freq_dict = {}
        dosage_freq_dict["frequency"] = to_int(self._data.get_str("dosage_frequency.value"))
        dosage_freq_dict["frequency_period_unit"] = self._data.get_str("dosage_frequency.unit")
        return dosage_freq_dict

    def _parse_dosage_text(self):
        dosage_text_dict = {}
        dosage_text_dict["text"] = self._data.get_str("dosage_text")
        return dosage_text_dict

    def _parse_dosage_quantity(self):
        dosage_quantity_dict = {}
        dosage_quantity_dict["quantity"] = to_float(self._data.get_str("dose_quantity.value"))
        dosage_quantity_dict["quantity_unit"] = self._data.get_str("dose_quantity.unit")
        return dosage_quantity_dict

    def _parse_dosage_rate(self):
        dosage_rate_dict = {}
        dosage_rate_dict["rate_value"] = self._data.get_str("rate_quantity.value")
        dosage_rate_dict["rate_unit"] = self._data.get_str("rate_quantity.unit")
        return dosage_rate_dict

    def _parse_max_dose_per_period(self):
        dose_per_period_dict = {}
        dose_per_period_dict["max_dose_per_period_value"] = to_float(self._data.get_str("max_dose_per_period"))
        return dose_per_period_dict

    def _parse_max_dose_per_administration(self):
        dose_per_administration_dict = {}
        dose_per_administration_dict["rate_value"] = to_float(self._data.get_str("max_dose_per_administration.value"))
        dose_per_administration_dict["rate_unit"] = self._data.get_str("max_dose_per_administration.unit")
        return dose_per_administration_dict

    def _parse_dosage(self):
        dosage_dict = []
        dosage_entry = {}
        dosage_text = self._parse_dosage_text()
        dosage_entry.update(dosage_text)
        dosage_site = self._parse_site()
        dosage_entry.update(dosage_site)
        dosage_route = self._parse_route()
        dosage_entry.update(dosage_route)
        dosage_frequency = self._parse_dosage_frequency()
        if dosage_frequency.get("frequency"):
            dosage_entry.update(dosage_frequency)
        dosage_quantity = self._parse_dosage_quantity()
        if dosage_quantity.get("quantity"):
            dosage_entry.update(dosage_quantity)
        dosage_rate = self._parse_dosage_rate()
        if dosage_rate.get("rate_value"):
            dosage_entry.update(dosage_rate)
        dose_per_period = self._parse_max_dose_per_period()
        if dose_per_period.get("max_dose_per_period_value"):
            dosage_entry.update(dose_per_period)
        dose_per_administration = self._parse_max_dose_per_administration()
        if dose_per_administration.get("rate_value"):
            dosage_entry.update(dose_per_administration)
        dosage_dict.append(dosage_entry)
        self.data_dict["dosages"] = dosage_dict

    def _parse_supply(self):
        supplies = self._data.get_list("supply")
        if len(supplies) > 0 and supplies[0]:
            # supply quantity
            self.data_dict["quantity"] = to_float(self._data.get_str("supply[0].quantity.value"))
            self.data_dict["quantity_unit"] = self._data.get_str("supply[0].quantity.unit")

            # supply validity period
            validity_date_time = parse_date_time(self._data.get_str("supply[0].validity_date_time"))
            if validity_date_time:
                self.data_dict["period_start_date"] = validity_date_time
            else:
                self.data_dict["period_start_date"] = parse_date_time(
                    self._data.get_str("supply[0].validity_period.start")
                )
                self.data_dict["period_end_date"] = parse_date_time(self._data.get_str("supply[0].validity_period.end"))

            # supply num. of repeats
            self.data_dict["number_of_repeats_allowed"] = to_int(self._data.get_str("supply[0].repeats_allowed"))

    def _parse_medication_reference(self):
        self.data_dict["medication_id"] = self._medication_id

    def _parse_practitioner_reference(self):
        self.data_dict["practitioner_id"] = self._practitioner_id

    def build(self):
        # internal id
        self._set_internal_id()
        # identifier
        self._parse_identifier()
        # category
        self._parse_category()
        # status
        self._parse_status()
        # authored on
        self._parse_authored()
        # dosage
        self._parse_dosage()
        # supply
        self._parse_supply()
        # patient reference
        self._parse_patient_reference()
        # medication reference
        self._parse_medication_reference()
        # practitioner reference
        self._parse_practitioner_reference()
        return self.data_dict
