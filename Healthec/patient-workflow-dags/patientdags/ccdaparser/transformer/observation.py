from benedict import benedict

from patientdags.ccdaparser.transformer.base import Base
from patientdags.ccdaparser.utils import (
    parse_date_time,
    parse_range_reference,
    to_float,
)


class Observation(Base):
    OBSERVATION_STATUS = [
        "registered",
        "preliminary",
        "final",
        "amended",
        "corrected",
        "cancelled",
        "entered-in-error",
        "unknown",
    ]

    def __init__(self, data: benedict, references: dict = {}, metadata: dict = {}) -> None:
        super().__init__(data, "Observation", references, metadata)
        self._type = type
        self._transformer.load_template("observation.j2")

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

    def _parse_method(self):
        method = self._data.get("method")
        if method and isinstance(method, list):
            self.data_dict["method_code"] = self._data.get_str("method[0].code")
            self.data_dict["method_system"] = self._data.get_str("method[0].system")
            self.data_dict["method_display"] = self._data.get_str("method[0].display")
            self.data_dict["method_text"] = (
                self._data.get_str("method[0].display")
                if self._data.get_str("method[0].display")
                else self._data.get_str("method[0].text")
            )
        elif method and isinstance(method, dict):
            self.data_dict["method_code"] = self._data.get_str("method.code")
            self.data_dict["method_system"] = self._data.get_str("method.system")
            self.data_dict["method_display"] = self._data.get_str("method.display")
            self.data_dict["method_text"] = (
                self._data.get_str("method.display")
                if self._data.get_str("method.display")
                else self._data.get_str("method.text")
            )

    def _parse_value(self):
        value = self._data.get("value")
        if value and isinstance(value, list):
            self.data_dict["value_code"] = self._data.get_str("value[0].code")
            self.data_dict["value_system"] = self._data.get_str("value[0].system")
            self.data_dict["value_display"] = self._data.get_str("value[0].display")
            self.data_dict["value_text"] = (
                self._data.get_str("value[0].display")
                if self._data.get_str("value[0].display")
                else self._data.get_str("value[0].text")
            )
        elif value and isinstance(value, dict):
            self.data_dict["value_code"] = self._data.get_str("value.code")
            self.data_dict["value_system"] = self._data.get_str("value.system")
            self.data_dict["value_display"] = self._data.get_str("value.display")
            self.data_dict["value_text"] = (
                self._data.get_str("value.display")
                if self._data.get_str("value.display")
                else self._data.get_str("value.text")
            )

    def _parse_effective(self):
        effective_date_time = parse_date_time(self._data.get_str("effective_date_time"))
        if effective_date_time:
            self.data_dict["effective_date_time"] = effective_date_time
        else:
            self.data_dict["effective_period_start"] = parse_date_time(self._data.get_str("effective_period.start"))
            self.data_dict["effective_period_end"] = parse_date_time(self._data.get_str("effective_period.end"))

    def _parse_status(self):
        status = str(self._data.get_str("status")).lower()
        # change status from completed to final
        if status == "completed":
            status = "final"
        if status in self.OBSERVATION_STATUS:
            self.data_dict["status"] = status

    def _parse_component_code(self, component: benedict, code_system: str = "") -> dict:
        code = {}
        added_prefer_code = False
        added_code = False
        if component.get_str("code.code"):
            code["code"] = component.get_str("code.code")
            code["code_system"] = component.get_str("code.system")
            code["code_display"] = component.get_str("code.display")
            code["code_text"] = (
                component.get_str("code.display")
                if component.get_str("code.display")
                else component.get_str("code.text")
            )
            if code_system and code_system in component.get_str("code.system"):
                added_prefer_code = True
            added_code = True
        if not added_prefer_code and len(component.get_list("code.translation")) > 0:
            for entry in component.get_list("code.translation"):
                if code_system and code_system in entry.get_str("system_name").lower():
                    code["code"] = entry.get_str("code")
                    code["code_system"] = entry.get_str("system")
                    code["code_display"] = entry.get_str("display")
                    code["code_text"] = entry.get_str("display") if entry.get_str("display") else entry.get_str("text")
                    added_prefer_code = True
                    added_code = True
            if not added_code:
                code["code"] = component.get_str("code.translation[0].code")
                code["code_system"] = component.get_str("code.translation[0].system")
                code["code_display"] = component.get_str("code.translation[0].display")
                code["code_text"] = (
                    component.get_str("code.translation[0].display")
                    if component.get_str("code.translation[0].display")
                    else component.get_str("code.translation[0].text")
                )
        return code

    def _parse_component_interpretation(self, component: benedict):
        interpr_dict = {}
        interpr = component.get("interpretation")
        if interpr and isinstance(interpr, list):
            interpr_dict["interpretation_code"] = component.get_str("interpretation[0].code")
            interpr_dict["interpretation_system"] = component.get_str("interpretation[0].system")
            interpr_dict["interpretation_display"] = component.get_str("interpretation[0].display")
            interpr_dict["interpretation_text"] = (
                component.get_str("interpretation[0].display")
                if component.get_str("interpretation[0].display")
                else component.get_str("interpretation[0].text")
            )
        elif interpr and isinstance(interpr, dict):
            interpr_dict["interpretation_code"] = component.get_str("interpretation.code")
            interpr_dict["interpretation_system"] = component.get_str("interpretation.system")
            interpr_dict["interpretation_display"] = component.get_str("interpretation.display")
            interpr_dict["interpretation_text"] = (
                component.get_str("interpretation.display")
                if component.get_str("interpretation.display")
                else component.get_str("interpretation.text")
            )
        return interpr_dict

    def _parse_component_ref_range(self, component: benedict):
        ref_range_dict = {}
        ref_range_text = component.get_str("reference_range.value")
        if ref_range_text:
            ref_range_low, ref_range_high = parse_range_reference(ref_range_text)
            if ref_range_low or ref_range_high:
                ref_range_dict["reference_range_low_value"] = ref_range_low
                ref_range_dict["reference_range_high_value"] = ref_range_high
        elif component.get("reference_range.low") or component.get("reference_range.high"):
            ref_range_dict["reference_range_low_value"] = component.get_str("reference_range.low.value")
            low_unit = component.get_str("reference_range.low.unit")
            ref_range_dict["reference_range_low_unit"] = low_unit if low_unit and low_unit != "1" else ""
            ref_range_dict["reference_range_high_value"] = component.get_str("reference_range.high.value")
            high_unit = component.get_str("reference_range.high.unit")
            ref_range_dict["reference_range_high_unit"] = high_unit if high_unit and high_unit != "1" else ""
        return ref_range_dict

    def _parse_component_value_quantity(self, component: benedict):
        value_quantity_dict = {}
        value_quantity = component.get("value")
        if value_quantity:
            value_quantity_dict["quantity_value"] = to_float(component.get_str("value.value"))
            quantity_unit = component.get_str("value.unit")
            value_quantity_dict["quantity_unit"] = quantity_unit if quantity_unit and quantity_unit != "1" else ""
        return value_quantity_dict

    def _parse_component(self):
        components_dict = []
        components = self._data.get("component", [])
        for component in components:
            components_dict_entry = {}
            code = self._parse_component_code(component)
            interpr = self._parse_component_interpretation(component)
            ref_range = self._parse_component_ref_range(component)
            value_quantity = self._parse_component_value_quantity(component)
            components_dict_entry.update(code)
            components_dict_entry.update(interpr)
            components_dict_entry.update(ref_range)
            components_dict_entry.update(value_quantity)
            if len(components_dict_entry) > 0:
                components_dict.append(components_dict_entry)
        if len(components_dict) > 0:
            self.data_dict["components"] = components_dict

    def build(self):
        # internal id
        self._set_internal_id()
        # identifier
        self._parse_identifier()
        # category
        self._parse_category()
        # code
        self._parse_code()
        # status
        self._parse_status()
        # body site
        self._parse_body_site()
        # method
        self._parse_method()
        # value
        self._parse_value()
        # effective date time
        self._parse_effective()
        # components
        self._parse_component()
        # patient reference
        self._parse_patient_reference()
        return self.data_dict
