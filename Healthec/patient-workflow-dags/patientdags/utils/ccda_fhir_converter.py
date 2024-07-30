import json
import uuid
import logging
from benedict import benedict
from smart_open import open
from copy import copy
from fhirtransformer.transformer import FHIRTransformer


PAT_JINJA_TEMPLATE = "patient.j2"
PRC_JINJA_TEMPLATE = "practitioner.j2"
ORG_JINJA_TEMPLATE = "organization.j2"
ENC_JINJA_TEMPLATE = "encounter.j2"
CON_JINJA_TEMPLATE = "condition.j2"

ENC_RESOURCE = "Encounter"
PAT_RESOURCE = "Patient"
PRC_RESOURCE = "Practitioner"
CON_RESOURCE = "Condition"
ORG_RESOURCE = "Organiztion"


class CcdaToFhir:
    def __init__(self, file_path: str) -> None:
        self._file_path = file_path
        self._transform = FHIRTransformer()
        self._reset_dependent_attriutes()

    def _reset_dependent_attriutes(self) -> None:
        self._resource_list = list()
        self._resource_unique_id = dict()
        self._org_prac_data = {"practitioner": [], "organization": []}
        self._resource_count = dict()

    def _update_resource_count(self, resource: str) -> None:
        resource = resource.lower()
        if resource in self._resource_count:
            self._resource_count[resource] += 1
        else:
            self._resource_count[resource] = 1

    def _get_resource_count(self, resource: str) -> int:
        resource = resource.lower()
        if resource not in self._resource_count:
            self._resource_count[resource] = 1
            return 1
        return self._resource_count[resource]

    def _update_row_id(self, row_id: str = None) -> None:
        self._row_id = row_id

    def _create_bundle(self, data: dict) -> dict:
        entry_uuid = data.get("id")
        entry = {
            "fullUrl": f"urn:uuid:{entry_uuid}",
            "resource": data,
            "request": {"method": "PUT", "url": f"{data.get('resourceType')}/{entry_uuid}"},
        }
        return entry

    def _update_identifier(self, data: dict) -> dict:
        for key, value in data.items():
            if value in self._resource_unique_id:
                data[key] = self._resource_unique_id[value]
        return data

    def _transform_resources(self, data: dict, resource_type: str) -> dict:
        self._update_resource_count(resource=resource_type)
        ref_lower = resource_type.lower()
        if ref_lower in self._org_prac_data:
            self._org_prac_data[ref_lower].append(data)
        get_unique_id = data.get("internal_id")
        if get_unique_id not in self._resource_unique_id:
            self._resource_unique_id[get_unique_id] = str(uuid.uuid4())
        data = self._update_identifier(data=data)
        transformed_data = json.loads(self._transform.render_resource(resource_type, data))
        self._resource_list.append(self._create_bundle(data=transformed_data))
        return transformed_data.get("id")

    def _load_template(self, template_name: str) -> None:
        self._transform.load_template(template_name=template_name)

    def _compare_resources(self, source_obj_type: str, search_obj: dict) -> str or None:
        for source_obj in self._org_prac_data.get(source_obj_type.lower()):
            copy_obj = copy(source_obj)
            del copy_obj["internal_id"]
            if copy_obj == search_obj:
                return source_obj.get("internal_id")
        return None

    def _transform_patient(self, data: dict, resource: str = "Patient", references: dict = {}) -> dict:
        data_dict = {}
        data_dict["internal_id"] = f"{resource}_{self._row_id}_{self._get_resource_count(resource=resource)}"
        data_dict["source_id"] = data.get_str("identifier[0].value")
        data_dict["ssn"] = data.get_str("identifier[1].value")
        data_dict["member_id"] = data.get_str("identifier[2].value")
        data_dict["city"] = data.get_str("address[0].city")
        data_dict["country"] = data.get_str("address[0].country")
        data_dict["zip"] = data.get_str("address[0].postal_code")
        data_dict["state"] = data.get_str("address[0].state")
        data_dict["street_address_1"] = data.get_str("address[0].line[0]")
        data_dict["street_address_2"] = data.get_str("address[0].line[1]")
        data_dict["lastname"] = data.get_str("name[0].family")
        data_dict["firstname"] = data.get_str("name[0].given[0]")
        data_dict["middleinitials"] = data.get_str("name[0].given[1]")
        data_dict["dob"] = data.get_str("birth_date")
        data_dict["gender"] = data.get_str("gender.code")
        data_dict["marital_status_code"] = data.get_str("marital_status.code")
        data_dict["marital_status_display"] = data.get_str("marital_status.display")
        data_dict["marital_status_system"] = data.get_str("marital_status.system")
        data_dict["marital_status_text"] = data.get_str("marital_status.system_name")
        data_dict["phone_home"] = data.get_str("phone_home[0]")
        data_dict["phone_mobile"] = data.get_str("phone_mobile[0]")
        data_dict["phone_work"] = data.get_str("phone_work[0]")
        data_dict["organization_id"] = references.get("organization_reference")
        return self._transform_resources(data_dict, resource)

    def _transform_practice(self, data: dict, resource: str = "Organization", references: dict = {}) -> dict:
        data_dict = {}
        data_dict["internal_id"] = f"{resource}_{self._row_id}_{self._get_resource_count(resource=resource)}"
        data_dict["source_id"] = data.get_int("identifier[0].value", "")
        data_dict["city"] = data.get_str("address.city")
        data_dict["country"] = data.get_str("address.country")
        data_dict["zip"] = data.get_str("address.postal_code")
        data_dict["state"] = data.get_str("address.state")
        data_dict["street_address_1"] = data.get_str("address.line[0]")
        data_dict["street_address_2"] = data.get_str("address.line[1]")
        data_dict["name"] = data.get_str("name")
        data_dict["phone_work"] = data.get_str("phone_work")
        return self._transform_resources(data_dict, resource)

    def _transform_practitioner(self, data: dict, resource: str = "Practitioner", references: dict = {}) -> dict:
        data_dict = {}
        data_dict["source_id"] = data.get_str("identifier[0].value")
        data_dict["city"] = data.get_str("address.city")
        data_dict["country"] = data.get_str("address.country")
        data_dict["zip"] = data.get_str("address.postal_code")
        data_dict["state"] = data.get_str("address.state")
        data_dict["street_address_1"] = data.get_str("address.line[0]")
        data_dict["street_address_2"] = data.get_str("address.line[1]")
        data_dict["lastname"] = data.get_str("name[0].family")
        data_dict["firstname"] = data.get_str("name[0].given[0]")
        data_dict["middleinitials"] = data.get_str("name[0].given[1]")
        find_existance = self._compare_resources(source_obj_type=resource, search_obj=data_dict)
        if not find_existance:
            data_dict["internal_id"] = f"{resource}_{self._row_id}_{self._get_resource_count(resource=resource)}"
            return self._transform_resources(data_dict, resource)
        return find_existance

    def _transform_condition(self, data: dict, resource: str = "Condition", references: dict = {}) -> dict:
        data_dict = {}
        data_dict["internal_id"] = f"{resource}_{self._row_id}_{self._get_resource_count(resource=resource)}"
        data_dict["source_id"] = data.get_str("identifier[0].value")
        data_dict["category_code"] = data.get_str("category.code")
        data_dict["category_display"] = data.get_str("category.display")
        data_dict["category_system"] = data.get_str("category.system")
        data_dict["code"] = data.get_str("code.code")
        data_dict["code_system"] = data.get_str("code.system")
        data_dict["code_display"] = data.get_str("code.display")
        data_dict["code_text"] = data.get_str("code.text")
        data_dict["onset_date_time"] = data.get_str("onset_date_time")
        data_dict["encounter_id"] = references.get("encounter_reference")
        data_dict["patient_id"] = references.get("patient_reference")
        # data_dict["practitioner_id"] = references.get("practitioner_reference")
        return self._transform_resources(data_dict, resource)

    def _transform_encounter(self, data: benedict, resource: str = "Encounter", references: dict = {}) -> dict:
        data_dict = {}
        data_dict["internal_id"] = f"{resource}_{self._row_id}_{self._get_resource_count(resource=resource)}"
        data_dict["period_start_date"] = data.get_str("effective_period.start").replace("-", "")
        data_dict["period_end_date"] = data.get_str("effective_period.end").replace("-", "")
        data_dict["source_id"] = data.get_str("identifier[0].value")
        data_dict["type_code"] = data.get_str("type.code")
        data_dict["type_system"] = data.get_str("type.system")
        data_dict["type_display"] = data.get_str("type.display")
        data_dict["type_text"] = data.get_str("type.text")
        data_dict["discharge_disposition_code"] = data.get_str("discharge_disposition.code")
        data_dict["discharge_disposition_display"] = data.get_str("discharge_disposition.display")
        data_dict["discharge_disposition_system"] = data.get_str("discharge_disposition.system")
        # data_dict["practitioner_id"] = references.get("practitioner_reference")[0]
        data_dict["patient_id"] = references.get("patient_reference")
        return self._transform_resources(data_dict, resource)

    def _process_patient(self, patient: benedict, source_key: str = None, reference: dict = None) -> dict:
        self._load_template(PAT_JINJA_TEMPLATE)
        return self._transform_patient(data=patient)

    def _process_practice(self, practice_data: benedict, source_key: str = None, reference: dict = None) -> dict:
        self._load_template(ORG_JINJA_TEMPLATE)
        if not source_key:
            return self._transform_practice(data=practice_data)

    def _process_practitioners(self, practitioners: benedict, source_key: str = None, reference: dict = None) -> list:
        self._load_template(PRC_JINJA_TEMPLATE)
        get_practitioners = len(practitioners.get(source_key))
        practitioner_ref = []
        if get_practitioners:
            for _index in range(get_practitioners):
                practitioner = practitioners.get(f"{source_key}[{_index}]")
                practitioner_ref.append(self._transform_practitioner(data=practitioner, references=reference))
        return practitioner_ref

    def _process_diagnoses(self, diagnoses: benedict, source_key: str = None, reference: dict = None) -> list:
        self._load_template(CON_JINJA_TEMPLATE)
        get_diagnoses_len = len(diagnoses.get(source_key))
        diagnoses_ref = []
        if get_diagnoses_len:
            for _index in range(get_diagnoses_len):
                diagnosis = diagnoses.get(f"{source_key}[{_index}]")
                diagnoses_ref.append(self._transform_condition(data=diagnosis, references=reference))
        return diagnoses_ref

    def _process_encounter(self, encounters: list = None, source_key: str = None, reference: dict = None) -> list:
        get_encounter_len = len(encounters.get(source_key))
        for _index in range(get_encounter_len):
            references = {}
            encounter = encounters.get(f"{source_key}[{_index}]")
            references.update(reference)
            if "performer" in encounter:
                references["practitioner_reference"] = self._process_practitioners(
                    practitioners=encounter, source_key="performer"
                )
            self._load_template(ENC_JINJA_TEMPLATE)
            references["encounter_reference"] = self._transform_encounter(data=encounter, references=references)
            if "diagnoses" in encounter:
                self._process_diagnoses(diagnoses=encounter, source_key="diagnoses", reference=references)

    def _process_resources(self) -> list:
        with open(self._file_path, "rb") as raw_file:
            canonical_data = json.load(raw_file)
            if isinstance(canonical_data, list):
                self._raw_data = benedict(canonical_data[0])
            else:
                self._raw_data = benedict(canonical_data)
            self._update_row_id(self._raw_data.get("row_id"))
            references = {}
            references["organization_reference"] = self._process_practice(practice_data=self._raw_data.get("practice"))
            references["practitioner_reference"] = self._process_practitioners(
                practitioners=self._raw_data, source_key="providers", reference=references
            )
            references["patient_reference"] = self._process_patient(
                patient=self._raw_data.get("patient"), reference=references
            )
            self._process_encounter(encounters=self._raw_data, source_key="encounters", reference=references)
        return self._resource_list


def write_to_file(data: dict, filepath: str):
    with open(filepath, "w") as f:
        json.dump(data, f, indent=4)
    return True


def convert_ccd_to_fhir(data_file_path, bundle_file_path):
    try:
        obj = CcdaToFhir(file_path=data_file_path)
        bundle_data = obj._process_resources()
        get_uuid = str(uuid.uuid4())
        bundle = {"resourceType": "Bundle", "type": "batch", "id": get_uuid, "entry": bundle_data}
        write_to_file(bundle, f"{bundle_file_path}_{get_uuid}.json")
    except Exception as e:
        logging.error(e)
        raise e


if __name__ == "__main__":
    pass
