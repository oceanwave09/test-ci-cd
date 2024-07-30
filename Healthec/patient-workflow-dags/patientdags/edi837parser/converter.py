import json
import logging
import os
import uuid

import smart_open
from benedict import benedict
from fhirtransformer.transformer import FHIRTransformer

from patientdags.edi837parser.constants import (
    CONDITION_CODE_SYSTEM,
    LOCATION_CODE_SYSTEM,
    MODIFIER_CODE_SYSTEM,
    PRODUCT_SERVICE_CODE_SYSTEM,
    TYPE_PAY_CODE,
    TYPE_PAY_DISPLAY,
    TYPE_PROV_CODE,
    TYPE_PROV_DISPLAY,
)
from patientdags.edi837parser.enum import ClaimType, ResourceType
from patientdags.edi837parser.parser import EDI837Parser
from patientdags.edi837parser.utils import parse_date, to_float, update_gender

TEMPLATE_DIR = "templates"
FHIRBUNDLE_DIR = "jobs"

PAT_JINJA_TEMPLATE = "patient.j2"
PRC_JINJA_TEMPLATE = "practitioner.j2"
ORG_JINJA_TEMPLATE = "organization.j2"
CLM_JINJA_TEMPLATE = "claim.j2"
COV_JINJA_TEMPLATE = "coverage.j2"
PROC_JINJA_TEMPLATE = "procedure.j2"
CON_JINJA_TEMPLATE = "condition.j2"
LOC_JINJA_TEMPLATE = "location.j2"


class EDI837ToFhir:
    def __init__(self, file_path: str, claim_type: str = ClaimType.PROFESSIONAL.value, parse: bool = True) -> None:
        self._file_path = file_path
        self._transform = FHIRTransformer()
        self._claim_type = claim_type
        self._parse = parse
        self._meta_info = {}
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
            "request": {
                "method": "PUT",
                "url": f"{data.get('resourceType')}/{entry_uuid}",
            },
        }
        return entry

    def _update_identifier(self, data: dict) -> dict:
        for key, value in data.items():
            if isinstance(value, list):
                for list_data in value:
                    self._update_identifier(list_data)
            elif isinstance(value, dict):
                self._update_identifier(value)
            elif value in self._resource_unique_id:
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
            if source_obj_type == ResourceType.Practitioner.value:
                source_name = "".join(
                    [
                        source_obj.get("firstname", ""),
                        source_obj.get("middleinitials", ""),
                        source_obj.get("lastname", ""),
                    ]
                )
                search_name = "".join(
                    [
                        search_obj.get("firstname", ""),
                        search_obj.get("middleinitials", ""),
                        search_obj.get("lastname", ""),
                    ]
                )
            elif source_obj_type == ResourceType.Organization.value:
                source_name = source_obj.get("name")
                search_name = search_obj.get("name")

            if (source_name and search_name) and (search_name == source_name):
                return source_obj.get("internal_id")
        return None

    def _get_source_identifier(self) -> dict:
        return {
            "source_file_name": self._meta_info.get("file_format", ""),
            "assigner_organization_id": self._meta_info.get("src_organization_id", ""),
        }

    def _is_valid_data(self, data_dict: dict) -> bool:
        for _key in data_dict:
            if data_dict.get(_key):
                return True
        return False

    def _transform_patient(self, data: benedict, references: dict = {}) -> dict:
        resource_type = ResourceType.Patient.value
        data_dict = {}
        data_dict["source_id"] = data.get_str("casualityClaimNumber")
        data_dict["ssn"] = data.get_str("ssn")
        data_dict["member_id"] = data.get_str("id")
        data_dict["city"] = data.get_str("address.city")
        data_dict["country"] = data.get_str("address.country")
        data_dict["zip"] = data.get_str("address.zip")
        data_dict["state"] = data.get_str("address.state")
        data_dict["street_address_1"] = data.get_str("address.line1")
        data_dict["lastname"] = data.get_str("lastName")
        data_dict["firstname"] = data.get_str("firstName")
        data_dict["middleinitials"] = data.get_str("middleName")
        data_dict["dob"] = parse_date(data.get_str("dob"))
        data_dict["gender"] = update_gender(data.get_str("gender"))
        data_dict["phone_mobile"] = data.get_str("phone")
        if self._is_valid_data(data_dict=data_dict):
            data_dict[
                "internal_id"
            ] = f"{resource_type}_{self._row_id}_{self._get_resource_count(resource=resource_type)}"
            data_dict["organization_id"] = references.get("provider_organization_reference")
            if data_dict.get("source_id"):
                data_dict.update(self._get_source_identifier())
            return self._transform_resources(data_dict, resource_type)
        return ""

    def _transform_practice(self, source_key, data: benedict, references: dict = {}) -> dict:
        resource_type = ResourceType.Organization.value
        data_dict = {}
        data_dict["source_id"] = data.get_str("payorId")
        data_dict["tax_id"] = data.get_str("tin")
        if source_key == "payer":
            data_dict["type_code"] = TYPE_PAY_CODE
            data_dict["type_display"] = TYPE_PAY_DISPLAY
            data_dict["name"] = data.get_str("name")
        if source_key == "billingProvider":
            data_dict["npi"] = data.get_str("id")
            data_dict["type_code"] = TYPE_PROV_CODE
            data_dict["type_display"] = TYPE_PROV_DISPLAY
            data_dict["name"] = data.get_str("lastName")
        data_dict["city"] = data.get_str("address.city")
        data_dict["country"] = data.get_str("address.country")
        data_dict["zip"] = data.get_str("address.zip")
        data_dict["state"] = data.get_str("address.state")
        data_dict["street_address_1"] = data.get_str("address.line1")
        if self._is_valid_data(data_dict=data_dict):
            if data_dict.get("source_id"):
                data_dict.update(self._get_source_identifier())
            find_existance = self._compare_resources(source_obj_type=resource_type, search_obj=data_dict)
            if not find_existance:
                data_dict[
                    "internal_id"
                ] = f"{resource_type}_{self._row_id}_{self._get_resource_count(resource=resource_type)}"
                return self._transform_resources(data_dict, resource_type)
            return find_existance
        return ""

    def _transform_practitioner(self, data: benedict, references: dict = {}) -> dict:
        resource_type = ResourceType.Practitioner.value
        data_dict = {}
        data_dict["source_id"] = data.get_str("id")
        data_dict["tax_id"] = data.get_str("tin")
        data_dict["ssn"] = data.get_str("ssn")
        data_dict["state_license"] = data.get_str("stateLicense")
        data_dict["city"] = data.get_str("address.city")
        data_dict["country"] = data.get_str("address.country")
        data_dict["zip"] = data.get_str("address.postal_code")
        data_dict["state"] = data.get_str("address.state")
        data_dict["street_address_1"] = data.get_str("address.line1")
        data_dict["street_address_2"] = data.get_str("address.line2")
        data_dict["lastname"] = data.get_str("lastName")
        data_dict["firstname"] = data.get_str("firstName")
        data_dict["phone_home"] = data.get_str("contact.phone")
        data_dict["email"] = data.get_str("contact.email")
        data_dict["fax"] = data.get_str("contact.fax")
        if self._is_valid_data(data_dict=data_dict):
            if data_dict.get("source_id"):
                data_dict.update(self._get_source_identifier())
            find_existance = self._compare_resources(source_obj_type=resource_type, search_obj=data_dict)
            if not find_existance:
                data_dict[
                    "internal_id"
                ] = f"{resource_type}_{self._row_id}_{self._get_resource_count(resource=resource_type)}"
                return self._transform_resources(data_dict, resource_type)
            return find_existance
        return ""

    def _transform_claim_diagnosis(self, data: benedict, references: dict = {}):
        diagnoses_list = []
        for index, (key, diagnosis) in enumerate(data.get_dict("diagnoses_data_dict").items(), start=1):
            diagnoses_dict = {}
            diagnoses_dict["diagnosis_code"] = diagnosis.get("code")
            diagnoses_dict["sequence"] = index
            diagnoses_dict["diagnosis_condition_id"] = references.get("diagnosis_reference")[index - 1]
            diagnoses_list.append(diagnoses_dict)
        return diagnoses_list

    def _transform_claim_procdure(self, data: benedict, references: dict = {}):
        procedure_list = []
        for index, (key, procedure) in enumerate(data.get_dict("procedure_data_dict").items(), start=1):
            procedure_dict = {}
            procedure_dict["procedure_code"] = procedure.get("code")
            procedure_dict["sequence"] = index
            procedure_dict["procedure_id"] = references.get("procedure_reference")[index - 1]
            procedure_list.append(procedure_dict)
        return procedure_list

    def _transform_claim_care_team(self, references: dict = {}):
        care_team = []
        for index, care_team_entry in enumerate(references.get("practitioner_reference", []), start=1):
            for key, value in care_team_entry.items():
                care_team.append(
                    {
                        "careteam_sequence": index,
                        "careteam_provider_practitioner_id": value[0],
                        "careteam_role_code": key,
                    }
                )
        return care_team

    def _transform_claim_service_lines_modifiers(self, component: dict = {}):
        modifier_code_list = []
        for _index in range(1, 5):
            modifier_code_list.append(
                {
                    "modifier_code": component.get(f"serviceModifier{_index}"),
                    "modifier_system": MODIFIER_CODE_SYSTEM,
                }
            )
        return modifier_code_list

    def _transform_claim_service_lines(self, data: benedict, references: dict = {}):
        service_lines = data.get_list("serviceLines")
        if isinstance(service_lines, list):
            line = []
            support_info_list = []
            for service_line in service_lines:
                entry = {}
                support_info_dict = {}
                support_info_dict["code"] = service_line.get("reportTypeCode")
                support_info_list.append(support_info_dict)

                # To change effectiveDate based on input
                if service_line.get("fromDate"):
                    date = str(service_line.get("fromDate"))
                    if len(date.split("-")) > 0:
                        entry["service_period_start"] = date.split("-")[0].strip()
                    if len(date.split("-")) > 1:
                        entry["service_period_end"] = date.split("-")[1].strip()

                entry["sequence"] = service_line.get("lineNumber", "")
                entry["net_value"] = to_float(service_line.get("lineAmount", ""))
                entry["location_code"] = service_line.get("placeOfService", "")
                if "location_code" in entry:
                    entry["location_system"] = LOCATION_CODE_SYSTEM
                entry["quantity_value"] = to_float(service_line.get("quantity", ""))
                entry["unit_value"] = to_float(service_line.get("unitCode", ""))
                product_service_code = service_line.get("serviceIdCode", "")
                if product_service_code:
                    product_or_service = {
                        "code": product_service_code,
                        "system": PRODUCT_SERVICE_CODE_SYSTEM,
                    }
                    entry["product_or_service"] = [product_or_service]

                entry["svc_modifiers"] = self._transform_claim_service_lines_modifiers(service_line)

            line.append(entry)
            return support_info_list, line

    def _transform_claim(self, data: benedict, references: dict = {}) -> dict:
        resource_type = ResourceType.Claim.value
        data_dict = {}
        data_dict["internal_id"] = f"{resource_type}_{self._row_id}_{self._get_resource_count(resource=resource_type)}"
        data_dict["source_id"] = data.get_str("id")
        data_dict["total_value"] = data.get_str("amount")
        data_dict["patient_paid_amount"] = data.get_str("patientAmountPaid")
        data_dict["diagnoses"] = self._transform_claim_diagnosis(data, references)
        data_dict["procedures"] = self._transform_claim_procdure(data, references)
        data_dict["care_teams"] = self._transform_claim_care_team(references)

        # To change effectiveDate based on input
        if data.get("admissionDate"):
            date = str(data.get("admissionDate"))
            if len(date.split("-")) > 0:
                data_dict["billable_period_start"] = date.split("-")[0].strip()

        if data.get("dischargeDate"):
            date = str(data.get("dischargeDate"))
            if len(date.split("-")) > 1:
                data_dict["billable_period_end"] = date.split("-")[1].strip()

        data_dict["service_lines"] = self._transform_claim_service_lines(data, references)[1]
        data_dict["support_info"] = self._transform_claim_service_lines(data, references)[0]
        data_dict["facility_location_id"] = references.get("location_facility_refernce")
        data_dict["patient_id"] = references.get("patient_reference")
        data_dict["insurer_organization_id"] = references.get("payer_organization_reference")
        data_dict["insurance_coverage_id"] = references.get("coverage_reference")
        data_dict["provider_practitioner_id"] = (
            references.get("provider_practitioner_reference")[0] if "practitioner_reference" in references else ""
        )
        return self._transform_resources(data_dict, resource_type)

    def _transform_condition(self, data: dict, references: dict = {}) -> dict:
        resource_type = ResourceType.Condition.value
        data_dict = {}
        data_dict["code"] = data.get("code")
        if self._is_valid_data(data_dict=data_dict):
            data_dict[
                "internal_id"
            ] = f"{resource_type}_{self._row_id}_{self._get_resource_count(resource=resource_type)}"
            data_dict["code_system"] = CONDITION_CODE_SYSTEM
            data_dict["patient_id"] = references.get("patient_reference")
            return self._transform_resources(data_dict, resource_type)
        return ""

    def _transform_procedure(self, data: dict, references: dict = {}) -> dict:
        resource_type = ResourceType.Procedure.value
        data_dict = {}
        data_dict["code"] = data.get("code")
        if self._is_valid_data(data_dict=data_dict):
            data_dict[
                "internal_id"
            ] = f"{resource_type}_{self._row_id}_{self._get_resource_count(resource=resource_type)}"
            data_dict["patient_id"] = references.get("patient_reference") if "patient_reference" in references else ""
            return self._transform_resources(data_dict, resource_type)
        return ""

    def _transform_coverage(self, data: benedict, references: dict = {}) -> dict:
        resource_type = ResourceType.Coverage.value
        data_dict = {}
        data_dict["member_id"] = data.get_str("id")
        if self._is_valid_data(data_dict=data_dict):
            if data_dict.get("source_id"):
                data_dict.update(self._get_source_identifier())
            data_dict[
                "internal_id"
            ] = f"{resource_type}_{self._row_id}_{self._get_resource_count(resource=resource_type)}"
            data_dict["subscriber_patient_id"] = references.get("patient_sub_reference")
            data_dict["beneficiary_patient_id"] = references.get("patient_reference")
            data_dict["insurer_organization_id"] = references.get("payer_organization_reference")
            return self._transform_resources(data_dict, resource_type)
        return ""

    def _transform_location(self, data: benedict, references: dict = {}) -> dict:
        resource_type = ResourceType.Location.value
        data_dict = {}
        data_dict["source_id"] = data.get_str("faclity")
        if self._is_valid_data(data_dict=data_dict):
            if data_dict.get("source_id"):
                data_dict.update(self._get_source_identifier())
            data_dict[
                "internal_id"
            ] = f"{resource_type}_{self._row_id}_{self._get_resource_count(resource=resource_type)}"
            return self._transform_resources(data_dict, resource_type)
        return ""

    def _process_patient(self, patient: benedict, source_key: str = None, resource_ref: dict = None) -> dict:
        self._load_template(PAT_JINJA_TEMPLATE)
        data = patient
        if source_key:
            data = patient.get(source_key)
        return self._transform_patient(data=data, references=resource_ref)

    def _process_diagnoses(self, diagnoses: dict, source_key: str = None, resource_ref: dict = None) -> list:
        self._load_template(CON_JINJA_TEMPLATE)
        diagnoses_ref = []
        for code, diagnosis in diagnoses.items():
            diagnoses_ref.append(self._transform_condition(data=diagnosis, references=resource_ref))
        return diagnoses_ref

    def _process_procedure(self, procedures: dict, source_key: str = None, resource_ref: dict = None) -> list:
        self._load_template(PROC_JINJA_TEMPLATE)
        procedures_ref = []
        for code, procedure in procedures.items():
            procedures_ref.append(self._transform_procedure(data=procedure, references=resource_ref))
        return procedures_ref

    def _process_practice(self, practice_data: benedict, source_key: str = None, resource_ref: dict = None) -> dict:
        self._load_template(ORG_JINJA_TEMPLATE)
        data = practice_data
        if source_key:
            data = practice_data.get(source_key)
        return self._transform_practice(source_key, data=data, references=resource_ref)

    def _process_coverage(
        self,
        coverage: benedict = None,
        source_key: str = None,
        resource_ref: dict = None,
    ) -> dict:
        references = {}
        self._load_template(COV_JINJA_TEMPLATE)
        data = coverage
        if source_key:
            data = coverage.get(source_key)
        references["coverage_reference"] = self._transform_coverage(data=data, references=resource_ref)
        return references["coverage_reference"]

    def _process_location(self, locations: benedict, source_key: str = None, resource_ref: dict = None) -> list:
        self._load_template(LOC_JINJA_TEMPLATE)
        location_ref = []
        location = locations.get(source_key)
        if isinstance(location, dict):
            location_ref.append(self._transform_location(data=location, references=resource_ref))
        elif isinstance(location, list):
            get_locations = len(location)
            for _index in range(get_locations):
                location = locations.get(f"{source_key}[{_index}]")
                get_reference_id = self._transform_location(data=location, references=resource_ref)
                if get_reference_id not in location_ref:
                    location_ref.append(get_reference_id)
        return location_ref

    def _process_practitioners(self, practitioners: benedict, source_key: str = None, resource_ref: dict = None) -> list:
        self._load_template(PRC_JINJA_TEMPLATE)
        practitioner_ref = []
        practitioner = practitioners.get(source_key)
        if isinstance(practitioner, dict):
            practitioner_ref.append(self._transform_practitioner(data=practitioner, references=resource_ref))
        elif isinstance(practitioner, list):
            get_practitioners = len(practitioner)
            for _index in range(get_practitioners):
                practitioner = practitioners.get(f"{source_key}[{_index}]")
                get_reference_id = self._transform_practitioner(data=practitioner, references=resource_ref)
                if get_reference_id not in practitioner_ref:
                    practitioner_ref.append(get_reference_id)
        return practitioner_ref

    def _process_claim_diagnosis(self, claim):
        diagnoses_data_dict = {}
        if "diagnosisCodes" in claim and claim["diagnosisCodes"]:
            for diagnosis_code in claim["diagnosisCodes"]:
                if diagnosis_code["code"] not in diagnoses_data_dict:
                    diagnoses_data_dict[diagnosis_code["code"]] = diagnosis_code

        return diagnoses_data_dict

    def _process_claim_procedures(self, claim):
        procedure_data_dict = {}
        if "anesthesiaRelatedSurgicalProcedureCode" in claim and claim["anesthesiaRelatedSurgicalProcedureCode"]:
            for procedure_code in claim["anesthesiaRelatedSurgicalProcedureCode"]:
                if procedure_code["code"] not in procedure_data_dict:
                    procedure_data_dict[procedure_code["code"]] = procedure_code

        return procedure_data_dict

    def _process_care_team_practitioner(self, claim, references):
        references["practitioner_reference"] = []
        if "refferingProvider" in claim:
            references["practitioner_reference"].append(
                {"reffering": self._process_practitioners(practitioners=claim, source_key="refferingProvider")}
            )

        if "renderingProvider" in claim:
            references["practitioner_reference"].append(
                {"rendering": self._process_practitioners(practitioners=claim, source_key="renderingProvider")}
            )

        if "supervisingProvider" in claim:
            references["practitioner_reference"].append(
                {"supervising": self._process_practitioners(practitioners=claim, source_key="supervisingProvider")}
            )

        if "operatingProvider" in claim:
            references["practitioner_reference"].append(
                {"operating": self._process_practitioners(practitioners=claim, source_key="operatingProvider")}
            )

        return references

    def _process_claim(self, claims: list = None, source_key: str = None, resource_ref: dict = None) -> list:
        get_claim_len = len(claims.get(source_key))
        clm_reference = []
        for _index in range(get_claim_len):
            references = {}
            claim = claims.get(f"{source_key}[{_index}]")
            diagnoses_data_dict = self._process_claim_diagnosis(claim)
            claim["diagnoses_data_dict"] = diagnoses_data_dict

            procedure_data_dict = self._process_claim_procedures(claim)
            claim["procedure_data_dict"] = procedure_data_dict
            references.update(resource_ref)

            if diagnoses_data_dict:
                references["diagnosis_reference"] = self._process_diagnoses(
                    diagnoses=diagnoses_data_dict,
                    resource_ref=resource_ref,
                )

            if procedure_data_dict:
                references["procedure_reference"] = self._process_procedure(
                    procedures=procedure_data_dict,
                    resource_ref=resource_ref,
                )

            if "serviceFacility" in claim:
                references["location_facility_refernce"] = self._process_location(
                    locations=claim, source_key="serviceFacility"
                )

            references = self._process_care_team_practitioner(claim, references)
            references["coverage_reference"] = self._process_coverage(
                coverage=self._raw_data,
                source_key="subscriber",
                resource_ref=references,
            )

            self._load_template(CLM_JINJA_TEMPLATE)
            references["claim_reference"] = self._transform_claim(data=claim, references=references)
            clm_reference.append(references["claim_reference"])

    def _process_resources(self, file_path) -> list:
        with open(file_path, "rb") as raw_file:
            raw_data_list = json.load(raw_file)

            for data in raw_data_list:
                self._raw_data = benedict(data)
                self._update_row_id(self._raw_data.get("row_id"))
                references = {}
                if "payer" in self._raw_data:
                    references["payer_organization_reference"] = self._process_practice(
                        practice_data=self._raw_data, source_key="payer"
                    )
                if "billingProvider" in self._raw_data:
                    provider_data = self._raw_data.get("billingProvider")
                    if "lastName" and "firstName" in provider_data:
                        references["billing_provider_practitioner_reference"] = self._process_practitioners(
                            practitioners=self._raw_data,
                            source_key="billingProvider",
                            resource_ref=references,
                        )
                    else:
                        references["provider_organization_reference"] = self._process_practice(
                            practice_data=self._raw_data, source_key="billingProvider"
                        )
                if "receiver" in self._raw_data:
                    references["provider_practitioner_reference"] = self._process_practitioners(
                        practitioners=self._raw_data,
                        source_key="receiver",
                        resource_ref=references,
                    )
                references["patient_reference"] = self._process_patient(
                    patient=self._raw_data, source_key="patient", resource_ref=references
                )
                references["patient_sub_reference"] = self._process_patient(
                    patient=self._raw_data, source_key="subscriber", resource_ref=references
                )
                if "claims" in self._raw_data:
                    self._process_claim(claims=self._raw_data, source_key="claims", resource_ref=references)

        return self._resource_list

    def _write_file(self, data: dict, file_path: str, metadata: str = ""):
        # with smart_open.open(file_path, "w") as f:
        #     json.dump(data, f, indent=2 * " ")
        if metadata:
            # add metadata to target S3 file
            transport_params = {
                "client_kwargs": {"S3.Client.create_multipart_upload": {"Metadata": json.loads(metadata)}}
            }
            dest_file = smart_open.open(file_path, "w", transport_params=transport_params)
        else:
            dest_file = smart_open.open(file_path, "w")
        json.dump(data, dest_file, indent=2)

    def convert(self, output_file: str, metadata: dict = {}):
        try:
            if self._parse:
                edi837_parser = EDI837Parser(self._file_path, self._claim_type)
                parsed_file = edi837_parser.parse()
            else:
                parsed_file = self._file_path
            self._meta_info = metadata
            resources = self._process_resources(parsed_file)
            bundle_id = str(uuid.uuid4())
            bundle = {"resourceType": "Bundle", "type": "batch", "id": bundle_id, "entry": resources}
            if metadata:
                self._write_file(bundle, output_file, json.dumps(metadata))
            else:
                self._write_file(bundle, output_file)
            return output_file
        except Exception as e:
            logging.error("Failed to convert EDI 837 into FHIR. Error:", str(e))
            raise e
        finally:
            if parsed_file.startswith("/tmp/") and os.path.exists(parsed_file):
                os.unlink(parsed_file)
