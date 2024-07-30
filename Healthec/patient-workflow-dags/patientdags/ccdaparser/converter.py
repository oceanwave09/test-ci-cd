import json
import logging
import os
import uuid

import smart_open
from benedict import benedict

from patientdags.ccdaparser.enum import ResourceType
from patientdags.ccdaparser.parser import CcdaParser
from patientdags.ccdaparser.transformer.allergy_intolerance import AllergyIntolerance
from patientdags.ccdaparser.transformer.base import Base
from patientdags.ccdaparser.transformer.condition import Condition
from patientdags.ccdaparser.transformer.coverage import Coverage
from patientdags.ccdaparser.transformer.encounter import Encounter
from patientdags.ccdaparser.transformer.immunization import Immunization
from patientdags.ccdaparser.transformer.insurance_plan import InsurancePlan
from patientdags.ccdaparser.transformer.medication import Medication
from patientdags.ccdaparser.transformer.medication_request import MedicationRequest
from patientdags.ccdaparser.transformer.observation import Observation
from patientdags.ccdaparser.transformer.organization import Organization
from patientdags.ccdaparser.transformer.patient import Patient
from patientdags.ccdaparser.transformer.practitioner import Practitioner
from patientdags.ccdaparser.transformer.procedure import Procedure
from patientdags.ccdaparser.utils import parse_date, parse_gender


class CcdaToFhir:
    def __init__(self, file_path: str, parse_file_path: str = "", parse: bool = True) -> None:
        self._file_path = file_path
        self._parse_file_path = parse_file_path
        self._parse = parse
        self._metadata = {}
        self._reset_attriutes()

    def _reset_attriutes(self) -> None:
        self._res_list = list()
        self._res_count = dict()
        self._res_reference = dict()
        self._res_data = dict()

    def _add_resource_count(self, res_type: str) -> int:
        if res_type in self._res_count:
            self._res_count[res_type] = self._res_count[res_type] + 1
        else:
            self._res_count[res_type] = 1
        return self._res_count[res_type]

    def _add_resource_reference(self, res_type: str, res_id: str) -> None:
        if res_type in self._res_reference:
            self._res_reference[res_type].append(res_id)
        else:
            self._res_reference[res_type] = [res_id]

    def _add_resource_list(self, res: dict) -> None:
        self._res_list.append(res)

    def _add_resource_data(self, res_type: str, res_data: dict) -> None:
        if res_type in self._res_data:
            self._res_data[res_type].append(res_data)
        else:
            self._res_data[res_type] = [res_data]

    def _check_resource_existence(self, res_type: str, res_data: dict) -> str:
        if res_type not in [
            ResourceType.Organization.value,
            ResourceType.Practitioner.value,
            ResourceType.Encounter.value,
            ResourceType.Patient.value,
            ResourceType.Medication.value,
        ]:
            return ""
        if not self._res_data.get(res_type):
            return ""
        for src_res_data in self._res_data.get(res_type):
            if res_type == ResourceType.Practitioner.value:
                src_param = "|".join(
                    [
                        src_res_data.get("firstname", ""),
                        src_res_data.get("middleinitials", ""),
                        src_res_data.get("lastname", ""),
                    ]
                )
                srch_param = "|".join(
                    [
                        res_data.get("firstname", ""),
                        res_data.get("middleinitials", ""),
                        res_data.get("lastname", ""),
                    ]
                )
            elif res_type == ResourceType.Organization.value:
                src_param = src_res_data.get("name", "")
                srch_param = res_data.get("name", "")
            elif res_type == ResourceType.Encounter.value:
                src_param = "|".join([src_res_data.get("source_id", ""), src_res_data.get("source_system", "")])
                srch_param = "|".join([res_data.get("source_id", ""), res_data.get("source_system", "")])
            elif res_type == ResourceType.Patient.value:
                src_param = "|".join(
                    [
                        src_res_data.get("firstname", ""),
                        src_res_data.get("middleinitials", ""),
                        src_res_data.get("lastname", ""),
                        src_res_data.get("dob", ""),
                        src_res_data.get("gender", ""),
                    ]
                )
                srch_param = "|".join(
                    [
                        res_data.get("firstname", ""),
                        res_data.get("middleinitials", ""),
                        res_data.get("lastname", ""),
                        res_data.get("dob", ""),
                        res_data.get("gender", ""),
                    ]
                )
            elif res_type == ResourceType.Medication.value:
                src_param = "|".join([src_res_data.get("code", ""), src_res_data.get("code_system", "")])
                srch_param = "|".join([res_data.get("code", ""), res_data.get("code_system", "")])
            if (src_param and srch_param) and (srch_param == src_param):
                return src_res_data.get("internal_id")
        return ""

    def _check_valid_practitioner(self, data: benedict) -> bool:
        valid_practitioner = False
        # check identifier
        valid_identifier = False
        identifiers = data.get("identifier", [])
        for identifier in identifiers:
            if "2.16.840.1.113883.4.6" in identifier.get("system") and identifier.get("value"):
                valid_identifier = True
                break
        # check name
        valid_name = False
        names = data.get("name", [])
        for name in names:
            if name.get("family") and name.get("given", []):
                valid_name = True
                break
        if valid_identifier and valid_name:
            valid_practitioner = True
        return valid_practitioner

    def _parse_name(self, data: benedict) -> dict:
        data_dict = {}
        data_dict["lastname"] = data.get_str("name[0].family")
        data_dict["firstname"] = data.get_str("name[0].given[0]")
        data_dict["middleinitials"] = data.get_str("name[0].given[1]")
        data_dict["dob"] = parse_date(data.get_str("birth_date"))
        data_dict["gender"] = parse_gender(data.get_str("gender.display"))
        return data_dict

    def _parse_source_id(self, data: benedict) -> dict:
        data_dict = {}
        identifiers = data.get_list("identifier")
        if len(identifiers) > 0 and identifiers[0]:
            data_dict["source_id"] = identifiers[0].get_str("value")
            data_dict["source_system"] = identifiers[0].get_str("system")
        return data_dict

    def _write_file(self, data: dict, file_path: str):
        if self._metadata:
            # add metadata to target S3 file
            transport_params = {"client_kwargs": {"S3.Client.create_multipart_upload": {"Metadata": self._metadata}}}
            dest_file = smart_open.open(file_path, "w", transport_params=transport_params)
        else:
            dest_file = smart_open.open(file_path, "w")
        json.dump(data, dest_file, indent=2)

    def _transform_resource(self, res_type: str, transformer: Base) -> str:
        self._add_resource_count(res_type)
        res_data = transformer.build()
        res_id = self._check_resource_existence(res_type, res_data)
        if not res_id:
            res = transformer.transform()
            res_id = res_data.get("internal_id")
            self._add_resource_list(res)
            self._add_resource_reference(res_type, res_id)
            self._add_resource_data(res_type, res_data)
        return res_id

    def _process_practice(self, data: benedict):
        if self._metadata.get("src_organization_id", ""):
            return self._metadata.get("src_organization_id", "")
        res_type = ResourceType.Organization.value
        transformer = Organization(data, type="prov", references=self._res_reference, metadata=self._metadata)
        return self._transform_resource(res_type, transformer)

    def _process_provider(self, data: benedict):
        res_type = ResourceType.Practitioner.value
        transformer = Practitioner(data, references=self._res_reference, metadata=self._metadata)
        return self._transform_resource(res_type, transformer)

    def _process_patient(self, data: benedict):
        res_type = ResourceType.Patient.value
        transformer = Patient(data, references=self._res_reference, metadata=self._metadata)
        return self._transform_resource(res_type, transformer)

    def _process_encounter(self, data: benedict, prc_id: str = ""):
        res_type = ResourceType.Encounter.value
        transformer = Encounter(data, references=self._res_reference, metadata=self._metadata, practitioner_id=prc_id)
        return self._transform_resource(res_type, transformer)

    def _process_encounter_diagnosis(self, data: benedict, enc_id: str = ""):
        # skip creating condition resource if code does not exists
        if not data.get_str("code.code") and not data.get_str("code.translation[0].code"):
            return ""
        res_type = ResourceType.Condition.value
        transformer = Condition(data, references=self._res_reference, metadata=self._metadata, encounter_id=enc_id)
        return self._transform_resource(res_type, transformer)

    def _process_allergy(self, data: benedict, prc_id: str = ""):
        # skip creating allergy resource if code does not exists
        if not data.get_str("code.code") and not data.get_str("code.translation[0].code"):
            return ""
        res_type = ResourceType.AllergyIntolerance.value
        transformer = AllergyIntolerance(
            data, references=self._res_reference, metadata=self._metadata, practitioner_id=prc_id
        )
        return self._transform_resource(res_type, transformer)

    def _process_problem(self, data: benedict, prc_id: str = ""):
        # skip creating condition resource if code does not exists
        if not data.get_str("code.code") and not data.get_str("code.translation[0].code"):
            return ""
        res_type = ResourceType.Condition.value
        transformer = Condition(data, references=self._res_reference, metadata=self._metadata, practitioner_id=prc_id)
        return self._transform_resource(res_type, transformer)

    def _process_past_medical_history(self, data: benedict, prc_id: str = ""):
        # skip creating condition resource if code does not exists
        if not data.get_str("code.code") and not data.get_str("code.translation[0].code"):
            return ""
        res_type = ResourceType.Condition.value
        transformer = Condition(data, references=self._res_reference, metadata=self._metadata, practitioner_id=prc_id)
        return self._transform_resource(res_type, transformer)

    def _process_payer(self, data: benedict):
        res_type = ResourceType.Organization.value
        transformer = Organization(data, type="pay", references=self._res_reference, metadata=self._metadata)
        return self._transform_resource(res_type, transformer)

    def _process_plan(self, data: benedict):
        res_type = ResourceType.InsurancePlan.value
        transformer = InsurancePlan(data, references=self._res_reference, metadata=self._metadata)
        return self._transform_resource(res_type, transformer)

    def _process_coverage(self, data: benedict, pat_id: str = "", pay_id: str = "", plan_id: str = ""):
        res_type = ResourceType.Coverage.value
        transformer = Coverage(
            data,
            references=self._res_reference,
            metadata=self._metadata,
            beneficiary_id=pat_id,
            insurer_id=pay_id,
            insurance_plan_id=plan_id,
        )
        return self._transform_resource(res_type, transformer)

    def _process_immunization(self, data: benedict, prc_id: str = "", enc_id: str = ""):
        # skip creating immunization resource if code does not exists
        if not data.get_str("vaccine_code.code") and not data.get_str("vaccine_code.translation[0].code"):
            return ""
        res_type = ResourceType.Immunization.value
        transformer = Immunization(
            data, references=self._res_reference, metadata=self._metadata, practitioner_id=prc_id, encounter_id=enc_id
        )
        return self._transform_resource(res_type, transformer)

    def _process_procedure(self, data: benedict, prc_id: str = "", enc_id: str = ""):
        # skip creating procedure resource if code does not exists
        if not data.get_str("code.code") and not data.get_str("code.translation[0].code"):
            return ""
        res_type = ResourceType.Procedure.value
        transformer = Procedure(
            data, references=self._res_reference, metadata=self._metadata, practitioner_id=prc_id, encounter_id=enc_id
        )
        return self._transform_resource(res_type, transformer)

    def _process_social_history(self, data: benedict):
        # skip creating observation resource if code does not exists
        if not data.get_str("code.code") and not data.get_str("code.translation[0].code"):
            return ""
        res_type = ResourceType.Observation.value
        transformer = Observation(data, references=self._res_reference, metadata=self._metadata)
        return self._transform_resource(res_type, transformer)

    def _process_vital_sign(self, data: benedict):
        # skip creating observation resource if component code does not exists
        if not data.get("component", []):
            return ""
        final_components = []
        for entry in data.get("component", []):
            if entry.get_str("code.code") or entry.get_str("code.translation[0].code"):
                final_components.append(entry)
        data["component"] = final_components
        if not data.get("component", []):
            return ""
        res_type = ResourceType.Observation.value
        transformer = Observation(data, references=self._res_reference, metadata=self._metadata)
        return self._transform_resource(res_type, transformer)

    def _process_result(self, data: benedict):
        # skip creating observation resource if code and component code do not exists
        if not data.get("component", []):
            return ""
        final_components = []
        for entry in data.get("component", []):
            if entry.get_str("code.code") or entry.get_str("code.translation[0].code"):
                final_components.append(entry)
        data["component"] = final_components
        if (
            not data.get("component", [])
            and not data.get_str("code.code")
            and not data.get_str("code.translation[0].code")
        ):
            return ""
        res_type = ResourceType.Observation.value
        transformer = Observation(data, references=self._res_reference, metadata=self._metadata)
        return self._transform_resource(res_type, transformer)

    def _process_medication(self, data: benedict):
        # skip creating medication resource if code does not exists
        if not data.get_str("code.code") and not data.get_str("code.translation[0].code"):
            return ""
        res_type = ResourceType.Medication.value
        transformer = Medication(data, references=self._res_reference, metadata=self._metadata)
        return self._transform_resource(res_type, transformer)

    def _process_medication_request(self, data: benedict, med_id: str = "", prc_id: str = ""):
        # skip creating medication request if med_id does not exists
        if not med_id:
            return ""
        res_type = ResourceType.MedicationRequest.value
        transformer = MedicationRequest(
            data, references=self._res_reference, metadata=self._metadata, medication_id=med_id, practitioner_id=prc_id
        )
        return self._transform_resource(res_type, transformer)

    def _process_planned_procedure(self, data: benedict):
        # skip creating procedure resource if code does not exists
        if not data.get_str("code.code") and not data.get_str("code.translation[0].code"):
            return ""
        res_type = ResourceType.Procedure.value
        transformer = Procedure(data, references=self._res_reference, metadata=self._metadata)
        return self._transform_resource(res_type, transformer)

    def _process_planned_observation(self, data: benedict):
        # skip creating observation resource if code does not exists
        if not data.get_str("code.code") and not data.get_str("code.translation[0].code"):
            return ""
        res_type = ResourceType.Observation.value
        transformer = Observation(data, references=self._res_reference, metadata=self._metadata)
        return self._transform_resource(res_type, transformer)

    def _process_planned_immunization(self, data: benedict):
        # skip creating immunization resource if code does not exists
        if not data.get_str("vaccine_code.code") and not data.get_str("vaccine_code.translation[0].code"):
            return ""
        res_type = ResourceType.Immunization.value
        transformer = Immunization(data, references=self._res_reference, metadata=self._metadata)
        return self._transform_resource(res_type, transformer)

    def _process_planned_encounter(self, data: benedict):
        # skip creating condition resource if code does not exists
        if not data.get_str("code.code") and not data.get_str("code.translation[0].code"):
            return ""
        res_type = ResourceType.Condition.value
        transformer = Condition(data, references=self._res_reference, metadata=self._metadata)
        return self._transform_resource(res_type, transformer)

    def _process_planned_medication(self, data: benedict):
        res_type = ResourceType.MedicationRequest.value
        transformer = MedicationRequest(data, references=self._res_reference, metadata=self._metadata)
        return self._transform_resource(res_type, transformer)

    def _process_resources(self, file_path: str) -> list:
        with smart_open.open(file_path, "rb") as file:
            data = json.load(file)
            if isinstance(data, list):
                self._bene_data = benedict(data[0])
            else:
                self._bene_data = benedict(data)
            self._row_id = self._bene_data.get("row_id")

            # process practice
            if self._bene_data.get("practice"):
                data = self._bene_data.get("practice")
                if isinstance(data, dict):
                    self._process_practice(data)
                elif isinstance(data, list):
                    for entry in data:
                        self._process_practice(entry)

            # process providers
            if self._bene_data.get("providers"):
                data = self._bene_data.get("providers")
                if isinstance(data, dict):
                    self._process_provider(data)
                elif isinstance(data, list):
                    for entry in data:
                        self._process_provider(entry)

            # process patient
            if self._bene_data.get("patient"):
                data = self._bene_data.get("patient")
                if isinstance(data, dict):
                    self._process_patient(data)
                elif isinstance(data, list):
                    for entry in data:
                        self._process_patient(entry)

            # process encounters
            if self._bene_data.get("encounters"):
                data = self._bene_data.get("encounters")
                if isinstance(data, dict):
                    # resolve practitioner
                    prc_id = ""
                    performers = data.get("performer", [])
                    for performer in performers:
                        if performer:
                            prc_dict = self._parse_name(performer)
                            prc_id = self._check_resource_existence(ResourceType.Practitioner.value, prc_dict)
                        if not prc_id and self._check_valid_practitioner(performer):
                            prc_id = self._process_provider(performer)
                        if prc_id:
                            break
                    enc_id = self._process_encounter(data, prc_id)
                    diagnoses = data.get("diagnoses", [])
                    for diagnosis in diagnoses:
                        self._process_encounter_diagnosis(diagnosis, enc_id)
                elif isinstance(data, list):
                    for entry in data:
                        # resolve practitioner
                        prc_id = ""
                        performers = entry.get("performer", [])
                        for performer in performers:
                            if performer:
                                prc_dict = self._parse_name(performer)
                                prc_id = self._check_resource_existence(ResourceType.Practitioner.value, prc_dict)
                            if not prc_id and self._check_valid_practitioner(performer):
                                prc_id = self._process_provider(performer)
                            if prc_id:
                                break
                        enc_id = self._process_encounter(entry, prc_id)
                        diagnoses = entry.get("diagnoses", [])
                        for diagnosis in diagnoses:
                            self._process_encounter_diagnosis(diagnosis, enc_id)

            # process allergies
            if self._bene_data.get("allergies"):
                data = self._bene_data.get("allergies")
                if isinstance(data, dict):
                    prc_id = ""
                    asserter = data.get("asserter", {})
                    if asserter:
                        prc_dict = self._parse_name(asserter)
                        prc_id = self._check_resource_existence(ResourceType.Practitioner.value, prc_dict)
                        if not prc_id and self._check_valid_practitioner(asserter):
                            prc_id = self._process_provider(asserter)
                    self._process_allergy(data, prc_id)
                elif isinstance(data, list):
                    for entry in data:
                        prc_id = ""
                        asserter = entry.get("asserter", {})
                        if asserter:
                            prc_dict = self._parse_name(asserter) if asserter else ""
                            prc_id = self._check_resource_existence(ResourceType.Practitioner.value, prc_dict)
                            if not prc_id and self._check_valid_practitioner(asserter):
                                prc_id = self._process_provider(asserter)
                        self._process_allergy(entry, prc_id)

            # process problems
            if self._bene_data.get("problems"):
                data = self._bene_data.get("problems")
                if isinstance(data, dict):
                    prc_id = ""
                    performer = data.get("performer", {})
                    if performer:
                        prc_dict = self._parse_name(performer)
                        prc_id = self._check_resource_existence(ResourceType.Practitioner.value, prc_dict)
                        if not prc_id and self._check_valid_practitioner(performer):
                            prc_id = self._process_provider(performer)
                    self._process_problem(data, prc_id)
                elif isinstance(data, list):
                    for entry in data:
                        prc_id = ""
                        performer = entry.get("performer", {})
                        if performer:
                            prc_dict = self._parse_name(performer) if performer else ""
                            prc_id = self._check_resource_existence(ResourceType.Practitioner.value, prc_dict)
                            if not prc_id and self._check_valid_practitioner(performer):
                                prc_id = self._process_provider(performer)
                        self._process_problem(entry, prc_id)

            # process past medical history
            if self._bene_data.get("past_medical_histories"):
                data = self._bene_data.get("past_medical_histories")
                if isinstance(data, dict):
                    prc_id = ""
                    performer = data.get("performer", {})
                    if performer:
                        prc_dict = self._parse_name(performer)
                        prc_id = self._check_resource_existence(ResourceType.Practitioner.value, prc_dict)
                        if not prc_id and self._check_valid_practitioner(performer):
                            prc_id = self._process_provider(performer)
                    self._process_past_medical_history(data, prc_id)
                elif isinstance(data, list):
                    for entry in data:
                        prc_id = ""
                        performer = entry.get("performer", {})
                        if performer:
                            prc_dict = self._parse_name(performer) if performer else ""
                            prc_id = self._check_resource_existence(ResourceType.Practitioner.value, prc_dict)
                            if not prc_id and self._check_valid_practitioner(performer):
                                prc_id = self._process_provider(performer)
                        self._process_past_medical_history(entry, prc_id)

            # TODO: understand payer section existing mapping
            # # process coverages
            # if self._bene_data.get("coverages"):
            #     data = self._bene_data.get("coverages")
            #     if isinstance(data, dict):
            #         # process payer
            #         pay_id = ""
            #         payer_data = data.get("payer")
            #         if payer_data:
            #             pay_id = self._process_payer(payer_data)
            #         # process insurance plan
            #         plan_id = ""
            #         plan_data = data.get("plan")
            #         if plan_data:
            #             plan_id = self._process_plan(plan_data)
            #         # resolve beneficiary(patient)
            #         pat_id = ""
            #         patient_data = data.get("beneficiary")
            #         if patient_data:
            #             pat_dict = self._parse_name(patient_data)
            #             pat_id = self._check_resource_existence(ResourceType.Patient.value, pat_dict)
            #         self._process_coverage(data, pat_id, pay_id, plan_id)
            #     elif isinstance(data, list):
            #         for entry in data:
            #             # process payer
            #             pay_id = ""
            #             payer_data = entry.get("payer")
            #             if payer_data:
            #                 pay_id = self._process_payer(payer_data)
            #             # process insurance plan
            #             plan_id = ""
            #             plan_data = entry.get("plan")
            #             if plan_data:
            #                 plan_id = self._process_plan(plan_data)
            #             # resolve beneficiary(patient)
            #             pat_id = ""
            #             patient_data = entry.get("beneficiary")
            #             if patient_data:
            #                 pat_dict = self._parse_name(patient_data)
            #                 pat_id = self._check_resource_existence(ResourceType.Patient.value, pat_dict)
            #             self._process_coverage(entry, pat_id, pay_id, plan_id)

            # process immunizations
            if self._bene_data.get("immunizations"):
                data = self._bene_data.get("immunizations")
                if isinstance(data, dict):
                    # resolve practitioner
                    prc_id = ""
                    performers = data.get("performer", [])
                    for performer in performers:
                        if performer:
                            prc_dict = self._parse_name(performer)
                            prc_id = self._check_resource_existence(ResourceType.Practitioner.value, prc_dict)
                        if not prc_id and self._check_valid_practitioner(performer):
                            prc_id = self._process_provider(performer)
                        if prc_id:
                            break
                    self._process_immunization(data, prc_id)
                elif isinstance(data, list):
                    for entry in data:
                        # resolve practitioner
                        prc_id = ""
                        performers = entry.get("performer", [])
                        for performer in performers:
                            if performer:
                                prc_dict = self._parse_name(performer)
                                prc_id = self._check_resource_existence(ResourceType.Practitioner.value, prc_dict)
                            if not prc_id and self._check_valid_practitioner(performer):
                                prc_id = self._process_provider(performer)
                            if prc_id:
                                break
                        self._process_immunization(entry, prc_id)

            # process procedures
            if self._bene_data.get("procedures"):
                data = self._bene_data.get("procedures")
                if isinstance(data, dict):
                    # resolve practitioner
                    prc_id = ""
                    performers = data.get("performer", [])
                    for performer in performers:
                        if performer:
                            prc_dict = self._parse_name(performer)
                            prc_id = self._check_resource_existence(ResourceType.Practitioner.value, prc_dict)
                        if not prc_id and self._check_valid_practitioner(performer):
                            prc_id = self._process_provider(performer)
                        if prc_id:
                            break
                    # resolve encounter
                    enc_id = ""
                    encounters = data.get("encounter", [])
                    for encounter in encounters:
                        if encounter:
                            enc_dict = self._parse_source_id(encounter)
                            enc_id = self._check_resource_existence(ResourceType.Encounter.value, enc_dict)
                        if enc_id:
                            break
                    self._process_procedure(data, prc_id, enc_id)
                elif isinstance(data, list):
                    for entry in data:
                        # resolve practitioner
                        prc_id = ""
                        performers = entry.get("performer", [])
                        for performer in performers:
                            if performer:
                                prc_dict = self._parse_name(performer)
                                prc_id = self._check_resource_existence(ResourceType.Practitioner.value, prc_dict)
                            if not prc_id and self._check_valid_practitioner(performer):
                                prc_id = self._process_provider(performer)
                            if prc_id:
                                break
                        # resolve encounter
                        enc_id = ""
                        encounters = entry.get("encounter", [])
                        for encounter in encounters:
                            if encounter:
                                enc_dict = self._parse_source_id(encounter)
                                enc_id = self._check_resource_existence(ResourceType.Encounter.value, enc_dict)
                            if enc_id:
                                break
                        self._process_procedure(entry, prc_id, enc_id)

            # process social histories
            if self._bene_data.get("social_histories"):
                data = self._bene_data.get("social_histories")
                if isinstance(data, dict):
                    self._process_social_history(data)
                elif isinstance(data, list):
                    for entry in data:
                        self._process_social_history(entry)

            # process vital signs
            if self._bene_data.get("vital_signs"):
                data = self._bene_data.get("vital_signs")
                if isinstance(data, dict):
                    self._process_vital_sign(data)
                elif isinstance(data, list):
                    for entry in data:
                        self._process_vital_sign(entry)

            # process results
            if self._bene_data.get("results"):
                data = self._bene_data.get("results")
                if isinstance(data, dict):
                    self._process_result(data)
                elif isinstance(data, list):
                    for entry in data:
                        self._process_result(entry)

            # TODO: understand plan of treatment section existing mapping
            # # process plan of treatments
            # if self._bene_data.get("plan_of_treatments"):
            #     data = self._bene_data.get("plan_of_treatments")
            #     if isinstance(data, dict):
            #         # process planned procedures
            #         procedures = data.get("planned_procedure", [])
            #         for procedure in procedures:
            #             self._process_planned_procedure(procedure)
            #         # process planned encounters
            #         encounters = data.get("planned_encounter", [])
            #         for encounter in encounters:
            #             self._process_planned_encounter(encounter)
            #         # process planned observations
            #         observations = data.get("planned_observation", [])
            #         for observation in observations:
            #             self._process_planned_observation(observation)
            #         # process planned immunizations
            #         immunizations = data.get("planned_immunization", [])
            #         for immunization in immunizations:
            #             self._process_planned_immunization(immunization)
            #         # process planned medications
            #         medications = data.get("planned_medication", [])
            #         for medication in medications:
            #             self._process_planned_medication(medication)
            #     elif isinstance(data, list):
            #         for entry in data:
            #             # process planned procedures
            #             procedures = entry.get("planned_procedure", [])
            #             for procedure in procedures:
            #                 self._process_planned_procedure(procedure)
            #             # process planned encounters
            #             encounters = entry.get("planned_encounter", [])
            #             for encounter in encounters:
            #                 self._process_planned_encounter(encounter)
            #             # process planned observations
            #             observations = entry.get("planned_observation", [])
            #             for observation in observations:
            #                 self._process_planned_observation(observation)
            #             # process planned immunizations
            #             immunizations = entry.get("planned_immunization", [])
            #             for immunization in immunizations:
            #                 self._process_planned_immunization(immunization)
            #             # process planned medications
            #             medications = entry.get("planned_medication", [])
            #             for medication in medications:
            #                 self._process_planned_medication(medication)

            # process medication
            if self._bene_data.get("medications"):
                data = self._bene_data.get("medications")
                if isinstance(data, dict):
                    # resolve practitioner
                    prc_id = ""
                    performers = data.get("performer", [])
                    for performer in performers:
                        if performer:
                            prc_dict = self._parse_name(performer)
                            prc_id = self._check_resource_existence(ResourceType.Practitioner.value, prc_dict)
                        if not prc_id and self._check_valid_practitioner(performer):
                            prc_id = self._process_provider(performer)
                        if prc_id:
                            break
                    # medication
                    medication = data.get("product", {})
                    if medication.get:
                        med_id = self._process_medication(medication)
                        self._process_medication_request(data, med_id, prc_id)
                elif isinstance(data, list):
                    for entry in data:
                        # resolve practitioner
                        prc_id = ""
                        performers = entry.get("performer", [])
                        for performer in performers:
                            if performer:
                                prc_dict = self._parse_name(performer)
                                prc_id = self._check_resource_existence(ResourceType.Practitioner.value, prc_dict)
                            if not prc_id and self._check_valid_practitioner(performer):
                                prc_id = self._process_provider(performer)
                            if prc_id:
                                break
                        # medication
                        medication = entry.get("product", {})
                        if medication:
                            med_id = self._process_medication(medication)
                            self._process_medication_request(entry, med_id, prc_id)
        return self._res_list

    def convert(self, output_file: str, metadata: dict = {}):
        try:
            if self._parse:
                ccda_parser = CcdaParser(self._file_path)
                if self._parse_file_path:
                    parsed_file = ccda_parser.parse(self._parse_file_path)
                else:
                    parsed_file = ccda_parser.parse()
            else:
                parsed_file = self._file_path
            self._metadata = metadata
            resources = self._process_resources(parsed_file)
            bundle_id = str(uuid.uuid4())
            bundle = {"resourceType": "Bundle", "type": "batch", "id": bundle_id, "entry": resources}
            self._write_file(bundle, output_file)
            return output_file
        except Exception as e:
            logging.error("Failed to convert CCD into FHIR. Error:", str(e))
            raise e
        finally:
            if parsed_file.startswith("/tmp/") and os.path.exists(parsed_file):
                os.unlink(parsed_file)
