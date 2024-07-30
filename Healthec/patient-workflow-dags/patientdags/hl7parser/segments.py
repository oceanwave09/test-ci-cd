import json
from benedict import benedict
from patientdags.hl7parser.enum import ResourceType
from patientdags.hl7parser.transformer.patient import PatientPID, PatientIN1
from patientdags.hl7parser.transformer.organization import Organization
from patientdags.hl7parser.transformer.practitioner import Practitioner
from patientdags.hl7parser.transformer.location import Location
from patientdags.hl7parser.transformer.encouter import Encounter
from patientdags.hl7parser.transformer.condition import Condition
from patientdags.hl7parser.transformer.observation import Observation
from patientdags.hl7parser.transformer.procedure import Procedure
from patientdags.hl7parser.transformer.allergy_intolerance import AllergyIntolerance
from patientdags.hl7parser.transformer.coverage import Coverage
from patientdags.hl7parser.transformer.immunization import Immunization
from patientdags.hl7parser.transformer.service_request import ServiceRequest
from patientdags.hl7parser.constants import (
    TYPE_PAY_CODE,
    TYPE_PAY_DISPLAY,
    # TYPE_PROV_CODE,
    # TYPE_PROV_DISPLAY,
    TYPE_OTHER_CODE,
    TYPE_OTHER_DISPLAY,
    TRANSFORM_PRACTITIONER_TYPES,
    TRANSFORM_PRACTICE_TYPES,
    # TRANSFORM_LOCATION_TYPES,
)
from patientdags.hl7parser.client import get_child_org_id


class Hl7ToFhir:
    def __init__(self, metadata: dict = {}) -> None:
        self._meta_info = metadata  # src_file_name, src_organization_id
        self._reset_dependent_attriutes()

    def _reset_dependent_attriutes(self) -> None:
        self._resource_list = list()
        self._org_prac_data = {"practitioner": [], "organization": [], "patient": []}

    def _create_bundle(self, data: dict) -> dict:
        entry_uuid = data.get("id")
        entry = {
            "fullUrl": f"urn:uuid:{entry_uuid}",
            "resource": data,
            "request": {"method": "PUT", "url": f"{data.get('resourceType')}/{entry_uuid}"},
        }
        return entry

    def _transform_resources(self, data: dict, resource_type: str) -> dict:
        if data:
            ref_lower = resource_type.lower()
            if ref_lower in self._org_prac_data:
                res_id = self._compare_resources(source_obj_type=resource_type, search_obj=data)
                if res_id:
                    return res_id
                else:
                    self._org_prac_data[ref_lower].append(data)
            transformed_data = json.loads(self.res_obj.transform())
            if len(transformed_data) > 2:
                self._resource_list.append(self._create_bundle(data=transformed_data))
                return transformed_data.get("id")
        return ""

    def _compare_resources(self, source_obj_type: str, search_obj: dict) -> str or None:
        for source_obj in self._org_prac_data.get(source_obj_type.lower()):
            if source_obj_type != ResourceType.Organization.value:
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

    def _update_org_type(self, source_type: str) -> dict:
        if source_type.upper() in ["MSH", "RXA"]:
            return {"type_code": TYPE_OTHER_CODE, "type_display": TYPE_OTHER_DISPLAY}
        else:
            return {"type_code": TYPE_PAY_CODE, "type_display": TYPE_PAY_DISPLAY}

    def _construct_practitioner_references(
        self,
        practitioner_type: list,
        data: benedict,
        source_type: str,
        source_key: str = None,
        resource_ref: dict = None,
    ) -> dict:
        practitioner_reference = {}
        for practitioner in practitioner_type:
            practitioner_reference[f"{practitioner}_reference"] = self._process_practitioner(
                data=data, source_type=source_type, role_type=practitioner, resource_ref=resource_ref
            )
        return practitioner_reference

    def _transform_patient(
        self, data: benedict, source_type: str, resource: str = ResourceType.Patient.value, references: dict = {}
    ) -> dict:
        if source_type == "PID":
            self.res_obj = PatientPID(
                data=data, resource_type=resource, references=references, metadata=self._meta_info
            )
        elif source_type == "IN1":
            self.res_obj = PatientIN1(
                data=data, resource_type=resource, references=references, metadata=self._meta_info
            )
        get_res_data = self.res_obj.build()
        return self._transform_resources(get_res_data, resource)

    def _transform_practice(
        self,
        data: benedict,
        source_type: str,
        role_type: str,
        resource: str = ResourceType.Organization.value,
        references: dict = {},
    ) -> dict:
        self.res_obj = Organization(
            data=data, role_type=role_type, resource_type=resource, references=references, metadata=self._meta_info
        )
        get_res_data = self.res_obj.build()
        if get_res_data:
            get_res_data.update(self._update_org_type(source_type=source_type))
            return self._transform_resources(get_res_data, resource)

    def _transform_practitioner(
        self,
        data: benedict,
        source_type: str,
        role_type: str,
        resource: str = ResourceType.Practitioner.value,
        references: dict = {},
    ) -> dict:
        self.res_obj = Practitioner(
            data=data, role_type=role_type, resource_type=resource, references=references, metadata=self._meta_info
        )
        get_res_data = self.res_obj.build()
        if get_res_data:
            return self._transform_resources(get_res_data, resource)

    def _transform_location(
        self,
        data: benedict,
        source_type: str,
        role_type: str,
        resource: str = ResourceType.Location.value,
        references: dict = {},
    ) -> dict:
        self.res_obj = Location(
            data=data, role_type=role_type, resource_type=resource, references=references, metadata=self._meta_info
        )
        get_res_data = self.res_obj.build()
        if get_res_data:
            return self._transform_resources(get_res_data, resource)

    def _transform_encounter(
        self, data: benedict, source_type: str, resource: str = ResourceType.Encounter.value, references: dict = {}
    ) -> dict:
        self.res_obj = Encounter(
            data=data, resource_type=resource, references=references, metadata=self._meta_info
        )
        get_res_data = self.res_obj.build()
        return self._transform_resources(get_res_data, resource)

    def _transform_condition(
        self, data: benedict, source_type: str, resource: str = ResourceType.Condition.value, references: dict = {}
    ) -> dict:
        if data.get_str("diagnosis_code_identifier") or data.get_str("associated_diagnosis_code_identifier"):
            self.res_obj = Condition(
                data=data, resource_type=resource, references=references, metadata=self._meta_info
            )
            get_res_data = self.res_obj.build()
            return self._transform_resources(get_res_data, resource)

    def _transform_procedure(
        self, data: dict, source_type: str, resource: str = ResourceType.Procedure.value, references: dict = {}
    ) -> dict:
        if data.get_str("procedure_code_identifier"):
            self.res_obj = Procedure(
                data=data, resource_type=resource, references=references, metadata=self._meta_info
            )
            get_res_data = self.res_obj.build()
            return self._transform_resources(get_res_data, resource)

    def _transform_observation(
        self, data: dict, source_type: str, resource: str = ResourceType.Observation.value, references: dict = {}
    ) -> dict:
        if data.get_str("observation_value"):
            self.res_obj = Observation(
                data=data, resource_type=resource, references=references, metadata=self._meta_info
            )
            get_res_data = self.res_obj.build()
            return self._transform_resources(get_res_data, resource)

    def _transform_coverage(
        self, data: dict, source_type: str, resource: str = ResourceType.Coverage.value, references: dict = {}
    ) -> dict:
        if data.get_str("health_plan_identifier"):
            self.res_obj = Coverage(
                data=data, resource_type=resource, references=references, metadata=self._meta_info
            )
            get_res_data = self.res_obj.build()
            return self._transform_resources(get_res_data, resource)

    def _transform_allergy(
        self,
        data: dict,
        source_type: str,
        resource: str = ResourceType.AllergyIntolerance.value,
        references: dict = {},
    ) -> dict:
        if data.get_str("allergy_code_identifier"):
            self.res_obj = AllergyIntolerance(
                data=data, resource_type=resource, references=references, metadata=self._meta_info
            )
            get_res_data = self.res_obj.build()
            return self._transform_resources(get_res_data, resource)

    def _transform_immunization(
        self, data: dict, source_type: str, resource: str = ResourceType.Immunization.value, references: dict = {}
    ) -> dict:
        if data.get_str("administered_code_identifier") or data.get_str("administered_code_alternate_identifier"):
            self.res_obj = Immunization(
                data=data, resource_type=resource, references=references, metadata=self._meta_info
            )
            get_res_data = self.res_obj.build()
            return self._transform_resources(get_res_data, resource)

    def _transform_service_request(
        self,
        data: dict,
        source_type: str,
        resource: str = ResourceType.ServiceRequest.value,
        references: dict = {},
    ) -> dict:
        if data.get_str("placer_order_number_identifier") or data.get_str("filler_order_number_identifier"):
            self.res_obj = ServiceRequest(
                data=data, resource_type=resource, references=references, metadata=self._meta_info
            )
            get_res_data = self.res_obj.build()
            return self._transform_resources(get_res_data, resource)

    def _process_practice(
        self,
        data: benedict,
        source_type: str,
        role_type: str = None,
        source_key: str = None,
        resource_ref: dict = None,
    ) -> dict:
        practice_data = data if not source_key else data.get(source_key)
        if not practice_data:
            return
        return self._transform_practice(
            data=practice_data, source_type=source_type, role_type=role_type, references=resource_ref
        )

    def _process_practitioner(
        self,
        data: benedict,
        source_type: str,
        role_type: str = None,
        source_key: str = None,
        resource_ref: dict = None,
    ) -> dict:
        practitioner_data = data if not source_key else data.get(source_key)
        if not practitioner_data:
            return
        if isinstance(practitioner_data, dict):
            return self._transform_practitioner(
                data=practitioner_data, source_type=source_type, role_type=role_type, references=resource_ref
            )
        else:
            practitioner_ref = []
            for _index in range(len(practitioner_data)):
                pract = data.get(f"{source_key}[{_index}]")
                practitioner_ref.append(
                    self._transform_practitioner(
                        data=pract, source_type=source_type, role_type=role_type, references=resource_ref
                    )
                )
            return practitioner_ref

    def _process_patient(
        self, data: benedict, source_type: str, source_key: str = None, resource_ref: dict = None
    ) -> dict:
        patient_ref = {}
        if resource_ref:
            patient_ref.update(resource_ref)
        patient_data = data if not source_key else data.get(source_key)
        if not patient_data:
            return
        if source_type == "PID":
            if "PD1" in data and data.get("PD1"):
                patient_ref["organization_reference"] = self._process_practice(
                    data=data.get("PD1"),
                    source_type="PID",
                    role_type=TRANSFORM_PRACTICE_TYPES.get("PD1")[0],
                    resource_ref=patient_ref,
                )
                patient_ref["practitioner_reference"] = self._process_practitioner(
                    data=data.get("PD1"),
                    source_type="PD1",
                    role_type=TRANSFORM_PRACTITIONER_TYPES.get("PD1")[0],
                    resource_ref=patient_ref,
                )
            if "NK1" in data and data.get("NK1"):
                patient_data.update({"NK1": data.get("NK1")})

            patient_ref["patient_reference"] = self._transform_patient(
                data=patient_data, source_type=source_type, references=patient_ref
            )
            return patient_ref
        else:
            return self._transform_patient(data=patient_data, source_type=source_type, references=patient_ref)

    def _process_location(
        self,
        data: benedict,
        source_type: str,
        role_type: str = None,
        source_key: str = None,
        resource_ref: dict = None,
    ) -> dict:
        location_data = data if not source_key else data.get(source_key)
        if not location_data:
            return
        return self._transform_location(
            data=location_data, source_type=source_type, role_type=role_type, references=resource_ref
        )

    def _process_encounter(
        self, data: benedict, source_type: str, source_key: str = None, resource_ref: dict = None
    ) -> dict or list:
        encounter_ref = {}
        if resource_ref:
            encounter_ref.update(resource_ref)
        encounter_data = data.get("PV1") if not source_key else data.get(source_key)
        if not encounter_data:
            return
        if "PV2" in data and data.get("PV2"):
            encounter_data.update({"PV2": data.get("PV2")})
        if "PID" in data and data.get("PID"):
            encounter_data.update(
                {
                    "patient_account_number": data.get("PID.patient_account_number"),
                    "patient_identifier_list_id": data.get("PID.patient_identifier_list_id"),
                    "patient_identifier_id": data.get("PID.patient_identifier_id"),
                }
            )
        # encounter_ref["assigned_patient_reference"] = self._process_location(
        #    data=encounter_data, source_type= "PV1",
        #    role_type=TRANSFORM_LOCATION_TYPES.get("PV1")[0], resource_ref=encounter_ref
        # )
        encounter_ref.update(
            self._construct_practitioner_references(
                practitioner_type=TRANSFORM_PRACTITIONER_TYPES.get("PV1"),
                data=encounter_data,
                source_key=source_key,
                source_type=source_type,
                resource_ref=resource_ref,
            )
        )
        return self._transform_encounter(data=encounter_data, source_type=source_type, references=encounter_ref)

    def _process_condition(
        self, data: benedict, source_type: str, source_key: str = None, resource_ref: dict = None
    ) -> dict:
        condition_data = data if not source_key else data.get(source_key)
        if not condition_data:
            return
        if isinstance(condition_data, dict):
            return self._transform_condition(data=condition_data, source_type=source_type, references=resource_ref)
        elif isinstance(condition_data, list):
            condition_ref = []
            for _index in range(len(condition_data)):
                cond = condition_data[_index]
                condition_ref.append(
                    self._transform_condition(data=cond, source_type=source_type, references=resource_ref)
                )
            return condition_ref
        return {}

    def _process_procedure(
        self, data: benedict, source_type: str, source_key: str = None, resource_ref: dict = None
    ) -> dict:
        procedure_ref = {}
        if resource_ref:
            procedure_ref.update(resource_ref)
        procedure_data = data if not source_key else data.get(source_key)
        if not procedure_data:
            return
        if isinstance(procedure_data, dict):
            procedure_ref["condition_reference"] = self._process_condition(
                data=procedure_data, source_type=source_type, resource_ref=procedure_ref
            )
            procedure_ref.update(
                self._construct_practitioner_references(
                    practitioner_type=TRANSFORM_PRACTITIONER_TYPES.get("PR1"),
                    data=procedure_data,
                    source_key=source_key,
                    source_type=source_type,
                    resource_ref=resource_ref,
                )
            )

            return self._transform_procedure(
                data=procedure_data, source_type=source_type, references=procedure_ref
            )
        elif isinstance(procedure_data, list):
            proce_res = []
            for _index in range(len(procedure_data)):
                procedure = procedure_data[_index]
                procedure_ref["condition_reference"] = self._process_condition(
                    data=procedure, source_type=source_type, resource_ref=procedure_ref
                )
                procedure_ref.update(
                    self._construct_practitioner_references(
                        practitioner_type=TRANSFORM_PRACTITIONER_TYPES.get("PR1"),
                        data=procedure,
                        source_key=source_key,
                        source_type=source_type,
                        resource_ref=resource_ref,
                    )
                )
                proce_res.append(
                    self._transform_procedure(data=procedure, source_type=source_type, references=procedure_ref)
                )
            return proce_res
        return {}

    def _process_observation(
        self, data: benedict, source_type: str, source_key: str = None, resource_ref: dict = None
    ) -> dict:
        observation_data = data if not source_key else data.get(source_key)
        if not observation_data:
            return
        obs_prac_ref = {}
        if resource_ref:
            obs_prac_ref.update(resource_ref)
        if isinstance(observation_data, dict):
            obs_prac_ref["performing_organization_reference"] = self._process_practice(
                data=observation_data,
                source_type="OBX",
                role_type=TRANSFORM_PRACTICE_TYPES.get("OBX")[0],
                resource_ref=resource_ref,
            )
            obs_prac_ref["responsible_observer_reference"] = self._process_practitioner(
                data=observation_data,
                source_type="OBX",
                role_type=TRANSFORM_PRACTITIONER_TYPES.get("OBX")[0],
                resource_ref=obs_prac_ref,
            )
            return self._transform_observation(
                data=observation_data, source_type=source_type, references=obs_prac_ref
            )
        elif isinstance(observation_data, list):
            obs_res = []
            for _index in range(len(observation_data)):
                obs = observation_data[_index]
                obs_prac_ref["performing_organization_reference"] = self._process_practice(
                    data=obs,
                    source_type="OBX",
                    role_type=TRANSFORM_PRACTICE_TYPES.get("OBX")[0],
                    resource_ref=resource_ref,
                )
                obs_prac_ref["responsible_observer_reference"] = self._process_practitioner(
                    data=obs,
                    source_type="OBX",
                    role_type=TRANSFORM_PRACTITIONER_TYPES.get("OBX")[0],
                    resource_ref=obs_prac_ref,
                )
                obs_res.append(
                    self._transform_observation(data=obs, source_type=source_type, references=obs_prac_ref)
                )
            return obs_res
        return {}

    def _process_coverage(
        self, data: benedict, source_type: str, source_key: str = None, resource_ref: dict = None
    ) -> dict:
        insurance_data = data if not source_key else data.get(source_key)
        if not insurance_data:
            return
        insurance_ref = {}
        if resource_ref:
            insurance_ref.update(resource_ref)
        if isinstance(insurance_data, dict):
            insurance_ref["insurance_company_reference"] = self._process_practice(
                data=insurance_data,
                source_type="IN1",
                role_type=TRANSFORM_PRACTICE_TYPES.get("IN1")[0],
                resource_ref=insurance_ref,
            )
            # if insurance_data.get("insured_group_emp_id"):
            # insurance_ref["policy_holder_reference"] = self._process_practice(
            #     data=insurance_data,
            #     source_type="IN1",
            #     role_type=TRANSFORM_PRACTICE_TYPES.get("IN1")[1],
            #     resource_ref=insurance_ref,
            # )
            # else:
            #     insurance_ref["subscriber_reference"] = self._process_patient(
            #         data=insurance_data, source_type=source_type, resource_ref=insurance_ref
            #     )

            return self._transform_coverage(data=insurance_data, source_type=source_type, references=insurance_ref)
        else:
            ins_res = []
            for _index in range(len(insurance_data)):
                ins = insurance_data[_index]
                insurance_ref["insurance_company_reference"] = self._process_practice(
                    data=ins,
                    source_type="IN1",
                    role_type=TRANSFORM_PRACTICE_TYPES.get("IN1")[0],
                    resource_ref=insurance_ref,
                )
                # if ins.get("insured_group_emp_id"):
                #     insurance_ref["policy_holder_reference"] = self._process_practice(
                # data=ins, source_type="IN1",
                #     role_type=TRANSFORM_PRACTICE_TYPES.get("IN1")[1], resource_ref=insurance_ref
                # )
                # else:
                #     insurance_ref["subscriber_reference"] = self._process_patient(
                #         data=ins, source_type=source_type, resource_ref=insurance_ref
                #     )

                ins_res.append(
                    self._transform_coverage(data=ins, source_type=source_type, references=insurance_ref)
                )
            return ins_res

    def _process_allergy(
        self, data: benedict, source_type: str, source_key: str = None, resource_ref: dict = None
    ) -> dict:
        allergy_data = data if not source_key else data.get(source_key)
        if not allergy_data:
            return
        allergy_ref = {}
        if resource_ref:
            allergy_ref.update(resource_ref)
        if isinstance(allergy_data, dict):
            return self._transform_allergy(data=allergy_data, source_type=source_type, references=allergy_ref)
        elif isinstance(allergy_data, list):
            allergy_res = []
            for _index in range(len(allergy_data)):
                allergy = allergy_data[_index]
                allergy_res.append(
                    self._transform_allergy(data=allergy, source_type=source_type, references=allergy_ref)
                )
            return allergy_res
        return {}

    def _process_immunization(
        self, data: benedict, source_type: str, source_key: str = None, resource_ref: dict = None
    ) -> dict:
        immunization_data = data if not source_key else data.get(source_key)
        if not immunization_data:
            return
        immu_prac_ref = {}
        if resource_ref:
            immu_prac_ref.update(resource_ref)
        if "RXR" in data:
            immunization_data.update(data.get("RXR"))
        if isinstance(immunization_data, dict):
            immu_prac_ref["manufacturer_reference"] = self._process_practice(
                data=immunization_data,
                source_type="RXA",
                role_type=TRANSFORM_PRACTICE_TYPES.get("RXA")[0],
                resource_ref=immu_prac_ref,
            )
            immu_prac_ref["administering_provider_reference"] = self._process_practitioner(
                data=immunization_data,
                source_type="RXA",
                role_type=TRANSFORM_PRACTITIONER_TYPES.get("RXA")[0],
                resource_ref=immu_prac_ref,
            )
            return self._transform_immunization(
                data=immunization_data, source_type=source_type, references=immu_prac_ref
            )
        elif isinstance(immunization_data, list):
            immu_res = []
            for _index in range(len(immunization_data)):
                immu = immunization_data[_index]
                immu_prac_ref["manufacturer_reference"] = self._process_practice(
                    data=immu,
                    source_type="RXA",
                    role_type=TRANSFORM_PRACTICE_TYPES.get("RXA")[0],
                    resource_ref=immu_prac_ref,
                )
                immu_prac_ref["administering_provider_reference"] = self._process_practitioner(
                    data=immu,
                    source_type="RXA",
                    role_type=TRANSFORM_PRACTITIONER_TYPES.get("RXA")[0],
                    resource_ref=immu_prac_ref,
                )
                immu_res.append(
                    self._transform_immunization(data=immu, source_type=source_type, references=immu_prac_ref)
                )
            return immu_res
        return {}

    def _process_service_request(
        self, data: benedict, source_type: str, source_key: str = None, resource_ref: dict = None
    ) -> dict:
        service_data = data if not source_key else data.get(source_key)
        if not service_data:
            return
        serv_req_ref = {}
        if resource_ref:
            serv_req_ref.update(resource_ref)
        if isinstance(service_data, dict):
            serv_req_ref["ordering_provider_reference"] = self._process_practitioner(
                data=service_data,
                source_type="ORC",
                role_type=TRANSFORM_PRACTITIONER_TYPES.get("ORC")[0],
                resource_ref=serv_req_ref,
            )
            return self._transform_service_request(
                data=service_data, source_type=source_type, references=serv_req_ref
            )
        elif isinstance(service_data, list):
            service_req = []
            for _index in range(len(service_data)):
                serv_req = service_data[_index]
                serv_req_ref["ordering_provider_reference"] = self._process_practitioner(
                    data=serv_req,
                    source_type="ORC",
                    role_type=TRANSFORM_PRACTITIONER_TYPES.get("ORC")[0],
                    resource_ref=serv_req_ref,
                )
                service_req.append(
                    self._transform_service_request(
                        data=serv_req, source_type=source_type, references=serv_req_ref
                    )
                )
            return service_req
        return {}

    def _process_msh(self, data: benedict, source_type: str, source_key: str = None, resource_ref: dict = None):
        msh_data = data if not source_key else data.get(source_key)
        if not msh_data:
            return
        get_child_org_name = msh_data.get(f"{TRANSFORM_PRACTICE_TYPES.get('MSH')[0]}_name")
        msh_data.update({"parent_organization_id": self._meta_info.get("src_organization_id")})
        org_obj = Organization(data=msh_data, role_type=TRANSFORM_PRACTICE_TYPES.get("MSH")[0])
        get_res_data = org_obj.build()
        if get_res_data:
            get_res_data.update(self._update_org_type(source_type=source_type))
            get_child_org_resource = json.loads(org_obj.transform())
            # self._resource_list.append(self._create_bundle(data=get_child_org_resource))
            child_org_id = get_child_org_id(
                tenant=self._meta_info.get("file_tenant"),
                parent_org_id=self._meta_info.get("src_organization_id"),
                child_org_name=get_child_org_name,
                child_org_resource=get_child_org_resource,
            )
            self._meta_info.update({"child_org_id": child_org_id})
            return child_org_id
