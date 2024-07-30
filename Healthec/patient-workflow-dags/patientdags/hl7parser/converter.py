import json
import logging
import os
import uuid
from benedict import benedict

from patientdags.hl7parser.message_type_parser import Hl7Parser
from patientdags.hl7parser.segments import Hl7ToFhir
from patientdags.hl7parser.utils import write_file
from patientdags.hl7parser.constants import AVAILABLE_SEGMENTS
from patientdags.hl7parser.segment_parser import Hl7SegmentParser


class Hl7Converter:
    def __init__(
        self,
        file_path: str,
        canonical_file_path: str,
        message_type: str = None,
        parse: bool = True,
        segment_order: bool = False,
    ) -> None:
        self._meta_info = {}
        self._file_path = file_path
        self._canonical_file_path = canonical_file_path
        self._raw_data = []
        self._fhir_data = []
        self._message_type = message_type
        self._segment_order = segment_order
        self._parse = parse

    def adt(self, data: benedict) -> list:
        obj = Hl7ToFhir(self._meta_info)
        references = {}
        if "MSH" in data:
            obj._process_msh(data=data, source_type="MSH", source_key="MSH")
        if "PD1" in data or "PID" in data:
            references.update(obj._process_patient(data=data, source_type="PID", source_key="PID"))
        if "PV1" in data or "PV2" in data:
            references["encounter_reference"] = obj._process_encounter(
                data=data, source_type="PV1", source_key="PV1", resource_ref=references
            )
        if "DG1" in data:
            references["condition_reference"] = obj._process_condition(
                data=data, source_type="DG1", source_key="DG1", resource_ref=references
            )
        if "procedure" in data:
            for proc in data.get("procedure"):
                if "PR1" in proc:
                    references["procedure_reference"] = obj._process_procedure(
                        data=benedict(proc), source_type="PR1", source_key="PR1", resource_ref=references
                    )
        if "OBX" in data:
            references["observation_reference"] = obj._process_observation(
                data=data, source_type="OBX", source_key="OBX", resource_ref=references
            )
        if "AL1" in data:
            references["allergy_referenece"] = obj._process_allergy(
                data=data, source_type="AL1", source_key="AL1", resource_ref=references
            )
        if "insurance" in data:
            for ins_data in data.get("insurance"):
                if "IN1" in ins_data:
                    references["insurance_referenece"] = obj._process_coverage(
                        data=benedict(ins_data), source_type="IN1", source_key="IN1", resource_ref=references
                    )
        return obj._resource_list

    # def adt_a02(self):
    #     obj = Hl7ToFhir(self._meta_info)
    #     for data in self._raw_data:
    #         _data = benedict(_data)
    #         references = {}
    #         if "PD1" in _data or "PID" in _data:
    #             references.update(obj._process_patient(data=_data, source_type="PID", source_key="PID"))
    #         if "PV1" in _data or "PV2" in _data:
    #             references["encounter_reference"] = obj._process_encounter(
    #                 data=_data, source_type="PV1", source_key="PV1", resource_ref=references
    #             )
    #         if "OBX" in _data:
    #             references["observation_reference"] = obj._process_observation(
    #                 data=_data, source_type="OBX", source_key="OBX", resource_ref=references
    #             )
    #     return obj._resource_list

    # def adt_a03(self):
    #     obj = Hl7ToFhir(self._meta_info)
    #     for _data in self._raw_data:
    #         _data = benedict(_data)
    #         references = {}
    #         if "PD1" in _data or "PID" in _data:
    #             references.update(obj._process_patient(data=_data, source_type="PID", source_key="PID"))
    #         if "PV1" in _data or "PV2" in _data:
    #             references["encounter_reference"] = obj._process_encounter(
    #                 data=_data, source_type="PV1", source_key="PV1", resource_ref=references
    #             )
    #         if "AL1" in _data:
    #             references["allergy_referenece"] = obj._process_allergy(
    #                 data=_data, source_type="AL1", source_key="AL1", resource_ref=references
    #             )
    #         if "DG1" in _data:
    #             references["condition_reference"] = obj._process_condition(
    #                 data=_data, source_type="DG1", source_key="DG1", resource_ref=references
    #             )
    #         if "procedure" in _data:
    #             for proc in _data.get("procedure"):
    #                 if "PR1" in proc:
    #                     references["procedure_reference"] = obj._process_procedure(
    #                         data=benedict(proc), source_type="PR1", source_key="PR1", resource_ref=references
    #                     )
    #         if "OBX" in _data:
    #             references["observation_reference"] = obj._process_observation(
    #                 data=_data, source_type="OBX", source_key="OBX", resource_ref=references
    #             )
    #         if "insurance" in _data:
    #             for ins_data in _data.get("insurance"):
    #                 if "IN1" in ins_data:
    #                     references["insurance_referenece"] = obj._process_coverage(
    #                         data=benedict(ins_data), source_type="IN1", source_key="IN1", resource_ref=references
    #                     )
    #     return obj._resource_list

    # def adt_a04(self):
    #     obj = Hl7ToFhir(self._meta_info)
    #     for _data in self._raw_data:
    #         _data = benedict(_data)
    #         references = {}
    #         if "PD1" in _data or "PID" in _data:
    #             references.update(obj._process_patient(data=_data, source_type="PID", source_key="PID"))
    #         if "PV1" in _data or "PV2" in _data:
    #             references["encounter_reference"] = obj._process_encounter(
    #                 data=_data, source_type="PV1", source_key="PV1", resource_ref=references
    #             )
    #         if "OBX" in _data:
    #             references["observation_reference"] = obj._process_observation(
    #                 data=_data, source_type="OBX", source_key="OBX", resource_ref=references
    #             )
    #         if "AL1" in _data:
    #             references["allergy_referenece"] = obj._process_allergy(
    #                 data=_data, source_type="AL1", source_key="AL1", resource_ref=references
    #             )
    #         if "DG1" in _data:
    #             references["condition_reference"] = obj._process_condition(
    #                 data=_data, source_type="DG1", source_key="DG1", resource_ref=references
    #             )
    #         if "procedure" in _data:
    #             for proc in _data.get("procedure"):
    #                 if "PR1" in proc:
    #                     references["procedure_reference"] = obj._process_procedure(
    #                         data=benedict(proc), source_type="PR1", source_key="PR1", resource_ref=references
    #                     )
    #         if "insurance" in _data:
    #             for ins_data in _data.get("insurance"):
    #                 if "IN1" in ins_data:
    #                     references["insurance_referenece"] = obj._process_coverage(
    #                         data=benedict(ins_data), source_type="IN1", source_key="IN1", resource_ref=references
    #                     )
    #     return obj._resource_list

    # def adt_a05(self):
    #     obj = Hl7ToFhir(self._meta_info)
    #     for _data in self._raw_data:
    #         _data = benedict(_data)
    #         references = {}
    #         if "PD1" in _data or "PID" in _data:
    #             references.update(obj._process_patient(data=_data, source_type="PID", source_key="PID"))
    #         if "PV1" in _data or "PV2" in _data:
    #             references["encounter_reference"] = obj._process_encounter(
    #                 data=_data, source_type="PV1", source_key="PV1", resource_ref=references
    #             )
    #         if "OBX" in _data:
    #             references["observation_reference"] = obj._process_observation(
    #                 data=_data, source_type="OBX", source_key="OBX", resource_ref=references
    #             )
    #         if "AL1" in _data:
    #             references["allergy_referenece"] = obj._process_allergy(
    #                 data=_data, source_type="AL1", source_key="AL1", resource_ref=references
    #             )
    #         if "DG1" in _data:
    #             references["condition_reference"] = obj._process_condition(
    #                 data=_data, source_type="DG1", source_key="DG1", resource_ref=references
    #             )
    #         if "procedure" in _data:
    #             for proc in _data.get("procedure"):
    #                 if "PR1" in proc:
    #                     references["procedure_reference"] = obj._process_procedure(
    #                         data=benedict(proc), source_type="PR1", source_key="PR1", resource_ref=references
    #                     )
    #         if "insurance" in _data:
    #             for ins_data in _data.get("insurance"):
    #                 if "IN1" in ins_data:
    #                     references["insurance_referenece"] = obj._process_coverage(
    #                         data=benedict(ins_data), source_type="IN1", source_key="IN1", resource_ref=references
    #                     )
    #     return obj._resource_list

    # def adt_a06(self):
    #     obj = Hl7ToFhir(self._meta_info)
    #     for _data in self._raw_data:
    #         _data = benedict(_data)
    #         references = {}
    #         if "PD1" in _data or "PID" in _data:
    #             references.update(obj._process_patient(data=_data, source_type="PID", source_key="PID"))
    #         if "PV1" in _data or "PV2" in _data:
    #             references["encounter_reference"] = obj._process_encounter(
    #                 data=_data, source_type="PV1", source_key="PV1", resource_ref=references
    #             )
    #         if "OBX" in _data:
    #             references["observation_reference"] = obj._process_observation(
    #                 data=_data, source_type="OBX", source_key="OBX", resource_ref=references
    #             )
    #         if "AL1" in _data:
    #             references["allergy_referenece"] = obj._process_allergy(
    #                 data=_data, source_type="AL1", source_key="AL1", resource_ref=references
    #             )
    #         if "DG1" in _data:
    #             references["condition_reference"] = obj._process_condition(
    #                 data=_data, source_type="DG1", source_key="DG1", resource_ref=references
    #             )
    #         if "procedure" in _data:
    #             for proc in _data.get("procedure"):
    #                 if "PR1" in proc:
    #                     references["procedure_reference"] = obj._process_procedure(
    #                         data=benedict(proc), source_type="PR1", source_key="PR1", resource_ref=references
    #                     )
    #         if "insurance" in _data:
    #             for ins_data in _data.get("insurance"):
    #                 if "IN1" in ins_data:
    #                     references["insurance_referenece"] = obj._process_coverage(
    #                         data=benedict(ins_data), source_type="IN1", source_key="IN1", resource_ref=references
    #                     )
    #     return obj._resource_list

    # def adt_a07(self):
    #     obj = Hl7ToFhir(self._meta_info)
    #     for _data in self._raw_data:
    #         _data = benedict(_data)
    #         references = {}
    #         if "PD1" in _data or "PID" in _data:
    #             references.update(obj._process_patient(data=_data, source_type="PID", source_key="PID"))
    #         if "PV1" in _data or "PV2" in _data:
    #             references["encounter_reference"] = obj._process_encounter(
    #                 data=_data, source_type="PV1", source_key="PV1", resource_ref=references
    #             )
    #         if "OBX" in _data:
    #             references["observation_reference"] = obj._process_observation(
    #                 data=_data, source_type="OBX", source_key="OBX", resource_ref=references
    #             )
    #         if "AL1" in _data:
    #             references["allergy_referenece"] = obj._process_allergy(
    #                 data=_data, source_type="AL1", source_key="AL1", resource_ref=references
    #             )
    #         if "DG1" in _data:
    #             references["condition_reference"] = obj._process_condition(
    #                 data=_data, source_type="DG1", source_key="DG1", resource_ref=references
    #             )
    #         if "procedure" in _data:
    #             for proc in _data.get("procedure"):
    #                 if "PR1" in proc:
    #                     references["procedure_reference"] = obj._process_procedure(
    #                         data=benedict(proc), source_type="PR1", source_key="PR1", resource_ref=references
    #                     )
    #         if "insurance" in _data:
    #             for ins_data in _data.get("insurance"):
    #                 if "IN1" in ins_data:
    #                     references["insurance_referenece"] = obj._process_coverage(
    #                         data=benedict(ins_data), source_type="IN1", source_key="IN1", resource_ref=references
    #                     )
    #     return obj._resource_list

    # def adt_a08(self):
    #     obj = Hl7ToFhir(self._meta_info)
    #     for _data in self._raw_data:
    #         _data = benedict(_data)
    #         references = {}
    #         if "PD1" in _data or "PID" in _data:
    #             references.update(obj._process_patient(data=_data, source_type="PID", source_key="PID"))
    #         if "PV1" in _data or "PV2" in _data:
    #             references["encounter_reference"] = obj._process_encounter(
    #                 data=_data, source_type="PV1", source_key="PV1", resource_ref=references
    #             )
    #         if "OBX" in _data:
    #             references["observation_reference"] = obj._process_observation(
    #                 data=_data, source_type="OBX", source_key="OBX", resource_ref=references
    #             )
    #         if "AL1" in _data:
    #             references["allergy_referenece"] = obj._process_allergy(
    #                 data=_data, source_type="AL1", source_key="AL1", resource_ref=references
    #             )
    #         if "DG1" in _data:
    #             references["condition_reference"] = obj._process_condition(
    #                 data=_data, source_type="DG1", source_key="DG1", resource_ref=references
    #             )
    #         if "procedure" in _data:
    #             for proc in _data.get("procedure"):
    #                 if "PR1" in proc:
    #                     references["procedure_reference"] = obj._process_procedure(
    #                         data=benedict(proc), source_type="PR1", source_key="PR1", resource_ref=references
    #                     )
    #         if "insurance" in _data:
    #             for ins_data in _data.get("insurance"):
    #                 if "IN1" in ins_data:
    #                     references["insurance_referenece"] = obj._process_coverage(
    #                         data=benedict(ins_data), source_type="IN1", source_key="IN1", resource_ref=references
    #                     )
    #     return obj._resource_list

    # def adt_a11(self):
    #     obj = Hl7ToFhir(self._meta_info)
    #     for _data in self._raw_data:
    #         _data = benedict(_data)
    #         references = {}
    #         if "PD1" in _data or "PID" in _data:
    #             references.update(obj._process_patient(data=_data, source_type="PID", source_key="PID"))
    #         if "PV1" in _data or "PV2" in _data:
    #             references["encounter_reference"] = obj._process_encounter(
    #                 data=_data, source_type="PV1", source_key="PV1", resource_ref=references
    #             )
    #         if "OBX" in _data:
    #             references["observation_reference"] = obj._process_observation(
    #                 data=_data, source_type="OBX", source_key="OBX", resource_ref=references
    #             )
    #         if "DG1" in _data:
    #             references["condition_reference"] = obj._process_condition(
    #                 data=_data, source_type="DG1", source_key="DG1", resource_ref=references
    #             )
    #     return obj._resource_list

    # def adt_a13(self):
    #     obj = Hl7ToFhir(self._meta_info)
    #     for _data in self._raw_data:
    #         _data = benedict(_data)
    #         references = {}
    #         if "PD1" in _data or "PID" in _data:
    #             references.update(obj._process_patient(data=_data, source_type="PID", source_key="PID"))
    #         if "PV1" in _data or "PV2" in _data:
    #             references["encounter_reference"] = obj._process_encounter(
    #                 data=_data, source_type="PV1", source_key="PV1", resource_ref=references
    #             )
    #         if "OBX" in _data:
    #             references["observation_reference"] = obj._process_observation(
    #                 data=_data, source_type="OBX", source_key="OBX", resource_ref=references
    #             )
    #         if "AL1" in _data:
    #             references["allergy_referenece"] = obj._process_allergy(
    #                 data=_data, source_type="AL1", source_key="AL1", resource_ref=references
    #             )
    #         if "DG1" in _data:
    #             references["condition_reference"] = obj._process_condition(
    #                 data=_data, source_type="DG1", source_key="DG1", resource_ref=references
    #             )
    #         if "procedure" in _data:
    #             for proc in _data.get("procedure"):
    #                 if "PR1" in proc:
    #                     references["procedure_reference"] = obj._process_procedure(
    #                         data=benedict(proc), source_type="PR1", source_key="PR1", resource_ref=references
    #                     )
    #         if "insurance" in _data:
    #             for ins_data in _data.get("insurance"):
    #                 if "IN1" in ins_data:
    #                     references["insurance_referenece"] = obj._process_coverage(
    #                         data=benedict(ins_data), source_type="IN1", source_key="IN1", resource_ref=references
    #                     )
    #     return obj._resource_list

    # def adt_a28(self):
    #     obj = Hl7ToFhir(self._meta_info)
    #     for _data in self._raw_data:
    #         _data = benedict(_data)
    #         references = {}
    #         if "PD1" in _data or "PID" in _data:
    #             references.update(obj._process_patient(data=_data, source_type="PID", source_key="PID"))
    #         if "PV1" in _data or "PV2" in _data:
    #             references["encounter_reference"] = obj._process_encounter(
    #                 data=_data, source_type="PV1", source_key="PV1", resource_ref=references
    #             )
    #         if "OBX" in _data:
    #             references["observation_reference"] = obj._process_observation(
    #                 data=_data, source_type="OBX", source_key="OBX", resource_ref=references
    #             )
    #         if "AL1" in _data:
    #             references["allergy_referenece"] = obj._process_allergy(
    #                 data=_data, source_type="AL1", source_key="AL1", resource_ref=references
    #             )
    #         if "DG1" in _data:
    #             references["condition_reference"] = obj._process_condition(
    #                 data=_data, source_type="DG1", source_key="DG1", resource_ref=references
    #             )
    #         if "procedure" in _data:
    #             for proc in _data.get("procedure"):
    #                 if "PR1" in proc:
    #                     references["procedure_reference"] = obj._process_procedure(
    #                         data=benedict(proc), source_type="PR1", source_key="PR1", resource_ref=references
    #                     )
    #         if "insurance" in _data:
    #             for ins_data in _data.get("insurance"):
    #                 if "IN1" in ins_data:
    #                     references["insurance_referenece"] = obj._process_coverage(
    #                         data=benedict(ins_data), source_type="IN1", source_key="IN1", resource_ref=references
    #                     )
    #     return obj._resource_list

    # def adt_a31(self):
    #     obj = Hl7ToFhir(self._meta_info)
    #     for _data in self._raw_data:
    #         _data = benedict(_data)
    #         references = {}
    #         if "PD1" in _data or "PID" in _data:
    #             references.update(obj._process_patient(data=_data, source_type="PID", source_key="PID"))
    #         if "PV1" in _data or "PV2" in _data:
    #             references["encounter_reference"] = obj._process_encounter(
    #                 data=_data, source_type="PV1", source_key="PV1", resource_ref=references
    #             )
    #         if "OBX" in _data:
    #             references["observation_reference"] = obj._process_observation(
    #                 data=_data, source_type="OBX", source_key="OBX", resource_ref=references
    #             )
    #         if "AL1" in _data:
    #             references["allergy_referenece"] = obj._process_allergy(
    #                 data=_data, source_type="AL1", source_key="AL1", resource_ref=references
    #             )
    #         if "DG1" in _data:
    #             references["condition_reference"] = obj._process_condition(
    #                 data=_data, source_type="DG1", source_key="DG1", resource_ref=references
    #             )
    #         if "procedure" in _data:
    #             for proc in _data.get("procedure"):
    #                 if "PR1" in proc:
    #                     references["procedure_reference"] = obj._process_procedure(
    #                         data=benedict(proc), source_type="PR1", source_key="PR1", resource_ref=references
    #                     )
    #         if "insurance" in _data:
    #             for ins_data in _data.get("insurance"):
    #                 if "IN1" in ins_data:
    #                     references["insurance_referenece"] = obj._process_coverage(
    #                         data=benedict(ins_data), source_type="IN1", source_key="IN1", resource_ref=references
    #                     )
    #     return obj._resource_list

    def oru(self, data: benedict) -> list:
        obj = Hl7ToFhir(self._meta_info)
        if "MSH" in data:
            obj._process_msh(data=data, source_type="MSH", source_key="MSH")
        for pat_results in data.get("patient_result", []):
            if "patient" in pat_results:
                patient = pat_results.get("patient")
                references = {}
                if "PD1" in patient or "PID" in patient:
                    references.update(obj._process_patient(data=benedict(patient), source_type="PID", source_key="PID"))
                if "visit" in patient:
                    visit_data = patient.get("visit")
                    if "PV1" in visit_data or "PV2" in visit_data:
                        references["encounter_reference"] = obj._process_encounter(
                            data=benedict(visit_data),
                            source_type="PV1",
                            source_key="PV1",
                            resource_ref=references,
                        )
            if "order_observation" in pat_results:
                for observation in pat_results.get("order_observation", []):
                    if "ORC" in observation:
                        obj._process_service_request(
                            data=benedict(observation),
                            source_type="ORC",
                            source_key="ORC",
                            resource_ref=references,
                        )
                    for obs in observation.get("observation", []):
                        if "OBX" in obs:
                            references["observation_reference"] = obj._process_observation(
                                data=benedict(obs),
                                source_type="OBX",
                                source_key="OBX",
                                resource_ref=references,
                            )
        return obj._resource_list

    def vxu(self, data: benedict) -> list:
        obj = Hl7ToFhir(self._meta_info)
        references = {}
        if "MSH" in data:
            obj._process_msh(data=data, source_type="MSH", source_key="MSH")
        if "PD1" in data or "PID" in data:
            references.update(obj._process_patient(data=data, source_type="PID", source_key="PID"))
        if "patient" in data:
            visit_data = data.get("patient")
            if "PV1" in visit_data or "PV2" in visit_data:
                references["encounter_reference"] = obj._process_encounter(
                    data=visit_data, source_type="PV1", source_key="PV1", resource_ref=references
                )
        if "insurance" in data:
            for ins_data in data.get("insurance"):
                if "IN1" in ins_data:
                    obj._process_coverage(
                        data=benedict(ins_data), source_type="IN1", source_key="IN1", resource_ref=references
                    )
        if "order" in data:
            for obs in data.get("order", []):
                if "ORC" in obs:
                    obj._process_service_request(
                        data=benedict(obs), source_type="ORC", source_key="ORC", resource_ref=references
                    )
                if "RXA" in obs:
                    obj._process_immunization(
                        data=benedict(obs), source_type="RXA", source_key="RXA", resource_ref=references
                    )
                if "observation" in obs:
                    for observation in obs.get("observation"):
                        if "OBX" in observation:
                            references["observation_reference"] = obj._process_observation(
                                data=benedict(observation),
                                source_type="OBX",
                                source_key="OBX",
                                resource_ref=references,
                            )
        return obj._resource_list

    def _read_raw_data(self, file_path) -> list:
        with open(file_path, "rb") as raw_file:
            self._raw_data = json.load(raw_file)

    def _process_resource(self) -> list:
        for _data in self._raw_data:
            if "MSH" in _data:
                _data = benedict(_data)
                get_msg_type = _data.get("MSH.message_type").lower()
                process_data = getattr(self, get_msg_type)
                self._fhir_data.extend(process_data(_data))
        return self._fhir_data

    def convert(self, output_file: str, metadata: dict = {}):
        try:
            parsed_file = ""
            if self._parse and self._segment_order:
                hl7_parser = Hl7Parser(self._file_path, self._message_type)
                self._read_raw_data(hl7_parser.parse())
            elif self._parse and not self._segment_order:
                hl7_parser = Hl7SegmentParser(self._message_type, AVAILABLE_SEGMENTS)
                self._raw_data = hl7_parser._process_file(self._file_path, self._canonical_file_path)
            else:
                parsed_file = self._file_path
                self._read_raw_data(parsed_file)
            self._meta_info = metadata
            resources = self._process_resource()
            bundle_id = str(uuid.uuid4())
            bundle = {"resourceType": "Bundle", "type": "batch", "id": bundle_id, "entry": resources}
            if metadata:
                write_file(bundle, output_file, json.dumps(metadata))
            else:
                write_file(bundle, output_file)
            return output_file
        except Exception as e:
            logging.error(f"Failed to convert HL7 message into FHIR. Error: {str(e)}")
            raise e
        finally:
            if parsed_file.startswith("/tmp/") and os.path.exists(parsed_file):
                os.unlink(parsed_file)
