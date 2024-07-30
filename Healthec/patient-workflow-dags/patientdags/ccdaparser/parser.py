import json
import logging
import os
import re
import tempfile
import xml.etree.ElementTree as ET
from collections import Counter

import simplejson
import smart_open
import xmlschema
from omniparser.parser import OmniParser

from patientdags.ccdaparser.constants import (
    CCDA_CONFIG,
    XML_SCHEMA,
    InputFormat,
    ReplaceSign,
)
from patientdags.ccdaparser.utils import get_random_string


class CcdaParser:
    def __init__(self, input: str, input_format: str = InputFormat.FILE) -> None:
        self._input = input
        self._input_format = input_format
        self._parser_dir = os.path.dirname(os.path.abspath(__file__))

    def _replace_keys(self, obj):
        if isinstance(obj, str):
            return obj
        if isinstance(obj, dict):
            new = obj.__class__()
            for k, v in obj.items():
                new[
                    k[k.startswith(ReplaceSign.AT) and len(ReplaceSign.AT) :].replace(  # E203
                        ReplaceSign.DOLLAR, "value"
                    )
                ] = self._replace_keys(v)
        elif isinstance(obj, list):
            new = obj.__class__(self._replace_keys(v) for v in obj)
        else:
            return obj
        return new

    def _update_reference_value(self, src_data: str, data: str, input_format: str = InputFormat.FILE) -> str:
        if input_format == InputFormat.FILE:
            root = ET.fromstring(src_data)
        else:
            tree = ET.parse(src_data)
            root = tree.getroot()
        matches = re.findall(r"\"reference\":\s*{\"value\":\s*\"#(.*?)\"}", data)

        # OLD LOGIC
        # for match in matches:
        #     if not match:
        #         continue
        #     id_xpath = ".//*[@ID='{}']".format(match)
        #     if root.find(id_xpath):
        #         value = root.find(id_xpath).text
        #         old_str = '"reference": {"value": "#' + match + '"}'
        #         new_str = '"value": "' + value.replace("\n", "").strip() + '"' if value else '"value": ""'
        #         data = data.replace(old_str, new_str)
        for match in matches:
            if not match:
                continue
            parent_values = {}
            ref_parent_xpath = ".//*[@value='#{}']...".format(match)
            if root.find(ref_parent_xpath) is not None:
                parent_elems = root.findall(ref_parent_xpath)
                for idx, elem in enumerate(parent_elems):
                    parent_values[idx] = str(elem.text).strip("\n").strip()
            id_xpath = ".//*[@ID='{}']".format(match)
            if root.find(id_xpath) is not None:
                value = str(root.find(id_xpath).text).strip("\n").strip()
            if value:
                old_str = '"reference": {"value": "#' + match + '"}'
                new_str = '"value": "' + value.replace('"', '\\"') + '"'
                data = data.replace(old_str, new_str)
            else:
                for idx, val in parent_values.items():
                    old_str = '"reference": {"value": "#' + match + '"}'
                    new_str = '"value": "' + val.replace('"', '\\"') + '"'
                    data = data.replace(old_str, new_str, 1)
        return data

    def _parse_sections(self, data, raw_data, input_format) -> str:
        ccda_data = self._replace_keys(data)
        parsed_ccda_data = {}
        # Add patient
        patient_role = (
            ccda_data.get("recordTarget", [])[0].get("patientRole", {})
            if len(ccda_data.get("recordTarget", [])) > 0
            else {}
        )
        parsed_ccda_data.update({"patient": patient_role})
        # Add provider organization
        custodian = ccda_data.get("custodian", {})
        parsed_ccda_data.update({"practice": custodian})
        # Add providers
        providers = []
        # if ccda_data.get("legalAuthenticator"):
        #     providers.append(ccda_data.get("legalAuthenticator"))
        if len(ccda_data.get("documentationOf", [])) > 0:
            documents = ccda_data.get("documentationOf", [])
            for document in documents:
                if len(document.get("serviceEvent", {}).get("performer", [])) > 0:
                    providers.extend(document.get("serviceEvent", {}).get("performer", []))
        npi_provider = []
        for provider in providers:
            ids = provider.get("assignedEntity", {}).get("id", [])
            for id in ids:
                if id.get("root", "") == "2.16.840.1.113883.4.6" and id.get("extension", ""):
                    npi_provider.append(provider)
                    break
        parsed_ccda_data.update({"providers": npi_provider})

        # CCDA Sections
        ccda_sections = {
            "2.16.840.1.113883.10.20.22.2.6.1": "allergies",
            "2.16.840.1.113883.10.20.22.2.1.1": "medications",
            "2.16.840.1.113883.10.20.22.2.5.1": "problems",
            "2.16.840.1.113883.10.20.22.2.20": "past_medical_histories",
            "2.16.840.1.113883.10.20.22.2.7.1": "procedures",
            "2.16.840.1.113883.10.20.22.2.3.1": "results",
            "2.16.840.1.113883.10.20.22.2.21": "advance_directives",
            "2.16.840.1.113883.10.20.22.2.22": "encounters",
            "2.16.840.1.113883.10.20.22.2.22.1": "encounters",
            "2.16.840.1.113883.10.20.22.2.15": "family_history",
            "2.16.840.1.113883.10.20.22.2.14": "functional_status",
            "2.16.840.1.113883.10.20.22.2.2.1": "immunizations",
            "2.16.840.1.113883.10.20.22.2.23": "medical_equipment",
            "2.16.840.1.113883.10.20.22.2.18": "payers",
            "2.16.840.1.113883.10.20.22.2.10": "plan_of_treatment",
            "2.16.840.1.113883.10.20.22.2.17": "social_history",
            "2.16.840.1.113883.10.20.22.2.4.1": "vital_signs",
            "2.16.840.1.113883.10.20.22.2.56": "mental_status",
            "2.16.840.1.113883.10.20.22.2.57": "nutrition",
        }

        # filter sections based on entry availability
        components = ccda_data.get("component", {}).get("structuredBody", {}).get("component", [])

        for component in components:
            # skip component if there is no entry
            if len(component.get("section", {}).get("entry", [])) == 0:
                continue
            # identify section
            section_name = ""
            templates = component.get("section", {}).get("templateId")
            for template in templates:
                root = template.get("root", "")
                if root in ccda_sections:
                    section_name = ccda_sections[root]
            if section_name:
                parsed_ccda_data.update({section_name: component.get("section", {}).get("entry", [])})
        final_ccda_data = self._update_reference_value(raw_data, simplejson.dumps(parsed_ccda_data), input_format)
        return final_ccda_data

    def _apply_defined_correction(self, data: str) -> str:
        defined_repl = {
            'xmlns:cda="urn:hl7-org:v3"': "",
            "<guardian>(.|\n)*<\\/guardian>": "",
            "<birthplace>(.|\n)*<\\/birthplace>": "",
            '""': '"None"',
            '<entryRelationship.*"COMP"\\/>': "",
        }
        for k, v in defined_repl.items():
            data = re.sub(k, v, data)
        return data

    def _handle_duplicate_id(self, data: str) -> str:
        tag_ids = re.findall(r"(?i)ID=\".*?\"", data)
        tag_id_counter = Counter(tag_ids)
        for tag_id, count in tag_id_counter.items():
            if count > 1:
                tag_id_group = re.search(r"(?i)ID=\"(.*?)\"", tag_id)
                if tag_id_group:
                    tag_id_value = tag_id_group.group(1)
                    for i in range(count):
                        tag_id_value_new = f"{tag_id_value}-{get_random_string(3)}"
                        data = re.sub(tag_id, str(tag_id).replace(tag_id_value, tag_id_value_new), data, 1)
                        if i == 0:
                            data = re.sub(f"#{tag_id_value}", f"#{tag_id_value_new}", data)
        return data

    def _read_data(self, file_path: str) -> str:
        data = ""
        with smart_open.open(file_path, "r") as f:
            data = f.read()
        data = self._handle_duplicate_id(data)
        return self._apply_defined_correction(data)

    def _parse_fields(self, src_file: str, dest_file: str, schema: str = None):
        if not schema:
            schema = os.path.join(self._parser_dir, CCDA_CONFIG)
        parser = OmniParser(schema)
        parser.transform(src_file, dest_file)
        return dest_file

    def parse(self, output_file: str = None):
        interm_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
        if not output_file:
            output_file = os.path.join(tempfile.gettempdir(), get_random_string())
        try:
            xs = xmlschema.XMLSchema(os.path.join(self._parser_dir, XML_SCHEMA))
            if self._input_format == InputFormat.FILE:
                xml_data = self._read_data(self._input)
            else:
                xml_data = self._input
            dict_data = xs.to_dict(xml_data, validation="lax")[0]
            parsed_data = self._parse_sections(data=dict_data, raw_data=xml_data, input_format=self._input_format)
            # remove control char
            parsed_data = parsed_data.replace("\n", "").replace("\t", "").replace("\r", "").replace("\0", "")
            with interm_file:
                simplejson.dump(json.loads(parsed_data), interm_file, indent=2)
            interm_file.close()
            self._parse_fields(interm_file.name, output_file)
            return output_file
        except Exception as e:
            logging.error("Failed to parse CCD XML into JSON. Error:", str(e))
            raise e
        finally:
            if os.path.exists(interm_file.name):
                os.unlink(interm_file.name)
