import os
import json
import logging
import copy
import uuid
from smart_open import open
from benedict import benedict
from patientdags.hl7parser.constants import (
    SCHEMAS,
    SEGMENT_SCHEMA,
    DEFAUTL_SEQ_INDEX,
    SEQUENCE_IDENTIFIER,
    MSG_TYPE_IDEN_INDEX,
)
from patientdags.hl7parser.utils import write_file
from omniparser.parser import OmniParser


class Hl7SegmentParser:
    IDENTIFIER_KEY = "patient_sequence"

    def __init__(self, schema, available_segements) -> None:
        # schema parameter not used now
        # if "ADT" in schema:
        #     self._schema = SCHEMAS["ADT_XXX"]
        # elif "ORU" in schema:
        #     self._schema = SCHEMAS["ORU_R01"]
        # elif "VXU" in schema:
        #     self._schema = SCHEMAS["VXU_V04"]
        self._schema = schema
        package_path = os.path.dirname(os.path.abspath(__file__))
        self._schema_path = os.path.join(package_path, SEGMENT_SCHEMA)
        self._transformed_data = []
        self._available_segements = available_segements
        self._dict_list = {}
        self._patient_count = 0
        self._msg_typ_indicator = {}
        self._segment_path = self._get_seg_path_attribute()

    def _update_msg_type(self, msg_type: str) -> str:
        return "ADT_XXX" if "ADT" in msg_type else msg_type

    def _get_seg_path_attribute(self):
        _path_attr = {}
        for _schema in SCHEMAS:
            _path_attr[_schema] = self._get_segment_path(SCHEMAS[_schema])
        return _path_attr

    def _update_patient_seq(row: list, seq: str, join_delimeter: str = "|") -> str:
        row[DEFAUTL_SEQ_INDEX:DEFAUTL_SEQ_INDEX] = [str(seq)]
        return f"{join_delimeter}".join(row)

    def _raw_to_canonical(self, data: dict = {}) -> list:
        try:
            canonical_data = []
            for seg in data:
                parser = OmniParser(self._schema_path.format(seg))
                parsed_data = json.loads(parser.transform_lines(data.get(seg)))
                if isinstance(parsed_data[0][seg], list):
                    parsed_data[0][seg].sort(key=lambda i: int(i.get("set_id", 1)))
                canonical_data.extend(parsed_data)
            return canonical_data
        except Exception as e:
            logging.error(f"Error, Raw to canonical conversion - {str(e)}")

    def _group_segments(self, lines: list) -> dict:
        segment_groups = {}
        try:
            for line in lines:
                split_line = line.split("|")
                get_segment = split_line[0]
                if get_segment.upper() == SEQUENCE_IDENTIFIER:
                    get_msg_header = split_line[MSG_TYPE_IDEN_INDEX].split("^")
                    self._msg_typ_indicator.update({self._patient_count: f"{get_msg_header[0]}_{get_msg_header[1]}"})
                    self._patient_count = self._patient_count + 1
                if get_segment in self._available_segements:
                    if get_segment not in segment_groups:
                        segment_groups[get_segment] = []
                    updated_line = Hl7SegmentParser._update_patient_seq(split_line, self._patient_count - 1)
                    segment_groups[get_segment].append(updated_line)
        except Exception as e:
            logging.error(f"Error, Grouping segments - {str(e)}")
        return segment_groups

    def _get_segment_path(self, schema: dict, exact_path: list = None, segment_route: dict = None) -> dict:
        try:
            exact_path = [] if not exact_path else exact_path
            segment_route = {} if not segment_route else segment_route
            schema = schema[0] if isinstance(schema, list) else schema
            for segment in schema:
                temp_path = []
                exact_path.append(segment)
                if len(schema[segment]):
                    if isinstance(schema[segment], list):
                        self._dict_list.update({segment: schema[segment]})
                    self._get_segment_path(schema[segment], exact_path, segment_route)
                else:
                    temp_path = copy.copy(exact_path)
                    if segment in segment_route:
                        segment_route[segment].append(temp_path)
                    else:
                        segment_route[segment] = [temp_path]
                exact_path.pop()
            return segment_route
        except Exception as e:
            logging.error(f"Error, Getting segment path - {str(e)}")

    def _update_path_index(self, abs_path: str, relative_path: str, key: str, seq: int = 0) -> str:
        temp_abs = abs_path
        try:
            if relative_path and isinstance(self._transformed_data[seq][relative_path], list):
                update_flag = False
                for _index in range(len(self._transformed_data[seq][relative_path]) - 1):
                    if update_flag:
                        break
                    if isinstance(self._transformed_data[seq][abs_path], dict):
                        if not self._transformed_data[seq][relative_path][_index][key]:
                            temp_abs = f"{relative_path}[{_index}].{key}"
                            break
                    else:
                        temp_abs = f"{relative_path}[{_index}].{key}"
                        update_flag = True
        except Exception as e:
            logging.error(f"Error, Update path index - {str(e)}")
        return temp_abs

    def _map_data_to_schema(self, abs_path: str, relative_path: str, key: str, data: dict, seq: int = 0) -> None:
        try:
            if isinstance(self._transformed_data[seq][abs_path], list):
                self._transformed_data[seq][abs_path].append(data)
            else:
                if self._transformed_data[seq][abs_path]:
                    get_len = len(self._transformed_data[seq][relative_path])
                    temp_path = f"{relative_path}[{get_len}]"
                    abs_path = f"{temp_path}.{key}"
                    get_sub_key = relative_path.split(".")[-1]
                    self._transformed_data[seq][temp_path] = copy.deepcopy(self._dict_list[get_sub_key][0])
                self._transformed_data[seq][abs_path] = data
        except Exception as e:
            logging.error(f"Error, Map data to schema - {str(e)}")
            raise e

    def _generate_abs_path(self, path_list: list, seq: int, data: dict) -> None:
        try:
            temp_path, abs_path, relative_path, key = "", "", "", ""
            if len(path_list) == 1:
                abs_path = path_list[0]
            else:
                key = path_list[-1]
                for path_key in path_list[:-1]:
                    if not temp_path:
                        if isinstance(self._transformed_data[seq][path_key], list):
                            get_len = len(self._transformed_data[seq][path_key]) - 1
                            relative_path = temp_path + f"{path_key}"
                            temp_path = f"{relative_path}[{get_len}]"
                        else:
                            temp_path = path_key
                    else:
                        if isinstance(self._transformed_data[seq][f"{temp_path}.{path_key}"], list):
                            get_len = len(self._transformed_data[seq][f"{temp_path}.{path_key}"]) - 1
                            relative_path = temp_path + f".{path_key}"
                            temp_path = f"{relative_path}[{get_len}]"
                        else:
                            temp_path = temp_path + "." + path_key
                abs_path = temp_path + "." + key
                abs_path = self._update_path_index(abs_path, relative_path, key, seq)
            self._map_data_to_schema(abs_path, relative_path, key, data, seq)
        except Exception as e:
            logging.error(f"Error, Construct path- {str(e)}")

    def _get_seg_attr(self, seq: int, segment: str):
        get_msg_type = self._msg_typ_indicator[seq]
        get_msg_type = self._update_msg_type(get_msg_type)
        return self._segment_path[get_msg_type][segment]

    def _update_seg_attr(self, seq: int, segment: str, next_seg_attr: str):
        get_msg_type = self._msg_typ_indicator[seq]
        get_msg_type = self._update_msg_type(get_msg_type)
        return self._segment_path[get_msg_type][segment][next_seg_attr]

    def _extract_canonical_data(self, canonical_data: list) -> None:
        try:
            seg_cnt, seq = {}, 0
            for seg_group in canonical_data:
                for segment, seg_data in seg_group.items():
                    if isinstance(seg_data, list):
                        # get_path = self._segment_path[segment][0]
                        for data in seg_data:
                            get_index = data.get("set_id", "")
                            seq = int(data.get(self.IDENTIFIER_KEY, 0))
                            get_path = self._get_seg_attr(seq, segment)
                            get_seg_path = get_path[0]
                            if seq not in seg_cnt:
                                seg_cnt[seq] = {}
                            get_index = 0 if get_index == "" else int(get_index) - 1
                            if seq in seg_cnt and segment in seg_cnt[seq] and get_index in seg_cnt[seq][segment]:
                                seg_cnt[seq][segment][get_index] = seg_cnt[seq][segment][get_index] + 1
                                if len(get_path) - 1 >= seg_cnt[seq][segment][get_index]:
                                    next_key_attr = seg_cnt[seq][segment][get_index]
                                    get_seg_path = self._update_seg_attr(seq, segment, next_key_attr)
                            if segment not in seg_cnt[seq]:
                                seg_cnt[seq] = {segment: {get_index: 0}}
                            elif segment in seg_cnt[seq] and get_index not in seg_cnt[seq][segment]:
                                seg_cnt[seq][segment].update({get_index: 0})
                            self._generate_abs_path(get_seg_path, seq, data)
                    else:
                        get_path = self._get_seg_attr(seq, segment)[0]
                        seq = int(seg_data.get(self.IDENTIFIER_KEY, 0))
                        self._generate_abs_path(get_path, seq, seg_data)
        except Exception as e:
            logging.error(f"Error, Extract canonical data - {str(e)}")
            raise e

    def _add_schema_based_on_patient_count(self):
        for _count in range(0, self._patient_count):
            get_msg_type = self._msg_typ_indicator[_count]
            get_msg_type = self._update_msg_type(get_msg_type)
            self._transformed_data.append(benedict(copy.deepcopy(SCHEMAS[get_msg_type])))
            self._transformed_data[_count].update({"row_id": str(uuid.uuid4())})

    def _process_file(self, input_file: str, canonical_path: str = None) -> str:
        with open(input_file, "r") as read_file:
            get_lines = read_file.readlines()
            get_grouped_data = self._group_segments(get_lines)
            get_canonical_data = self._raw_to_canonical(get_grouped_data)
            if canonical_path:
                write_file(get_canonical_data, canonical_path)
            self._add_schema_based_on_patient_count()
            self._extract_canonical_data(get_canonical_data)
        return self._transformed_data


if __name__ == "__main__":
    pass
