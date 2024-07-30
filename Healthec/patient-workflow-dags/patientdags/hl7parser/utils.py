import json
import random
import string
from datetime import date, datetime, timezone
from dateutil import parser
import smart_open

from patientdags.hl7parser.constants import (
    BOOLEAN_DICT,
    CDT_CODE_SYSTEM,
    CPT_CODE_SYSTEM,
    CVX_CODE_SYSTEM,
    GENDER_DICT,
    HCPCS_CODE_SYSTEM,
    ICT_IX_CM_CODE_SYSTEM,
    ICT_X_CM_CODE_SYSTEM,
    LONIC_CODE_SYSTEM,
    MARITAL_STATUS_DICT,
    NDC_CODE_SYSTEM,
    RX_NORM_CODE_SYSTEM,
    SNOMED_CODE_SYSTEM,
)


def write_file(data: dict, filepath: str, metadata: str = ""):
    # with smart_open.open(filepath, "w") as f:
    #     json.dump(data, f, indent=4)
    if metadata:
        # add metadata to target S3 file
        transport_params = {
            "client_kwargs": {"S3.Client.create_multipart_upload": {"Metadata": json.loads(metadata)}}
        }
        dest_file = smart_open.open(filepath, "w", transport_params=transport_params)
    else:
        dest_file = smart_open.open(filepath, "w")
    json.dump(data, dest_file, indent=2)
    return


def get_random_string(size=16):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=size))


def update_gender(value: str):
    first_char = value[0].upper() if value and len(value) > 0 else "U"
    return GENDER_DICT.get(first_char, "unknown")


def parse_date_time(value: str) -> str:
    if not value:
        return ""
    try:
        value_datetime = parser.parse(value, default=datetime(date.today().year, 1, 1))
        if value_datetime is None or value_datetime < datetime(1901, 1, 1):
            return ""
        return (
            value_datetime.replace(tzinfo=timezone.utc).isoformat()
            if value_datetime.tzname() is None
            else value_datetime.isoformat()
        )
    except parser.ParserError:
        return ""


def parse_date(value: str) -> str:
    if not value:
        return ""
    try:
        value_datetime = parser.parse(value, default=datetime(date.today().year, 1, 1))
        if value_datetime is None or value_datetime < datetime(1901, 1, 1):
            return ""
        return (
            value_datetime.replace(tzinfo=timezone.utc).strftime("%Y-%m-%d")
            if value_datetime.tzname() is None
            else value_datetime.strftime("%Y-%m-%d")
        )
    except parser.ParserError:
        return ""


def parse_boolean(value: str) -> str:
    first_char = value[0].upper() if value and len(value) > 0 else "F"
    return BOOLEAN_DICT.get(first_char, "false")


def parse_marital_status(value: str) -> dict:
    value = value.upper() if value and value.upper() in MARITAL_STATUS_DICT else "UNK"
    return MARITAL_STATUS_DICT.get(value if value != "SINGLE" else "UNMARRIED", {})


def parse_code_system(value: str) -> str:
    if not value:
        return ""
    first_char = value.upper()[0:1]
    if first_char == "S":
        return SNOMED_CODE_SYSTEM
    elif first_char == "C":
        return CPT_CODE_SYSTEM
    elif first_char == "L":
        return LONIC_CODE_SYSTEM
    elif first_char == "X":
        return ICT_X_CM_CODE_SYSTEM
    elif first_char == "9":
        return ICT_IX_CM_CODE_SYSTEM
    elif first_char == "H":
        return HCPCS_CODE_SYSTEM
    elif first_char == "D":
        return CDT_CODE_SYSTEM
    else:
        return ""


def parse_code_system_pharm(value: str) -> str:
    if not value:
        return ""
    first_char = value.upper()[0:1]
    if first_char == "R":
        return RX_NORM_CODE_SYSTEM
    elif first_char == "C":
        return CVX_CODE_SYSTEM
    elif first_char == "N":
        return NDC_CODE_SYSTEM
    else:
        return ""


def to_float(value: str) -> float:
    try:
        return float(value) if value else None
    except ValueError:
        return None


def to_int(value: str) -> float:
    try:
        return int(value) if value else None
    except ValueError:
        return None


def concat_str(prefix: str, suffix: str) -> str:
    try:
        return prefix if prefix else "" + suffix if suffix else ""
    except ValueError:
        return None


def get_type_code(type_code: str):
    if type_code == "MR":
        return "mrn"
    elif type_code == "MB":
        return "member_id"
    elif type_code == "MA":
        return "medicaid_id"
    elif type_code == "MC":
        return "mbi"
    else:
        return "source_id"


def update_death_indicator(data: str):
    cast_data = data.upper()
    death_ind = ""
    if cast_data:
        first_char = cast_data[0]
        if "N" in cast_data or cast_data == "FALSE":
            first_char = "F"
        elif "Y" in cast_data or cast_data == "TRUE":
            first_char = "T"
        death_ind = BOOLEAN_DICT.get(first_char, "false")
    return death_ind
