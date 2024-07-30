import random
import string
from datetime import date, datetime, timezone

from dateutil import parser

from patientdags.ccdaparser.constants import (  # ALLERGY_CATEGORY,
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


def get_random_string(size=16):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=size))


def parse_gender(value: str):
    first_char = value[0].upper() if value and len(value) > 0 else "U"
    return GENDER_DICT.get(first_char, "unknown")


def parse_date_time(value: str) -> str:
    if not value:
        return ""
    try:
        value = value.split("-")[0]
        value_datetime = parser.parse(
            value, default=datetime(date.today().year, 1, 1, tzinfo=timezone.utc), tzinfos=[timezone.utc]
        )
        if value_datetime is None or value_datetime < datetime(1901, 1, 1, tzinfo=timezone.utc):
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
        value_datetime = parser.parse(
            value, default=datetime(date.today().year, 1, 1, tzinfo=timezone.utc), tzinfos=[timezone.utc]
        )
        if value_datetime is None or value_datetime < datetime(1901, 1, 1, tzinfo=timezone.utc):
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


def parse_telecom(value: str) -> str:
    if not value:
        return ""
    value = value.replace("tel:", "").replace("mailto:", "")
    return value


def parse_range_reference(value: str):
    range_split = value.split("-") if value and "-" in value else []
    low = str(range_split[0]).strip() if len(range_split) > 0 else ""
    high = str(range_split[1]).strip() if len(range_split) > 1 else ""
    return to_float(low), to_float(high)


# def parse_allergy_category(value: str) -> str:
#     if not value:
#         return ""
#     for key, entries in ALLERGY_CATEGORY.items():
#         for entry in entries:
#             if entry in value:
#                 return key
#     return ""
