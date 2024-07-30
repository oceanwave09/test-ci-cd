from datetime import date, datetime, timedelta, timezone

from dateutil import parser
from pyspark.sql import functions as f

from utils.constants import (
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
    RACE_DICT,
    RX_NORM_CODE_SYSTEM,
    SNOMED_CODE_SYSTEM,
)

# import dateparser


def update_gender(value: str):
    first_char = value[0].upper() if value and len(value) > 0 else "U"
    return GENDER_DICT.get(first_char, "unknown")


# def parse_date_time(value: str) -> str:
#     if not value:
#         return ""
#     if value.isdigit():
#         if len(value) == 4:
#             value = f"{value}-01-01"
#         elif len(value) == 6:
#             value = f"{value[:4]}-{value[4:6]}-01"
#         elif len(value) == 8:
#             value = f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
#         elif len(value) == 12:
#             value = f"{value[0:4]}-{value[4:6]}-{value[6:8]} {value[8:10]}:{value[10:12]}"
#         elif len(value) == 14:
#             value = f"{value[0:4]}-{value[4:6]}-{value[6:8]} {value[8:10]}:{value[10:12]}:{value[13:14]}"
#     value_datetime = dateparser.parse(value)
#     if value_datetime is None:
#         return ""
#     return (
#         value_datetime.replace(tzinfo=timezone.utc).isoformat()
#         if value_datetime.tzname() is None
#         else value_datetime.isoformat()
#     )
def parse_date_time(value: str) -> str:
    if not value:
        return ""
    try:
        value_datetime = parser.parse(value, default=datetime(date.today().year, 1, 1))
        if value_datetime is None or value_datetime <= datetime(1899, 12, 31):
            return ""
        return (
            value_datetime.replace(tzinfo=timezone.utc).isoformat()
            if value_datetime.tzname() is None
            else value_datetime.isoformat()
        )
    except parser.ParserError:
        return ""


def update_race_code(value: str):
    return RACE_DICT.get(value)


# def parse_date(value: str) -> str:
#     if not value:
#         return ""
#     if value.isdigit():
#         if len(value) == 8:
#             value = f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
#         elif len(value) == 12:
#             value = f"{value[0:4]}-{value[4:6]}-{value[6:8]} {value[8:10]}:{value[10:12]}"
#     value_datetime = dateparser.parse(value)
#     if value_datetime is None:
#         return ""
#     return (
#         value_datetime.replace(tzinfo=timezone.utc).strftime("%Y-%m-%d")
#         if value_datetime.tzname() is None
#         else value_datetime.strftime("%Y-%m-%d")
#     )
def parse_date(value: str) -> str:
    if not value:
        return ""
    try:
        value_datetime = parser.parse(value, default=datetime(date.today().year, 1, 1))
        if value_datetime is None or value_datetime <= datetime(1899, 12, 31):
            return ""
        return (
            value_datetime.replace(tzinfo=timezone.utc).strftime("%Y-%m-%d")
            if value_datetime.tzname() is None
            else value_datetime.strftime("%Y-%m-%d")
        )
    except parser.ParserError:
        return ""


def transform_date_time(column):
    return f.date_format(
        f.coalesce(
            f.to_timestamp(column, "yyyy/MM/dd HH:mm:ss"),
            f.to_timestamp(column, "MM/dd/yyyy HH:mm:ss"),
            f.to_timestamp(column, "yyyy-MM-dd HH:mm:ss"),
            f.to_timestamp(column, "yyyyMMdd HH:mm:ss"),
            f.to_timestamp(column, "yyyy/MM/dd"),
            f.to_timestamp(column, "MM/dd/yyyy"),
            f.to_timestamp(column, "yyyy-MM-dd"),
            f.to_timestamp(column, "yyyyMMdd"),
            f.to_timestamp(column, "MM-dd-yyyy"),
            f.to_timestamp(column, "yyyy.MM.dd"),
            f.to_timestamp(column, "MMddyyyy"),
        ),
        "yyyy-MM-dd'T'HH:mm:ss+SS:SS",
    )


def transform_date(column):
    return f.coalesce(
        f.to_date(column, "yyyy-MM-dd"),
        f.to_date(column, "MM/dd/yyyy"),
        f.to_date(column, "yyyyMMdd"),
        f.to_date(column, "yyyy.MM.dd"),
        f.to_date(column, "yyyy/MM/dd"),
        f.to_date(column, "MM-dd-yyyy"),
        f.to_date(column, "MMddyyyy"),
        f.lit(None),
    )


def parse_boolean(value: str) -> str:
    first_char = value[0].upper() if value and len(value) > 0 else "F"
    return BOOLEAN_DICT.get(first_char, "false")


def parse_range_reference(value: str):
    range_split = value.split("-") if value and "-" in value else []
    low = str(range_split[0]).strip() if len(range_split) > 0 else ""
    high = str(range_split[1]).strip() if len(range_split) > 1 else ""
    return to_float(low), to_float(high)


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


def parse_interpretation(value: str) -> str:
    if not value:
        return {}
    interpretation = {"system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation"}
    code = ""
    display = ""
    first_char = value.upper()[0:1]
    if first_char == "H":
        code = "H"
        display = "High"
    elif first_char == "L":
        code = "L"
        display = "Low"
    elif first_char == "N":
        code = "N"
        display = "Normal"
    else:
        return {}
    interpretation.update(
        {
            "code": code,
            "display": display,
        }
    )
    return interpretation


def parse_vital_code(value: str) -> str:
    if not value:
        return {}
    vital_code = {"system": "http://loinc.org"}
    code = ""
    display = ""
    if value == "weight":
        code = "29463-7"
        display = "Body Weight"
    elif value == "height":
        code = "8302-2"
        display = "Body Height"
    elif value == "systolic":
        code = "8480-6"
        display = "Systolic Blood Pressure"
    elif value == "diastolic":
        code = "8462-4"
        display = "Diastolic Blood Pressure"
    elif value == "bmi":
        code = "39156-5"
        display = "Body Mass Index"
    else:
        return {}
    vital_code.update(
        {
            "code": code,
            "display": display,
        }
    )
    return vital_code


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


def age_to_date(value: str) -> str:
    try:
        return (datetime.today() - timedelta(int(value) * 365)).replace(day=1, month=1).strftime("%Y-%m-%d")
    except ValueError:
        return "1900-01-01"


def transform_code_system(value: str):
    return f.when(
        value.isNotNull(),
        f.when(f.upper(f.substring(value, 0, 1)).isin("S"), SNOMED_CODE_SYSTEM)
        .when(f.upper(f.substring(value, 0, 1)).isin("C"), CPT_CODE_SYSTEM)
        .when(f.upper(f.substring(value, 0, 1)).isin("L"), LONIC_CODE_SYSTEM)
        .when(f.upper(f.substring(value, 0, 1)).isin("X"), ICT_X_CM_CODE_SYSTEM)
        .when(f.upper(f.substring(value, 0, 1)).isin("9"), ICT_IX_CM_CODE_SYSTEM)
        .when(f.upper(f.substring(value, 0, 1)).isin("H"), HCPCS_CODE_SYSTEM)
        .when(f.upper(f.substring(value, 0, 1)).isin("D"), CDT_CODE_SYSTEM)
        .otherwise(f.lit("")),
    ).otherwise(f.lit(""))


def transform_code_system_pharm(value: str):
    return f.when(
        value.isNotNull(),
        f.when(f.upper(f.substring(value, 0, 1)).isin("R"), RX_NORM_CODE_SYSTEM)
        .when(f.upper(f.substring(value, 0, 1)).isin("C"), CVX_CODE_SYSTEM)
        .when(f.upper(f.substring(value, 0, 1)).isin("N"), NDC_CODE_SYSTEM)
        .otherwise(f.lit("")),
    ).otherwise(f.lit(""))


def transform_race(value: str):
    return f.when(
        value != "",
        f.when(value.isin("1"), "white")
        .when(value.isin("2"), "black")
        .when(value.isin("3"), "other")
        .when(value.isin("4"), "asian")
        .when(value.isin("5"), "hispanic")
        .when(value.isin("6"), "north American Native")
        .otherwise(f.lit("unknown")),
    ).otherwise(f.lit("unknown"))


def transform_gender(value: str):
    return f.when(
        value != "",
        f.when(f.upper(f.substring(value, 0, 1)).isin(["M", "1"]), "male")
        .when(f.upper(f.substring(value, 0, 1)).isin(["F", "2"]), "female")
        .when(f.upper(f.substring(value, 0, 1)).isin(["T", "O"]), "other")
        .otherwise(f.lit("unknown")),
    ).otherwise(f.lit("unknown"))


def transform_to_float(value: str):
    return f.when(value.isNotNull(), value.cast("float")).otherwise(None)
