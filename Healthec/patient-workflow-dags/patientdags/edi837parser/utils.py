import random
import string

# import dateparser
from datetime import date, datetime, timezone

from dateutil import parser

from patientdags.edi837parser.constants import GENDER_DICT


def get_random_string(size=16):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=size))


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
        if value_datetime is None or value_datetime < datetime(1901, 1, 1):
            return ""
        return (
            value_datetime.replace(tzinfo=timezone.utc).isoformat()
            if value_datetime.tzname() is None
            else value_datetime.isoformat()
        )
    except parser.ParserError:
        return ""


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
        if value_datetime is None or value_datetime < datetime(1901, 1, 1):
            return ""
        return (
            value_datetime.replace(tzinfo=timezone.utc).strftime("%Y-%m-%d")
            if value_datetime.tzname() is None
            else value_datetime.strftime("%Y-%m-%d")
        )
    except parser.ParserError:
        return ""


def to_float(value: str) -> float:
    try:
        return float(value) if value else None
    except ValueError:
        return None
