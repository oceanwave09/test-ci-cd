import os
from datetime import date, datetime, timezone
from urllib.parse import urlparse

import dateparser
from dateutil import parser

from patientdags.cynchealth.constants import (
    ENCOUNTER_CLASS_DICT,
    GENDER_DICT,
    MARITAL_STATUS_DICT,
    OBSERVATION_RESULT_DICT,
    RMSC_RESOURCE_TYPES,
    STATUS_DICT,
)
from patientdags.utils.aws_s3 import move_file_from_s3


def parse_s3_path(path: str) -> tuple:
    result = urlparse(path)
    return (result.netloc, result.path.strip("/"))


def get_start_date(file_details):
    start_date = file_details.split("_")[1]
    split_st_date = start_date.split("-")
    return f"{int(split_st_date[0]):04d}-{int(split_st_date[1]):02d}-{int(split_st_date[2]):02d}"


def group_files_by_resources(files: list) -> dict:
    """
    filename format = "{resource}_YYYY-MM-DD_YYYY-MM-DD.csv"
    Ex: "Encounter_2023-11-01_2023-11-08.csv"
    """
    file_groups = {}
    for file in files:
        file_name = os.path.basename(file)
        file_resource = file_name.split("_")[0]
        if file_resource in RMSC_RESOURCE_TYPES:
            if file_resource not in file_groups:
                file_groups[file_resource] = []
            file_groups[file_resource].append(file)
    return file_groups


def move_file_in_s3(src_file: str, dest_file: str):
    src_bucket, src_key = parse_s3_path(src_file)
    dest_bucket, dest_key = parse_s3_path(dest_file)
    move_file_from_s3(
        src_bucket=src_bucket,
        src_key=src_key,
        dest_bucket=dest_bucket,
        dest_key=dest_key,
    )


def parse_date(value: str) -> str:
    if not value:
        return ""
    if value.isdigit():
        if len(value) == 8:
            value = f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
        elif len(value) == 12:
            value = f"{value[0:4]}-{value[4:6]}-{value[6:8]} {value[8:10]}:{value[10:12]}"
    value_datetime = dateparser.parse(value)
    if value_datetime is None:
        return ""
    return (
        value_datetime.replace(tzinfo=timezone.utc).strftime("%Y-%m-%d")
        if value_datetime.tzname() is None
        else value_datetime.strftime("%Y-%m-%d")
    )


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


def parse_marital_status(value: str) -> dict:
    value = value.upper() if value else ""
    if value == "SINGLE":
        value = "UNMARRIED"
    return MARITAL_STATUS_DICT.get(value, {})


def update_gender(value: str):
    first_char = value[0].upper() if value else ""
    return GENDER_DICT.get(first_char, "unknown")


def update_observation_result(value: str):
    value = value.upper() if value else ""
    return OBSERVATION_RESULT_DICT.get(value, "unknown")


def parse_status(value: str):
    return STATUS_DICT.get(value, "false")


def parse_encounter_class(value: str) -> dict:
    value = value.upper() if value else ""
    return ENCOUNTER_CLASS_DICT.get(value, {})
