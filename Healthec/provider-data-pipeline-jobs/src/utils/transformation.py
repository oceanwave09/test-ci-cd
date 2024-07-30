from datetime import timezone

import dateparser
from pyspark.sql import functions as f


def parse_date_time(value: str) -> str:
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
        value_datetime.replace(tzinfo=timezone.utc).isoformat()
        if value_datetime.tzname() is None
        else value_datetime.isoformat()
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
