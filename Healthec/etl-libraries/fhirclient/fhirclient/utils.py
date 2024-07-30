# coding: utf-8


def remove_none_fields(value):
    """Recursively remove all None values from dictionaries and lists, and returns
    the result as a new dictionary or list.
    """
    if isinstance(value, list):
        return [remove_none_fields(x) for x in value if x is not None]
    elif isinstance(value, dict):
        return {
            key: remove_none_fields(val)
            for key, val in value.items()
            if val is not None
        }
    else:
        return value


def is_null_or_empty(value: str) -> None:
    """Checks the value is null or empty"""
    if value is None or len(value.strip()) == 0:
        return True
    return False
