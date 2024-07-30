from jsonvalidator.validation import Validator
from airflow.exceptions import AirflowFailException

INPUT_DATA_TYPE = ["file", "dict"]


def validate_fields(config_file_path: str, data_file_path: str, data_type: str = "file") -> list or None:
    try:
        if config_file_path and data_file_path:
            validator, data = Validator(yaml_file=config_file_path), data_file_path
            if data_type == INPUT_DATA_TYPE[0]:
                data = validator.read_file(file_path=data, flatten=True, format="json")
            return validator.get_error_list(data=data)
        raise ValueError("File Not Found")
    except Exception as error:
        raise AirflowFailException(error)
