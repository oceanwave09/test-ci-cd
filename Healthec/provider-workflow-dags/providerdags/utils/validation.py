from airflow.exceptions import AirflowFailException
from jsonvalidator.validation import Validator

from providerdags.utils.error_codes import ProviderDagsErrorCodes, publish_error_code

INPUT_DATA_TYPE = ["file", "dict"]


def validate_fields(config_file_path: str, data_file_path: str, data_type: str = "file") -> list or None:
    try:
        if config_file_path and data_file_path:
            validator, data = Validator(yaml_file=config_file_path), data_file_path
            if data_type == INPUT_DATA_TYPE[0]:
                data = validator.read_file(file_path=data, flatten=True, format="json")
            return validator.get_error_list(data=data)
        raise ValueError("Data or config file not found")
    except Exception as e:
        publish_error_code(f"{ProviderDagsErrorCodes.STAGELOAD_VALIDATION_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)
