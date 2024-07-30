import re
from smart_open import open
from benedict import benedict


class FieldValidators:
    """
    Common function that are support to valdation
    func:
        Read_file
        Validation functions
    """

    def __init__(self) -> None:
        """
        params:
            None
        Static values needed to validation
        """
        self.available_data_types = {
            "string": [str],
            "number": [int, float],
            "dict": [dict],
            "list": [list],
            "boolean": [bool],
        }
        self.available_length_types = ["max_length", "min_length", "exact_length"]

    def read_file(self, file_path, flatten=False, format="json") -> list:
        """
        params:
            file_path : str
                Absolute file path to read
            flatten : bool - optional
                Helps to flattern file
            format: str -optional
                Helps to flattern and return extact value list
        """
        with open(file_path, "r") as f:
            data = f.read()
        if flatten:
            data = benedict(data, format=format)
            if format == "json":
                data = data.get("values", [])
        return data

    def data_length_validation(self, data, required_length, length_type) -> bool:
        """
        params:
            data: str
                Data need to validate
            required_length: int
                Required length
            length_type : str
                Static length types
        return: bool
            Condition satisfaction
        """
        if length_type in self.available_length_types[0:1]:
            return len(data) <= required_length
        elif length_type in self.available_length_types[1:2]:
            return len(data) >= required_length
        else:
            return len(data) == required_length

    def data_type_validation(self, data, data_type) -> bool:
        """
        params:
            data: str
                Data need to validate
            required_length: int
                Required length
            length_type : str
                Static length types
        return: bool
            Condition satisfaction
        """
        return type(data) in self.available_data_types[data_type]

    def data_manditory_validation(self, data) -> bool:
        """
        params:
            data: str
                Data need to validate
        return: bool
            Condition satisfaction
        """
        return len(data) != 0 or data is not None

    def data_choice_validation(self, data, choices) -> bool:
        """
        params:
            data: str
                Data need to validate
            choices: list
                Available choices
        return: bool
            Condition satisfaction
        """
        return data.lower() in [choice.lower() for choice in choices]

    def data_pattern_validation(self, data, pattern) -> bool:
        """
        params:
            data: str
                Data need to validate
            Pattern: str
                Validation pattern
        return: bool
            Condition satisfaction
        """
        return re.match(pattern, data)


class Validator(FieldValidators):
    """
    Validate data using FieldValidators funcion
    """

    def __init__(self, yaml_file, validation_key_path="key_path") -> None:
        """
        params:
            yaml_file: str
                Absolute file path to get list of validation
            validation_key_path: str
                json_key contains contains which key to validate
        return:
            None
        """
        super().__init__()
        self.validators = self.read_file(
            file_path=yaml_file, flatten=True, format="yaml"
        )
        self.key_path = validation_key_path
        self.error_msg = {}

    def update_error_list(self, key, value):
        """
        Aggregating the error
        params:
            key : Which field failed the validation
            value: Validation type
        """
        if key in self.error_msg:
            self.error_msg[key].append(value)
        else:
            self.error_msg[key] = [value]

    def get_row_id(self, row) -> str or None:
        get_key, row_id = list(row.keys()), None
        try:
            if len(get_key) == 1 and row[get_key[0]].get("row_id") is not None:
                row_id = row[get_key[0]]["row_id"]
            elif len(get_key) > 1:
                row_id = row["row_id"]
        except Exception as e:
            pass
        return row_id

    def validate(self, row) -> dict:
        """
        params:
            row: dict
                dict data to validate
        return: dict
            Based on multiple validation, Each validation error appended to error_type key
        """
        validation_result = {"error": {}, "row_id": ""}
        for validator in self.validators:
            key = self.validators[validator][self.key_path]
            try:
                if self.validators[validator].get(
                    "max_length"
                ) is not None and not self.data_length_validation(
                    data=row[key],
                    required_length=self.validators[validator].get("max_length"),
                    length_type="max_length",
                ):
                    self.update_error_list(key=key, value="Field Max-Length Mismatch")
                if self.validators[validator].get(
                    "min_length"
                ) is not None and not self.data_length_validation(
                    data=row[key],
                    required_length=self.validators[validator].get("min_length"),
                    length_type="min_length",
                ):
                    self.update_error_list(key=key, value="Field Min-Length Mismatch")
                if self.validators[validator].get(
                    "length"
                ) is not None and not self.data_length_validation(
                    data=row[key],
                    required_length=self.validators[validator].get("length"),
                    length_type="length",
                ):
                    self.update_error_list(key=key, value="Field Length Mismatch")
                if self.validators[validator].get(
                    "type"
                ) is not None and not self.data_type_validation(
                    data=row[key], data_type=self.validators[validator]["type"]
                ):
                    self.update_error_list(key=key, value="Field Type Mismatch")
                if self.validators[validator].get(
                    "pattern"
                ) is not None and not self.data_pattern_validation(
                    data=row[key], pattern=self.validators[validator]["pattern"]
                ):
                    self.update_error_list(key=key, value="Field Pattern Mismatch")
                if self.validators[validator].get(
                    "required"
                ) is not None and not self.data_manditory_validation(data=row[key]):
                    self.update_error_list(key=key, value="Missing Required Field")
                if self.validators[validator].get(
                    "choices"
                ) is not None and not self.data_choice_validation(
                    data=row[key], choices=self.validators[validator]["choices"]
                ):
                    self.update_error_list(key=key, value="Choice Not Contains")
            except Exception as e:
                continue
        if self.error_msg:
            (
                validation_result["error"],
                validation_result["row_id"],
            ) = self.error_msg, self.get_row_id(row=row)
        return validation_result

    def construct_error(self, data) -> dict:
        """
        params:
            data:
                input data to be validate
        return: dict
            error from validate function
        """
        self.error_msg = {}
        validation_response = self.validate(row=data)
        return validation_response

    def get_error_list(self, data) -> list:
        """
        params:
            data:
                data to be validate
        return: list
            error_list from collected error
        """
        error_list = []
        if isinstance(data, list):
            for row in data:
                get_error = self.construct_error(data=row)
                error_list.append(get_error) if get_error["error"] else ""
        elif isinstance(data, dict):
            get_error = self.construct_error(data=row)
            error_list.append(get_error) if get_error["error"] else ""
        return error_list


if __name__ == "__main__":
    pass
