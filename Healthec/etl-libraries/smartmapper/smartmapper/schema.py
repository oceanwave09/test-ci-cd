import os
import yaml
from smart_open import open


class Schema:
    """ A class used to represent a schema to determine the parsing of incoming flat or
    delimiter data file.
    """

    __DELIMITER = "delimiter"
    __IS_DELIMTED_FILE_FLAG = "is_delimited_file"
    __HAS_HEADER = "has_header"
    __FIELD_MAPPINGS = "field_mappings"
    __VALIDATION_RULES = "validation_rules"
    __INCLUDE_FIELDS = "include_fields"
    __EXCLUDE_FIELDS = "exclude_fields"

    __DEFAULT_IS_DELIMTED_FILE_FLAG = False
    __DEFAULT_DELIMITER = ","
    __DEFAULT_HAS_HEADER_FLAG = False

    def __init__(self, file):
        self._load(file)

    @property
    def is_delimited_file(self):
        """ getter for attribute '_is_delimited_file' """
        return self._is_delimited_file

    @property
    def delimiter(self):
        """ getter for attribute '_delimiter' """
        return self._delimiter

    @property
    def has_header(self):
        """ getter for attribute '_has_header' """
        return self._has_header

    @property
    def field_mappings(self):
        """ getter for attribute 'field_mappings' """
        return self._field_mappings

    @property
    def validation_rules(self):
        """ getter for attribute 'validation_rules' """
        return self._validation_rules

    @property
    def include_fields(self):
        """ getter for attribute 'include_fields' """
        return self._include_fields

    @property
    def exclude_fields(self):
        """ getter for attribute 'exclude_fields' """
        return self._exclude_fields

    def _load(self, file):
        """ loads schema from file """
        attrs = yaml.load(file, Loader=yaml.SafeLoader)

        self._is_delimited_file = attrs.get(self.__IS_DELIMTED_FILE_FLAG,
                                            self.__DEFAULT_IS_DELIMTED_FILE_FLAG)
        self._delimiter = attrs.get(self.__DELIMITER, self.__DEFAULT_DELIMITER)
        self._has_header = attrs.get(
            self.__HAS_HEADER, self.__DEFAULT_HAS_HEADER_FLAG)
        self._field_mappings = attrs.get(self.__FIELD_MAPPINGS, {})
        self._validation_rules = attrs.get(self.__VALIDATION_RULES, [])
        self._include_fields = attrs.get(self.__INCLUDE_FIELDS, [])
        self._exclude_fields = attrs.get(self.__EXCLUDE_FIELDS, [])

    @classmethod
    def load_from_file(cls, filepath: str):
        """ loads schema from filepath """
        # if not os.path.exists(filepath):
        #     raise ValueError("Invalid schema filepath")
        with open(filepath, "r", encoding="utf8") as file:
            return cls(file)
