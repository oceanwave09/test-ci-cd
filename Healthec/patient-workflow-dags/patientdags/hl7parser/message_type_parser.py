import logging
import os
import tempfile
from omniparser.parser import OmniParser
from patientdags.hl7parser.constants import HL7_CONFIG
from patientdags.hl7parser.utils import get_random_string


class Hl7Parser:
    def __init__(self, input_file: str, message_type: str, schema: str = None) -> None:
        self._input_file = input_file
        self._message_type = message_type
        self._parser_dir = os.path.dirname(os.path.abspath(__file__))
        if schema:
            self._schema = schema
        else:
            if not HL7_CONFIG.get(message_type):
                raise ValueError("Invalid HL7 message type.")
            self._schema = os.path.join(self._parser_dir, HL7_CONFIG.get(message_type))

    def parse(self, output_file: str = None):
        if not output_file:
            output_file = os.path.join(tempfile.gettempdir(), get_random_string())
        try:
            parser = OmniParser(self._schema)
            parser.transform(self._input_file, output_file)
            return output_file
        except Exception as e:
            logging.error(f"Failed to parse HL7 {self._message_type} into JSON. Error: {str(e)}")
            raise e
