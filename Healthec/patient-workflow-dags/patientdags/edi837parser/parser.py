import logging
import os
import tempfile

from omniparser.parser import OmniParser

from patientdags.edi837parser.constants import EDI_837P_CONFIG, EDI_837I_CONFIG
from patientdags.edi837parser.enum import ClaimType
from patientdags.edi837parser.utils import get_random_string


class EDI837Parser:
    def __init__(self, input_file: str, claim_type: str, schema: str = None) -> None:
        self._input_file = input_file
        self._claim_type = claim_type
        self._parser_dir = os.path.dirname(os.path.abspath(__file__))
        if schema:
            self._schema = schema
        else:
            if self._claim_type == ClaimType.PROFESSIONAL:
                config_file = EDI_837P_CONFIG
            elif self._claim_type == ClaimType.INSTITUTIONAL:
                config_file = EDI_837I_CONFIG
            else:
                raise ValueError("Invalid claim type")
            self._schema = os.path.join(self._parser_dir, config_file)

    def parse(self, output_file: str = None):
        if not output_file:
            output_file = os.path.join(tempfile.gettempdir(), get_random_string())
        try:
            parser = OmniParser(self._schema)
            parser.transform(self._input_file, output_file)
            return output_file
        except Exception as e:
            logging.error(f"Failed to parse 837 {self._claim_type} into JSON. Error: {str(e)}")
            raise e
