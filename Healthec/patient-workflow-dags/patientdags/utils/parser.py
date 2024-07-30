import logging
import os
import tempfile

from airflow.exceptions import AirflowFailException
from omniparser.parser import OmniParser

from patientdags.utils.aws_s3 import upload_file_from_local
from patientdags.utils.error_codes import PatientDagsErrorCodes, publish_error_code
from patientdags.utils.utils import get_random_string, parse_s3_path, get_data_key


def convert_raw_to_canonical(schema: str, src_path: str, dest_path: str, metadata: dict = {}) -> None:
    output_file = os.path.join(tempfile.gettempdir(), f"tmp_{get_random_string(8)}")
    try:
        pipeline_data_key = ""
        external_values = {}
        if metadata.get("file_tenant"):
            pipeline_data_key = get_data_key(metadata.get("file_tenant"))
            external_values["data_key"] = pipeline_data_key
        parser = OmniParser(schema, external_values)
        parser.transform(src_path, output_file)
        bucket, key = parse_s3_path(dest_path)
        upload_file_from_local(bucket, key, output_file, metadata)
    except AirflowFailException as e:
        raise e
    except Exception as e:
        logging.error(e)
        publish_error_code(PatientDagsErrorCodes.RAW_TO_CANONICAL_CONVERSION_ERROR.value)
        raise AirflowFailException(e)
    finally:
        if os.path.exists(output_file):
            os.remove(output_file)
