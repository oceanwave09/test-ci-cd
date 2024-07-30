import logging
import os
import tempfile

from airflow.exceptions import AirflowFailException
from omniparser.parser import OmniParser

from providerdags.utils.api_client import get_data_key
from providerdags.utils.error_codes import ProviderDagsErrorCodes, publish_error_code
from providerdags.utils.utils import decrypt_file, get_random_string


def _clean_gap_file(orig_file_path: str, clean_file_path: str):
    with open(orig_file_path, "rb") as orig_file, open(clean_file_path, "wb") as clean_file:
        while True:
            chunk = orig_file.read(4096)
            if not chunk:
                break
            clean_file.write(chunk.replace(b"~,~", b'","').replace(b"~", b'"').replace(b'""', b'"'))
    return


def convert_raw_to_canonical(schema: str, src_path: str, dest_path: str, metadata: dict = {}) -> None:
    orig_file = os.path.join(tempfile.gettempdir(), f"tmp_{get_random_string(8)}")
    gap_orig_file = os.path.join(tempfile.gettempdir(), f"tmp_{get_random_string(8)}")
    try:
        pipeline_data_key = ""
        external_values = {}
        if metadata.get("file_tenant"):
            pipeline_data_key = get_data_key(metadata.get("file_tenant"))
            external_values["data_key"] = pipeline_data_key
        decrypt_file(pipeline_data_key, src_path, orig_file)
        parser = OmniParser(schema, external_values)
        if "gap" in schema.lower():
            # ISSUE: Omniparser could not support delimiter length more than 1 character
            # In GAP file, replace `~,~` with `","` and `~` with `"`
            _clean_gap_file(orig_file, gap_orig_file)
            parser.transform(gap_orig_file, dest_path, metadata, compression=".gz")
        else:
            parser.transform(orig_file, dest_path, metadata, compression=".gz")
        # bucket, key = parse_s3_path(dest_path)
        # upload_file_from_local(bucket, key, output_file, metadata)
    except AirflowFailException as e:
        raise e
    except Exception as e:
        logging.error(e)
        publish_error_code(ProviderDagsErrorCodes.RAW_TO_CANONICAL_CONVERSION_ERROR.value)
        raise AirflowFailException(e)
    finally:
        if os.path.exists(orig_file):
            os.remove(orig_file)
        elif os.path.exists(src_path):
            os.remove(src_path)
        if os.path.exists(gap_orig_file):
            os.remove(gap_orig_file)
