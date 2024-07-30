import os
import sys
import gzip
import time
import threading
import subprocess
from smart_open import open

from omniparser.utils import read_line
from omniparser.constants import OP_BIN_PATH


class OmniParser(object):

    def __init__(self, schema_file_path: str, external_values: dict = {}) -> None:
        package_path = os.path.dirname(os.path.abspath(__file__))
        self._schema_file_path = schema_file_path
        self._bin_path = os.path.join(package_path, OP_BIN_PATH)
        self._external = ",".join([f"{k}={v}" for k, v in external_values.items()])

    def transform(self, input_file_path: str, dest_file_path: str, metadata: dict = {}, compression="disable") -> None:
        if len(metadata) > 0:
            # add metadata to target S3 file
            transport_params = {
                "client_kwargs": {"S3.Client.create_multipart_upload": {"Metadata": metadata}}
            }
            dest_file = open(dest_file_path, 'w', transport_params=transport_params, compression=compression)
        else:
            dest_file = open(dest_file_path, 'w', compression=compression)
        command = [self._bin_path, "transform", "-s", f"{self._schema_file_path}"]
        if self._external:
            command.extend(["-e", f"{self._external}"])
        parser = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=0,
            close_fds=True,
        )
        t = threading.Thread(target=self._capture_output, args=(parser, dest_file,))
        t.start()
        with read_line(input_file_path) as lines:
            for line in lines:
                parser.stdin.write(line)
                # print(f'input - {time.strftime("%Y-%m-%d %H:%M:%S")} - {line}')
                # time.sleep(.0001)
        parser.stdin.close()
        parser.wait()
        t.join()
        dest_file.close()

    def _capture_output(self, popen: subprocess.Popen, dest_file):
        for line in popen.stdout:
            # print(f'output - {time.strftime("%Y-%m-%d %H:%M:%S")} - {line}')
            dest_file.write(line)
            dest_file.flush()
            sys.stdout.flush()

    def transform_lines(self, input_data: list) -> str:
        self._parsed_data = ""
        parser = subprocess.Popen(
            [self._bin_path, "transform", "-s", f"{self._schema_file_path}"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=0,
            close_fds=True,
        )
        t = threading.Thread(target=self._capture_lines, args=(parser,))
        t.start()
        for line in input_data:
            parser.stdin.write(line)
        time.sleep(0.0001)
        parser.stdin.close()
        parser.wait()
        t.join()
        if self._parsed_data:
            return self._parsed_data

    def _capture_lines(self, popen: subprocess.Popen) -> str:
        output_json = ""
        for line in popen.stdout:
            output_json += line
        self._parsed_data = output_json

    def transform_string(self, input_data: str) -> str:
        parser = subprocess.run(
            [self._bin_path, "transform", "-s", f"{self._schema_file_path}"],
            input=input_data,
            capture_output=True,
            text=True,
        )
        return parser.stdout


if __name__ == "__main__":
    pass
