import logging
import os
from pathlib import Path

import boto3
import click
import smart_open
import yaml
from botocore.exceptions import ClientError

loggerName = Path(__file__).stem

# create logging formatter
logFormatter = logging.Formatter(fmt="%(asctime)s:%(name)s:%(funcName)s:%(lineno)d - %(levelname)s - %(message)s")

# create logger
logger = logging.getLogger(loggerName)
logger.setLevel(logging.DEBUG)

# create console handler
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.DEBUG)
consoleHandler.setFormatter(logFormatter)

# Add console handler to logger
logger.addHandler(consoleHandler)

s3_client = boto3.client("s3")
action_paths = ["landing", "error", "processed"]
canonical_path = "CANONICAL"
error_report_paths = ["error_report"]


def _load_yaml(file_path: str) -> dict:
    with smart_open.open(file_path) as f:
        return yaml.safe_load(f)


def _exit_with_error(msg: str):
    logger.error(msg)
    raise ValueError(msg)


def _create_s3_path(bucket: str, path: str) -> bool:
    try:
        # check if the s3 path exists
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=path)
        is_exits = "Contents" in resp
        # create s3 path if path does not exists
        if not is_exits:
            path = f"{path}/" if not path.endswith("/") else path
            return s3_client.put_object(Bucket=bucket, Key=path)
        return is_exits
    except ClientError as e:
        logger.error(f"failed to create s3 path. bucket: {bucket}, path: {path}. Error: {str(e)}")
        raise e


def process_file_format(file_format_config: dict, client: str, practice: str, file_format: str):
    logger.info(f"initiating client: {client}, practice: {practice}, file_format: {file_format}")
    bucket = file_format_config.get("datapipeline_bucket", "")
    prefix = file_format_config.get("landing_key_prefix", "")
    formats_config = file_format_config.get("file_formats", {})
    format_config = formats_config.get(file_format, {})
    if len(format_config) == 0:
        err_msg = f"file format {file_format} config is missing!"
        _exit_with_error(err_msg)
    file_types = format_config.get("file_types") if format_config.get("file_types") else []
    has_canonical = format_config.get("canonical") if format_config.get("canonical") else False
    is_compressed = format_config.get("compressed") if format_config.get("compressed") else False
    has_error_report = format_config.get("error_report") if format_config.get("error_report") else False
    format_action_paths = action_paths
    if has_error_report:
        format_action_paths = action_paths + error_report_paths

    # source file path
    if is_compressed or len(file_types) == 0:
        for action_path in format_action_paths:
            if practice == "common":
                path = os.path.join(prefix, file_format, action_path, client)
            else:
                path = os.path.join(prefix, file_format, action_path, client, practice)
            logger.debug(f"creating s3 path, bucket: {bucket}, path: {path}")
            # _create_s3_path(bucket, path)
    else:
        for file_type in file_types:
            for action_path in format_action_paths:
                if practice == "common":
                    path = os.path.join(prefix, file_format, file_type, action_path, client)
                else:
                    path = os.path.join(prefix, file_format, file_type, action_path, client, practice)
                logger.debug(f"creating s3 path, bucket: {bucket}, path: {path}")
                # _create_s3_path(bucket, path)

    # canonical file path
    if has_canonical:
        if len(file_types) == 0:
            for action_path in format_action_paths:
                if practice == "common":
                    path = os.path.join(prefix, canonical_path, file_format, action_path, client)
                else:
                    path = os.path.join(prefix, canonical_path, file_format, action_path, client, practice)
                logger.debug(f"creating s3 path, bucket: {bucket}, path: {path}")
                # _create_s3_path(bucket, path)
        else:
            for file_type in file_types:
                for action_path in format_action_paths:
                    if practice == "common":
                        path = os.path.join(prefix, canonical_path, file_format, file_type, action_path, client)
                    else:
                        path = os.path.join(
                            prefix, canonical_path, file_format, file_type, action_path, client, practice
                        )
                    logger.debug(f"creating s3 path, bucket: {bucket}, path: {path}")
                    # _create_s3_path(bucket, path)


def initiate_landing_paths(client_config_path: str, file_format_config_path: str):
    file_format_config = _load_yaml(file_format_config_path)
    client_config = _load_yaml(client_config_path)
    # validate landing bucket
    if not file_format_config.get("datapipeline_bucket"):
        err_msg = "landing bucket should be provided!"
        _exit_with_error(err_msg)
    logger.debug(f"file_format_config: {file_format_config}")
    logger.debug(f"client_config: {client_config}")

    for client, config in client_config.items():
        for practice, file_formats in config.items():
            for file_format in file_formats:
                process_file_format(file_format_config, client, practice, file_format)


@click.command()
@click.option(
    "--client-config",
    "-c",
    help="client config file path",
    default=None,
)
@click.option(
    "--file-format-config",
    "-f",
    help="file format config file path",
    default=None,
)
def main(client_config: str, file_format_config: str):
    # validate arguments
    client_config_path = os.environ.get("CLIENT_CONFIG_PATH", client_config)
    if not client_config_path:
        err_msg = "client config path should be provided!"
        _exit_with_error(err_msg)

    file_format_config_path = os.environ.get("FILE_FORMAT_CONFIG_PATH", file_format_config)
    if not file_format_config_path:
        err_msg = "file format config path should be provided!"
        _exit_with_error(err_msg)

    initiate_landing_paths(client_config_path, file_format_config_path)


if __name__ == "__main__":
    main()
