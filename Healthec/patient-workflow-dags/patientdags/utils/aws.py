import os

from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError

from patientdags.utils.constants import S3_CONNECTION_NAME
from patientdags.utils.error_codes import PatientDagsErrorCodes, publish_error_code


def read_file_from_s3(bucket: str, key: str) -> str:
    try:
        s3_hook = S3Hook(S3_CONNECTION_NAME)
        data = s3_hook.read_key(key, bucket_name=bucket)
        return data
    except ClientError as e:
        publish_error_code(PatientDagsErrorCodes.AWS_CLIENT_ERROR.value)
        raise AirflowFailException(e)


def download_file_from_s3(bucket: str, key: str, local_path: str) -> str:
    try:
        # if data directory is not exists create data directory
        if not os.path.isdir(local_path):
            os.makedirs(local_path)
        s3_hook = S3Hook(S3_CONNECTION_NAME)
        file_path = s3_hook.download_file(key=key, bucket_name=bucket, local_path=local_path)
        return file_path
    except ClientError as e:
        publish_error_code(PatientDagsErrorCodes.AWS_CLIENT_ERROR.value)
        raise AirflowFailException(e)


def put_file_to_s3(bucket: str, key: str, data: str) -> None:
    try:
        s3_hook = S3Hook(S3_CONNECTION_NAME)
        s3_hook.load_string(data, key, bucket_name=bucket)
        return
    except ClientError as e:
        publish_error_code(PatientDagsErrorCodes.AWS_CLIENT_ERROR.value)
        raise AirflowFailException(e)


def delete_file_from_s3(bucket: str, key: str) -> None:
    try:
        s3_hook = S3Hook(S3_CONNECTION_NAME)
        s3_hook.delete_objects(bucket=bucket, keys=key)
        return
    except ClientError as e:
        publish_error_code(PatientDagsErrorCodes.AWS_CLIENT_ERROR.value)
        raise AirflowFailException(e)


def copy_file_from_s3(src_bucket: str, src_key: str, dest_bucket: str, dest_key: str = None) -> None:
    try:
        s3_hook = S3Hook(S3_CONNECTION_NAME)
        if dest_key is None:
            dest_key = src_key
        s3_hook.copy_object(
            source_bucket_name=src_bucket,
            source_bucket_key=src_key,
            dest_bucket_name=dest_bucket,
            dest_bucket_key=dest_key,
        )
        return
    except ClientError as e:
        publish_error_code(PatientDagsErrorCodes.AWS_CLIENT_ERROR.value)
        raise AirflowFailException(e)


def move_file_from_s3(src_bucket: str, src_key: str, dest_bucket: str, dest_key: str = None) -> None:
    try:
        s3_hook = S3Hook(S3_CONNECTION_NAME)
        if dest_key is None:
            dest_key = src_key
        s3_hook.copy_object(
            source_bucket_name=src_bucket,
            source_bucket_key=src_key,
            dest_bucket_name=dest_bucket,
            dest_bucket_key=dest_key,
        )
        s3_hook.delete_objects(bucket=src_bucket, keys=src_key)
        return
    except ClientError as e:
        publish_error_code(PatientDagsErrorCodes.AWS_CLIENT_ERROR.value)
        raise AirflowFailException(e)
