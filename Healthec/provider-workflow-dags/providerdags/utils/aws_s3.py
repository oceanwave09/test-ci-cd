import os

import boto3
from airflow.exceptions import AirflowFailException
from botocore.exceptions import ClientError

from providerdags.utils.error_codes import ProviderDagsErrorCodes, publish_error_code


def read_file_from_s3(bucket: str, key: str) -> tuple:
    try:
        client = boto3.client("s3")
        response = client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8"), response["Metadata"]
    except ClientError as e:
        publish_error_code(f"{ProviderDagsErrorCodes.AWS_CLIENT_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def get_file_metadata_from_s3(bucket: str, key: str) -> dict:
    try:
        client = boto3.client("s3")
        response = client.head_object(Bucket=bucket, Key=key)
        return response["Metadata"]
    except ClientError as e:
        publish_error_code(f"{ProviderDagsErrorCodes.AWS_CLIENT_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def write_file_to_s3(data: str, bucket: str, key: str, metadata: dict = {}) -> bool:
    try:
        client = boto3.client("s3")
        client.put_object(Body=data, Metadata=metadata, Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        publish_error_code(f"{ProviderDagsErrorCodes.AWS_CLIENT_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def download_file_from_s3(bucket: str, key: str, local_path: str) -> tuple:
    try:
        # if data directory is not exists create data directory
        if not os.path.isdir(local_path):
            os.makedirs(local_path)
        local_file_path = os.path.join(local_path, os.path.basename(key))
        client = boto3.client("s3")
        client.download_file(Bucket=bucket, Key=key, Filename=local_file_path)
        response = client.head_object(Bucket=bucket, Key=key)
        return local_file_path, response["Metadata"]
    except ClientError as e:
        publish_error_code(f"{ProviderDagsErrorCodes.AWS_CLIENT_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def upload_file_from_local(bucket: str, key: str, local_path: str, metadata: dict = {}) -> bool:
    try:
        # check if the file exists
        if not os.path.exists(local_path):
            raise ValueError(f"Failed to upload the file into S3. File '{local_path}' does not exists.")
        client = boto3.client("s3")
        client.upload_file(Filename=local_path, Bucket=bucket, Key=key, ExtraArgs={"Metadata": metadata})
        return True
    except ClientError as e:
        publish_error_code(f"{ProviderDagsErrorCodes.AWS_CLIENT_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def delete_file_from_s3(bucket: str, key: str) -> bool:
    try:
        client = boto3.client("s3")
        client.delete_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        publish_error_code(f"{ProviderDagsErrorCodes.AWS_CLIENT_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def copy_file_from_s3(src_bucket: str, src_key: str, dest_bucket: str, dest_key: str) -> bool:
    try:
        client = boto3.client("s3")
        client.copy_object(Bucket=dest_bucket, Key=dest_key, CopySource={"Bucket": src_bucket, "Key": src_key})
        return True
    except ClientError as e:
        publish_error_code(f"{ProviderDagsErrorCodes.AWS_CLIENT_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def move_file_from_s3(src_bucket: str, src_key: str, dest_bucket: str, dest_key: str) -> bool:
    try:
        client = boto3.client("s3")
        client.copy_object(Bucket=dest_bucket, Key=dest_key, CopySource={"Bucket": src_bucket, "Key": src_key})
        client.delete_object(Bucket=src_bucket, Key=src_key)
        return True
    except ClientError as e:
        publish_error_code(f"{ProviderDagsErrorCodes.AWS_CLIENT_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)
