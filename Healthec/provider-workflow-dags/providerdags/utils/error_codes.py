from enum import Enum

from airflow.operators.python import get_current_context

# Data Ingestion specific error codes starts with `DIxxxx`


class ProviderDagsErrorCodes(Enum):
    AWS_CLIENT_ERROR = "DI0001"
    KAFKA_EVENT_PUBLISH_ERROR = "DI0002"
    RAW_TO_CANONICAL_CONVERSION_ERROR = "DI3001"
    STAGELOAD_VALIDATION_ERROR = "DI4003"


def publish_error_code(errorCode):
    context = get_current_context()
    ti = context["ti"]
    if ti.xcom_pull(key="error_code") is None:
        ti.xcom_push(key="error_code", value=errorCode)
