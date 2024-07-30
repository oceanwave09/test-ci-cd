from enum import Enum

from airflow.operators.python import get_current_context

# Data Ingestion specific error codes starts with `DIxxxx`


class PatientDagsErrorCodes(Enum):
    AWS_CLIENT_ERROR = "DI0001"
    KAFKA_EVENT_PUBLISH_ERROR = "DI0002"
    API_CLIENT_ERROR = "DI0003"
    CCD_XML_TO_JSON_ERROR = "DI1001"
    CCD_RAW_TO_CANONICAL_CONVERSION_ERROR = "DI1002"
    CCD_FILE_DATA_VALIDATION_ERROR = "DI1003"
    CCD_FILE_MANDATORY_FIELD_VALIDATION_ERROR = "DI1004"
    CCD_TO_FHIR_CONVERSION_ERROR = "DI1005"
    HL7_FILE_DATA_VALIDATION_ERROR = "DI2001"
    HL7_FILE_MANDATORY_FIELD_VALIDATION_ERROR = "DI2002"
    HL7_TO_FHIR_CONVERSION_ERROR = "DI2003"
    # FHIRBUNDLE PROCESSING ERROR CODES
    RESOURCE_VALIDATION_ERROR = "DI3001"
    RESOURCE_PROCESSING_ERROR = "DI3002"
    RAW_TO_CANONICAL_CONVERSION_ERROR = "DI3003"
    EDI837_RAW_TO_CANONICAL_CONVERSION_ERROR = "DI4001"
    EDI837_FILE_DATA_VALIDATION_ERROR = "DI4002"
    EDI837_FILE_MANDATORY_FIELD_VALIDATION_ERROR = "DI4003"
    EDI837_TO_FHIR_CONVERSION_ERROR = "DI4004"


def publish_error_code(error_code):
    context = get_current_context()
    ti = context["ti"]
    if ti.xcom_pull(key="error_code") is None:
        ti.xcom_push(key="error_code", value=error_code)
