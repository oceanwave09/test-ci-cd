from airflow.models import Connection, Variable

# Airflow details
AIRFLOW_ENDPOINT = "http://airflow-webserver.data-pipeline.svc.cluster.local:8080"
AIRFLOW_USER = "airflow"

# Airflow connection names
S3_CONNECTION_NAME = "s3_conn"
PATIENT_SERVICE_CONNECTION_NAME = "patient_service_connection"
MEASURE_SERVICE_CONNECTION_NAME = "measure_service_connection"

# CCDA FHIR Transformation
CCDA_FILE_PATH = "/opt/airflow/CCDA/"

# Airflow dags names
HL7_TO_FHIR_DAG = "hl7_to_fhir_dag"
CCD_TO_FHIR_DAG = "ccd_to_fhir_dag"
EDI837_TO_FHIR_DAG = "edi837_to_fhir_dag"
FHIRBUNDLE_PROCESSING_DAG = "fhirbundle_processing_dag"
MEASURE_PROCESSING_DAG = "measure_processing_dag"
CCLF_STAGE_LOAD_DAG = "cclf_stage_load_dag"
CCLF_BENE_STAGE_LOAD_DAG = "cclf_beneficiary_stage_load_dag"

# for bulk processing
FHIRBUNDLE_BULK_PROCESSING_DAG = "fhirbundle_bulk_processing_dag"
RESTRICTED_EXTENTIONS = [".crc", "_SUCCESS"]

RADAID_STAGE_TO_FHIR_DAG = "radaid_stage_to_fhirbundle_dag"

DAG_TO_TRIGGER = {
    "BENEFICIARY": "beneficiary_stage_load_dag",
    "MEDICAL_CLAIM": "medical_claim_stage_load_dag",
    "RX_CLAIM": "rx_claim_stage_load_dag",
    "ECW": "ecw_stage_load_dag",
    "GAP": "gap_stage_load_dag",
    "NCQA_HEDIS": "ncqa_hedis_stage_load_dag",
    "CCLF": "cclf_stage_load_dag",
    "CCLF_BENEFICIARY": "cclf_beneficiary_stage_load_dag",
    "EDI837": "edi837_to_fhir_dag",
    "RADAID": "radaid_stage_load_dag",
}

# Analytical Airflow dag names
ANALYTICAL_PATIENT_DAG = "patient_analytical_load_dag"
ANALYTICAL_FINANCIAL_DAG = "financial_analytical_load_dag"
ANALYTICAL_MEASURE_DAG = "measure_analytical_load_dag"

# Airflow connections
S3_CONNECTION = Connection.get_connection_from_secrets(S3_CONNECTION_NAME)
PATIENT_CONNECTION = Connection.get_connection_from_secrets(PATIENT_SERVICE_CONNECTION_NAME)
MEASURE_CONNECTION = Connection.get_connection_from_secrets(MEASURE_SERVICE_CONNECTION_NAME)
AUTH_CLIENT_ID = Variable.get("AUTH_CLIENT_ID")
AUTH_CLIENT_SECRET = Variable.get("AUTH_CLIENT_SECRET")
MEASURE_AUTH_CLIENT_ID = Variable.get("MEASURE_AUTH_CLIENT_ID")
MEASURE_AUTH_CLIENT_SECRET = Variable.get("MEASURE_AUTH_CLIENT_SECRET")

EVENT_SERVICE_CONFIG_NAME = "event-service-config"
EVENT_SERVICE_CONFIG_NAMESPACE = "data-pipeline"
EVENT_SERVICE_SECRET_NAME = "event-service-auth-credentials"
EVENT_SERVICE_SECRET_NAMESPACE = "data-pipeline"

PLATFORM_SERVICE_CONFIG_NAME_SUFFIX = "platform-service-config"
PLATFORM_SERVICE_CONFIG_NAMESPACE = "data-pipeline"
PLATFORM_SERVICE_SECRET_NAME_SUFFIX = "platform-service-auth-credentials"
PLATFORM_SERVICE_SECRET_NAMESPACE = "data-pipeline"

PIPELINE_SECRET_KEY_NAME = "pipeline_data_key"

# Default Values
DEFAULT_FHIR_ORG_ID = ""
USE_DEFAULT_ORG = Variable.get("USE_DEFAULT_ORG")
if USE_DEFAULT_ORG == "true":
    DEFAULT_FHIR_ORG_ID = Variable.get("DEFAULT_FHIR_ORG_ID")


# Spark job definitions
TMPL_SEARCH_PATH = "/opt/airflow/dags/templates/"
SCHEMA_SEARCH_PATH = "/opt/airflow/dags/schemas/"
DATA_SEARCH_PATH = "/opt/airflow/fhirbundles/"

# Domain service constants
# TENANT_ID = '34cfbf50-d654-408d-a2af-3294543ef85f'
# TENANT_SUBDOMAIN = "cynchealth"
# SERVICE_HOST = "development.healthec.com"


RESPONSE_CODE_200 = 200
RESPONSE_CODE_201 = 201
RESPONSE_CODE_409 = 409

# KAFKA EVENT STATUSES
EVENT_SUCCESS_STATUS = "success"
EVENT_FAILURE_STATUS = "failure"

# Audit events
EVENT_START = "start"
EVENT_COMPLETE = "complete"

# Common constants
LANDING_PATH_SUFIX = "landing"
PROCESSED_PATH_SUFIX = "processed"
ERROR_PATH_SUFIX = "error"

FHIRBUNDLE_PATH_STR = "FHIRBUNDLE"
CANONICAL_PATH_STR = "CANONICAL"

# Supported file formats
CCD_FILE_FORMAT = "CCD"
HL7_FILE_FORMAT = "HL7"
FHIRBUNDLE_FILE_FORMAT = "FHIRBUNDLE"
MEASURE_FILE_FORMAT = "MEASURE"

# Templates path
TEMPLATE_SEARCH_PATH = "/opt/airflow/dags/templates/"

# Tenant schema mapping file path
TENANT_MAPPING_FILE_PATH = "/opt/airflow/dags/schemas/"

# Merger schema
PRACTITIONER_MERGER_SCHEMA = {
    "mergeStrategy": "objectMerge",
    "properties": {
        "resourceType": {
            "mergeStrategy": "discard",
        },
        "id": {
            "mergeStrategy": "discard",
        },
        "identifier": {
            "type": "array",
            "mergeStrategy": "arrayMergeById",
            "mergeOptions": {"idRef": "value"},
        },
        "active": {
            "mergeStrategy": "discard",
        },
        "name": {
            "mergeStrategy": "discard",
        },
        "telecom": {
            "type": "array",
            "mergeStrategy": "arrayMergeById",
            "mergeOptions": {"idRef": "system"},
        },
        "address": {
            "type": "array",
            "mergeStrategy": "arrayMergeById",
            "mergeOptions": {"idRef": "city"},
        },
        "gender": {
            "mergeStrategy": "discard",
        },
        "qualification": {
            "type": "array",
            "mergeStrategy": "arrayMergeById",
            "mergeOptions": {"idRef": "code/text"},
        },
    },
    "additionalProperties": False,
}

EVENT_FIELDS = [
    "links",
    "event",
    "eventType",
    "origin",
    "ownerId",
    "ownerType",
    "resourceId",
    "resourceType",
    "tenantId",
    "timestamp",
    "token",
    "traceId",
    "userId",
    "version",
]

MEASURE_EVENT_FIELDS = [
    "event",
    "eventType",
    "version",
    "timestamp",
    "origin",
    "ownerId",
    "ownerType",
    "status",
    "tenantId",
    "resourceId",
    "resourceType",
    "rulesetId",
    "rulesetVersionId",
    "links",
]


DOWNLOAD_ZIP_FILE_PATH = "/opt/airflow/dags/downloads"

CCLF_TYPES = {
    "CCLF1": "ZC1",
    "CCLF2": "ZC2",
    "CCLF3": "ZC3",
    "CCLF4": "ZC4",
    "CCLF5": "ZC5",
    "CCLF6": "ZC6",
    "CCLF7": "ZC7",
    "CCLFA": "ZCA",
    "CCLFB": "ZCB",
    "CCLF8": "ZC8",
    "CCLF9": "ZC9",
}

CCLF_BENEFICIARY_TYPES = {
    "1": "assigned_beneficiaries",
    "2": "beneficiary_primarycare_tin",
    "3": "beneficiary_primarycare_ccn",
    "4": "beneficiary_primarycare_npi",
    "5": "beneficiary_quaterly_turnover_analysis",
    "6": "assignable_beneficiaries",
    "7": "assigned_beneficiaries_covid19",
    "8": "assigned_ccns",
    "9": "assigned_underserved",
}
