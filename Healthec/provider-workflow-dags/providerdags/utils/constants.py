from airflow.models import Connection

# Airflow details
AIRFLOW_ENDPOINT = "http://airflow-webserver.data-pipeline.svc.cluster.local:8080"
AIRFLOW_USER = "airflow"

# Airflow connection names
S3_CONNECTION_NAME = "s3_conn"
EVENT_SERVICE_CONNECTION_NAME = "event_service_connection"

# Airflow dags names
SOURCE_TO_CANONICAL_DAG = "source_to_canonical_dag"
PRACTICE_STAGE_LOAD_DAG = "practice_stage_load_dag"
ANALYTICAL_PROVIDER_DAG = "provider_analytical_load_dag"


# Airflow connections
S3_CONNECTION = Connection.get_connection_from_secrets(S3_CONNECTION_NAME)

# Event service configmap, secret details from k8s
EVENT_SERVICE_CONFIG_NAME = "event-service-config"
EVENT_SERVICE_CONFIG_NAMESPACE = "data-pipeline"
EVENT_SERVICE_SECRET_NAME = "event-service-auth-credentials"
EVENT_SERVICE_SECRET_NAMESPACE = "data-pipeline"

# Date key secret details from k8s
DATA_KEY_NAMESPACE = "data-pipeline"

# Spark job definitions and omniparser schema paths in Airflow container
TMPL_SEARCH_PATH = "/opt/airflow/dags/templates/"
SCHEMA_SEARCH_PATH = "/opt/airflow/dags/schemas/"

RESPONSE_CODE_200 = 200
RESPONSE_CODE_201 = 201

CANONICAL_PATH_STR = "CANONICAL"

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
INCOMING_PATH_SUFIX = "incoming"

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


ENCRYPTED_PATH_SPLIT_KEY = "incoming"
