# General Constants
S3_SCHEMA = "s3a"
FIELD_SEPARATOR = ","

# Database Protocols
POSTGRES = "postgres"
MSSQL = "mssql"

# JDBC Driver Class
POSTGRES_DRIVER = "org.postgresql.Driver"
MSSQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

# Acceptable return codes from domain services
RESPONSE_CODE_200 = 200
RESPONSE_CODE_201 = 201
RESPONSE_CODE_409 = 409
ACCEPTABLE_RETURN_CODES = [RESPONSE_CODE_200, RESPONSE_CODE_201, RESPONSE_CODE_409]

DEFAULT_USER_NAME = "data-pipeline"
INGESTION_PIPELINE_USER = "ingestion-pipeline"
ANALYTICAL_PIPELINE_USER = "analytical-pipeline"

# Organization Types
TYPE_PROV_CODE = "prov"
TYPE_PROV_DISPLAY = "Healthcare Provider"
TYPE_DEPT_CODE = "dept"
TYPE_DEPT_DISPLAY = "Hospital Department"
TYPE_PAY_CODE = "pay"
TYPE_PAY_DISPLAY = "Payer"
TYPE_OTHER_CODE = "other"
TYPE_OTHER_DISPLAY = "Other"

ORG_ROLE_CODE = "provider"
ORG_ROLE_DISPLAY = "Provider"


# Constants used in FHIR bundle creation
LOCATION_MODE = "kind"
ACTIVE_STATUS = "true"
STATUS_ACTIVE = "active"
PRACTICE_LAB_SOURCE_SYSTEM = "http://healthec.com/practice-lab"
PAYER_SOURCE_SYSTEM = "http://healthec.com/payer-organization"
ACO_NETWORK_SOURCE_SYSTEM = "http://healthec.com/aco-network"
