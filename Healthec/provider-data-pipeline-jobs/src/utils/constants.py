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
ACCEPTABLE_RETURN_CODES = [
    RESPONSE_CODE_200,
    RESPONSE_CODE_201,
    RESPONSE_CODE_409,
]

# Default Values
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

ORG_ROLE_CODE = "payer"
ORG_ROLE_DISPLAY = "Payer"

# Constants used in FHIR bundle creation
LOCATION_MODE = "kind"
ACTIVE_STATUS = "true"
STATUS_ACTIVE = "active"
PRACTICE_LAB_SOURCE_SYSTEM = "http://healthec.com/practice-lab"
BACO_PAYER_SOURCE_SYSTEM = "http://baco-hap.com/organization/payer"
BACO_PROV_SOURCE_SYSTEM = "http://baco-hap.com/id/provider"
