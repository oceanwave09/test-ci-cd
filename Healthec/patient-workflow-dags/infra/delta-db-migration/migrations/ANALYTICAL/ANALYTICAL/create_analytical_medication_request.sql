CREATE TABLE IF NOT EXISTS TENANT_analytical.medication_request(
    medication_request_fhir_id VARCHAR,
    internal_id VARCHAR,
    status VARCHAR,
    intent VARCHAR,
    category_code VARCHAR,
    category_desc VARCHAR,
    priority_code VARCHAR,
    priority_desc VARCHAR,
    medication_fhir_id VARCHAR,
    patient_fhir_id VARCHAR,
    encounter_fhir_id VARCHAR,
    authored_on TIMESTAMP(3) WITH TIME ZONE,
    provider_fhir_id VARCHAR,
    reason_fhir_id VARCHAR,
    coverage_fhir_id VARCHAR,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/analytical/medication_request',
    CHECKPOINT_INTERVAL = 5
);