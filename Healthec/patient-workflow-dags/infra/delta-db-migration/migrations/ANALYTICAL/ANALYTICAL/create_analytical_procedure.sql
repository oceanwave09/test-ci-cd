CREATE TABLE IF NOT EXISTS TENANT_analytical.procedure(
    procedure_fhir_id VARCHAR,
    internal_id VARCHAR,
    status VARCHAR,
    category_code VARCHAR,
    category_desc VARCHAR,
    code VARCHAR,
    code_desc VARCHAR,
    patient_fhir_id VARCHAR,
    encounter_fhir_id VARCHAR,
    performed_start_date TIMESTAMP(3) WITH TIME ZONE,
    performed_end_date TIMESTAMP(3) WITH TIME ZONE,
    provider_fhir_id VARCHAR,
    facility_fhir_id VARCHAR,
    reason_fhir_id VARCHAR,
    bodysite_code VARCHAR,
    bodysite_desc VARCHAR,
    outcome_code VARCHAR,
    outcome_desc VARCHAR,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/analytical/procedure',
    CHECKPOINT_INTERVAL = 5
);