CREATE TABLE IF NOT EXISTS TENANT_analytical.observation (
    observation_fhir_id VARCHAR,
    internal_id VARCHAR,
    status VARCHAR,
    category_code VARCHAR,
    category_desc VARCHAR,
    code VARCHAR,
    code_desc VARCHAR,
    patient_fhir_id VARCHAR,
    encounter_fhir_id VARCHAR,
    effective_start_date TIMESTAMP(3) WITH TIME ZONE,
    effective_end_date TIMESTAMP(3) WITH TIME ZONE,
    provider_fhir_id VARCHAR,
    value VARCHAR,
    value_unit VARCHAR,
    value_desc VARCHAR,
    interpretation_code VARCHAR,
    interpretation_desc VARCHAR,
    body_site_code VARCHAR,
    body_site_desc VARCHAR,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/analytical/observation',
    CHECKPOINT_INTERVAL = 5
);