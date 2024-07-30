CREATE TABLE IF NOT EXISTS TENANT_analytical.provider_practice (
    provider_practice_fhir_id VARCHAR,
    internal_id VARCHAR,
    active BOOLEAN,
    start_date TIMESTAMP(3) WITH TIME ZONE,
    end_date TIMESTAMP(3) WITH TIME ZONE,
    provider_fhir_id VARCHAR,
    practice_fhir_id VARCHAR,
    role_code VARCHAR,
    role_desc VARCHAR,
    specialty_codes ARRAY(VARCHAR),
    facility_fhir_id ARRAY(VARCHAR),
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/analytical/provider_practice',
    CHECKPOINT_INTERVAL = 5
);
