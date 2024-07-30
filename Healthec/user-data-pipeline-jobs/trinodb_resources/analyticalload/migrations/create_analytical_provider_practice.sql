CREATE TABLE IF NOT EXISTS cynchealth_analytical.provider_practice (
    provider_practice_fhir_id VARCHAR,
    external_id VARCHAR,
    active BOOLEAN,
    start_date TIMESTAMP(3) WITH TIME ZONE,
    end_date TIMESTAMP(3) WITH TIME ZONE,
    provider_fhir_id VARCHAR,
    practice_fhir_id VARCHAR,
    role_code VARCHAR,
    role_system  VARCHAR,
    role_desc VARCHAR,
    specialty_code VARCHAR,
    specialty_system VARCHAR,
    specialty_desc VARCHAR,
    facility_fhir_id VARCHAR,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/cynchealth/analytical/provider_practice',
    CHECKPOINT_INTERVAL = 5
);
