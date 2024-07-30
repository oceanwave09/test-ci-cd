CREATE TABLE IF NOT EXISTS cynchealth_analytical.payer_practice (
    payer_fhir_id VARCHAR,
    external_id VARCHAR,
    practice_fhir_id VARCHAR,
    taxonomy_code VARCHAR,
    taxonomy_system VARCHAR,
    taxonomy_desc VARCHAR,
    specialty_code VARCHAR,
    specialty_system VARCHAR,
    specialty_desc VARCHAR,
    location_fhir_id VARCHAR,
    effective_start VARCHAR,
    effective_end VARCHAR,
    active BOOLEAN,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/cynchealth/analytical/payer_practice',
    CHECKPOINT_INTERVAL = 5
);