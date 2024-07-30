CREATE TABLE IF NOT EXISTS cynchealth_analytical.provider (
    provider_fhir_id VARCHAR,
    external_id VARCHAR,
    ssn VARCHAR,
    tin VARCHAR,
    npi VARCHAR,
    state_license VARCHAR,
    active BOOLEAN,
    city VARCHAR,
    county VARCHAR,
    state VARCHAR,
    zip VARCHAR,
    country VARCHAR,
    gender VARCHAR,
    birth_date DATE,
    qualification_code VARCHAR,
    qualification_system VARCHAR,
    qualification_desc VARCHAR,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/cynchealth/analytical/provider',
    CHECKPOINT_INTERVAL = 5
);
