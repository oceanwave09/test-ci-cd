CREATE TABLE IF NOT EXISTS cynchealth_staging.gap_sdoh (
    row_id VARCHAR,
    row VARCHAR,
    domain VARCHAR,
    patient_first_name VARCHAR,
    patient_last_name VARCHAR,
    patient_date_of_birth VARCHAR,
    external_patient_id VARCHAR,
    external_practice_id VARCHAR,
    source_name VARCHAR,
    screen_date VARCHAR,
    screening_practice_name VARCHAR,
    question_group VARCHAR,
    question VARCHAR,
    answer VARCHAR,
    mihin_common_key VARCHAR,
    provider_name VARCHAR,
    provider_npi VARCHAR,
    file_batch_id VARCHAR,
    file_name VARCHAR,
    file_source_name VARCHAR,
    file_status VARCHAR,
    created_user VARCHAR,
    created_ts TIMESTAMP(3) WITH TIME ZONE,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/cynchealth/staging/gap_sdoh',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);