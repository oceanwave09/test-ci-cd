CREATE TABLE IF NOT EXISTS TENANT_staging.ecw_practiceuser (
    row_id VARCHAR,
    apuid VARCHAR,
    record_id VARCHAR,
    user_id VARCHAR,
    provider_npi VARCHAR,
    user_type VARCHAR,
    user_first_name VARCHAR,
    user_last_name VARCHAR,
    speciality VARCHAR,
    facility_id VARCHAR,
    date_of_birth DATE,
    gender VARCHAR,
    user_status VARCHAR,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/ecw_practiceuser',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);