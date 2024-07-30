CREATE TABLE IF NOT EXISTS TENANT_staging.ecw_surgicalhistory (
    row_id VARCHAR, 
    apuid VARCHAR,
    record_id VARCHAR,
    patient_id VARCHAR,
    patient_account_no VARCHAR,
    encounter_id VARCHAR,
    provider_npi VARCHAR,
    reason VARCHAR,
    surgical_date TIMESTAMP(3) WITH TIME ZONE,
    cpt_code VARCHAR,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/ecw_surgicalhistory',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);