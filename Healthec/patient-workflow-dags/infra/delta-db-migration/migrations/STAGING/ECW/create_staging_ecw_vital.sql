CREATE TABLE IF NOT EXISTS TENANT_staging.ecw_vital (
    row_id VARCHAR,
    apuid VARCHAR,
    record_id VARCHAR,
    patient_id VARCHAR,
    patient_account_no VARCHAR,
    encounter_id VARCHAR,
    provider_npi VARCHAR,
    item_id VARCHAR,
    vitals_captured_by VARCHAR,
    vitals_name VARCHAR,
    standard_vital VARCHAR,
    vitals_value VARCHAR,
    modified_date TIMESTAMP(3) WITH TIME ZONE,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/ecw_vital',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);