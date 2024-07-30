CREATE TABLE IF NOT EXISTS TENANT_staging.ecw_problemlist (
    row_id VARCHAR,
    apuid VARCHAR,
    record_id VARCHAR,
    patient_id VARCHAR,
    patient_account_no VARCHAR,
    asmt_id VARCHAR,
    onset_date TIMESTAMP(3) WITH TIME ZONE,
    code VARCHAR,
    name VARCHAR,
    encounter_id VARCHAR,
    status VARCHAR,
    pt_condition VARCHAR,
    risk VARCHAR,
    added_date TIMESTAMP(3) WITH TIME ZONE,
    resolved_on TIMESTAMP(3) WITH TIME ZONE,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/ecw_problemlist',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);