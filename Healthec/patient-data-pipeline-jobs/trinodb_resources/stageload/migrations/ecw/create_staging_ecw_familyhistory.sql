CREATE TABLE IF NOT EXISTS cynchealth_staging.ecw_familyhistory (
    row_id VARCHAR,
    apuid VARCHAR,
    record_id VARCHAR,
    patient_id VARCHAR,
    patient_account_no VARCHAR,
    encounter_id VARCHAR,
    provider_npi VARCHAR,
    relationship VARCHAR,
    status VARCHAR,
    age VARCHAR,
    date_of_birth VARCHAR,
    icd_item_id VARCHAR,
    icd_code VARCHAR,
    icd_desc VARCHAR,
    diagnosed_year VARCHAR,
    diagnosed_age VARCHAR,
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
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/cynchealth/staging/ecw_familyhistory',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);