CREATE TABLE IF NOT EXISTS TENANT_staging.ecw_referral (
    row_id VARCHAR,
    apuid VARCHAR,
    record_id VARCHAR,
    referral_id VARCHAR,
    patient_id VARCHAR,
    patient_account_no VARCHAR,
    speciality_code VARCHAR,
    speciality VARCHAR,
    referral_type VARCHAR,
    referral_status VARCHAR,
    referral_from_name VARCHAR,
    referral_from_npi VARCHAR,
    referral_to_name VARCHAR,
    referral_to_npi VARCHAR,
    facility_from VARCHAR,
    facility_to VARCHAR,
    referral_date TIMESTAMP(3) WITH TIME ZONE,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/ecw_referral',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);