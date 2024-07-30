CREATE TABLE IF NOT EXISTS cynchealth_staging.ecw_encounter (
    row_id VARCHAR,
    apuid VARCHAR,
    record_id VARCHAR,
    patient_id VARCHAR,
    patient_account_no VARCHAR,
    provider_id VARCHAR,
    provider_npi VARCHAR,
    encounter_id VARCHAR,
    encounter_visit_type VARCHAR,
    encounter_status VARCHAR,
    encounter_date VARCHAR,
    encounter_start_time VARCHAR,
    encounter_end_time VARCHAR,
    encounter_reason VARCHAR,
    encounter_lock_status VARCHAR,
    medication_reconciliation_flag VARCHAR,
    facility_name VARCHAR,
    facility_id VARCHAR,
    pos VARCHAR,
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
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/cynchealth/staging/ecw_encounter',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);